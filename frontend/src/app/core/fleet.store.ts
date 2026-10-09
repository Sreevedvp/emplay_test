import { Injectable, WritableSignal, computed, effect, inject, signal } from '@angular/core';
import { Subject } from 'rxjs';
import { Auth } from './auth.service';
import { ApiError } from './api';
import { CONFIG, Incident, MetricEvent, Rule, Service, Snapshot, Replay } from './models';
import { Integrity, isMetric } from './integrity';
import { RuleEvaluator } from './algorithms';
import { Coordinator, ConnectionState } from './coordinator';
import { createTransportFactory } from './transports';
@Injectable({ providedIn: 'root' })
export class FleetStore {
  private auth = inject(Auth);
  readonly services = signal<Service[]>([]);
  readonly rules = signal<Rule[]>([]);
  readonly incidents = signal<Incident[]>([]);
  readonly notice = signal('');
  readonly error = signal('');
  readonly rulesError = signal('');
  readonly lastEvent = signal(0);
  readonly clock = signal(Date.now());
  readonly connection = signal<ConnectionState>({
    kind: 'ws',
    connected: false,
    retryAt: 0,
    offline: !navigator.onLine,
  });
  readonly counters = signal({
    duplicates: 0,
    reordered: 0,
    malformed: 0,
    replays: 0,
    resets: 0,
    pending: 0,
    sequenceErrors: 0,
  });
  readonly events = new Subject<MetricEvent>();
  readonly resetHistory = new Subject<void>();
  readonly revision = signal(0);
  readonly stale = computed(
    () => !this.lastEvent() || this.clock() - this.lastEvent() > CONFIG.staleMs,
  );
  readonly status = computed(() => {
    const c = this.connection();
    if (c.offline) return 'Offline';
    if (this.stale() && this.lastEvent())
      return `Stale: last event ${Math.floor((this.clock() - this.lastEvent()) / 1000)} s ago`;
    if (c.retryAt)
      return `Reconnecting in ${Math.max(0, Math.ceil((c.retryAt - this.clock()) / 1000))} s`;
    if (!c.connected) return 'Connecting';
    return c.kind === 'ws' ? 'Live (WebSocket)' : c.kind === 'sse' ? 'Live (SSE)' : 'Polling';
  });
  readonly rows = new Map<string, WritableSignal<MetricEvent | null>>();
  readonly latest = new Map<string, MetricEvent>();
  private pending = new Map<string, MetricEvent>();
  private integrity = new Integrity();
  private evaluator = new RuleEvaluator();
  private confirmedRules: Rule[] = [];
  private pendingIncidents: Incident[] = [];
  private coordinator?: Coordinator;
  private controller?: AbortController;
  private recovery: Promise<void> | null = null;
  private active = false;
  private rawMalformed = 0;
  private replayCount = 0;
  private resetCount = 0;
  private newestTs = 0;
  constructor() {
    for (let i = 1; i <= 2000; i++)
      this.rows.set(`svc-${String(i).padStart(4, '0')}`, signal(null));
    effect(() => {
      const user = this.auth.user();
      if (user && !this.active) {
        this.active = true;
        void this.start();
      } else if (!user && this.active) this.stop();
    });
    window.addEventListener('offline', () => this.coordinator?.offline(true));
    window.addEventListener('online', () => {
      if (this.coordinator) this.coordinator.offline(false);
      else if (this.active) void this.start();
    });
    setInterval(() => {
      this.clock.set(Date.now());
      if (this.active && navigator.onLine && this.integrity.gapDue(Date.now())) void this.recover();
    }, 500);
    setInterval(() => this.flush(), CONFIG.renderMs);
  }
  row(id: string): WritableSignal<MetricEvent | null> {
    return this.rows.get(id) ?? signal(null);
  }
  private async start(): Promise<void> {
    this.controller?.abort();
    this.controller = new AbortController();
    const signal = this.controller.signal;
    this.error.set('');
    try {
      const [services, snapshot] = await Promise.all([
        this.auth.api.request<Service[]>('/services', {}, signal),
        this.auth.api.request<Snapshot>('/metrics/snapshot', {}, signal),
      ]);
      if (signal.aborted) return;
      this.services.set(services);
      this.applySnapshot(snapshot);
      void this.loadRules();
      this.coordinator = new Coordinator(
        createTransportFactory(this.auth.api),
        (raw) => this.receive(raw),
        () => void this.recover(),
        (c) => this.connection.set(c),
        () => void this.recover(),
      );
      this.coordinator.start();
      if (!navigator.onLine) this.coordinator.offline(true);
    } catch (error) {
      if (!signal.aborted) this.error.set(`Fleet load failed: ${String(error)}`);
    }
  }
  retry(): void {
    this.coordinator?.stop();
    this.coordinator = undefined;
    void this.start();
  }
  private stop(): void {
    this.active = false;
    this.pendingIncidents = [];
    this.controller?.abort();
    this.coordinator?.stop();
    this.coordinator = undefined;
    this.integrity.clear();
    this.evaluator.clear();
    this.latest.clear();
    this.pending.clear();
    this.services.set([]);
    this.rules.set([]);
    this.confirmedRules = [];
    this.incidents.set([]);
    this.notice.set('');
    this.lastEvent.set(0);
    this.newestTs = 0;
    for (const row of this.rows.values()) row.set(null);
  }
  private applySnapshot(snapshot: Snapshot): void {
    if (
      !Number.isInteger(snapshot.offset) ||
      !Number.isFinite(snapshot.ts) ||
      snapshot.metrics.length !== 2000 ||
      !snapshot.metrics.every(isMetric) ||
      new Set(snapshot.metrics.map((e) => e.serviceId)).size !== 2000
    )
      throw new Error('Invalid snapshot');
    this.integrity.reset(snapshot);
    for (const event of snapshot.metrics) {
      this.latest.set(event.serviceId, event);
      this.pending.set(event.serviceId, event);
      this.newestTs = Math.max(this.newestTs, event.ts);
    }
    this.flush();
  }
  private receive(raw: string): boolean {
    try {
      const event: unknown = JSON.parse(raw);
      if (
        event &&
        typeof event === 'object' &&
        'type' in event &&
        event.type === 'ping' &&
        'ts' in event &&
        typeof event.ts === 'number' &&
        Number.isFinite(event.ts)
      )
        return true;
      if (!isMetric(event)) throw new Error('Invalid metric schema');
      this.apply(this.integrity.push(event, Date.now()));
    } catch (error) {
      this.rawMalformed++;
      if (this.rawMalformed <= 5 || this.rawMalformed % 100 === 0)
        console.warn('Dropped malformed stream event', error);
    }
    return false; // Only a valid server ping resets the heartbeat watchdog.
  }
  private apply(events: MetricEvent[]): void {
    for (const event of events) {
      this.latest.set(event.serviceId, event);
      this.pending.set(event.serviceId, event);
      this.newestTs = Math.max(this.newestTs, event.ts);
      this.events.next(event);
      const incidents = this.evaluator.evaluate(this.confirmedRules, event);
      if (incidents.length) {
        this.pendingIncidents = [...incidents, ...this.pendingIncidents].slice(
          0,
          CONFIG.incidentLimit,
        );
      }
    }
  }
  private flush(): void {
    if (this.pendingIncidents.length) {
      const newest = this.pendingIncidents[0];
      this.incidents.update((current) =>
        [...this.pendingIncidents, ...current].slice(0, CONFIG.incidentLimit),
      );
      this.notice.set(`${newest.rule} fired for ${newest.serviceId}`);
      this.pendingIncidents = [];
    }
    if (this.pending.size) {
      for (const [id, event] of this.pending) this.rows.get(id)?.set(event);
      this.pending.clear();
      this.revision.update((n) => n + 1);
      this.lastEvent.set(this.newestTs);
    }
    const counters = {
      duplicates: this.integrity.duplicates,
      reordered: this.integrity.reordered,
      malformed: this.rawMalformed,
      replays: this.replayCount,
      resets: this.resetCount,
      pending: this.integrity.size,
      sequenceErrors: this.integrity.sequenceErrors,
    };
    if (JSON.stringify(counters) !== JSON.stringify(this.counters())) this.counters.set(counters);
  }
  private recover(): Promise<void> {
    if (this.recovery) return this.recovery;
    const signal = this.controller?.signal;
    if (!this.active || signal?.aborted || !navigator.onLine) return Promise.resolve();
    this.recovery = (async () => {
      try {
        if (this.integrity.overflow || this.integrity.sequenceFault) {
          await this.resnapshot(signal);
          return;
        }
        const result = await this.auth.api.request<Replay>(
          `/metrics/replay?since=${this.integrity.offset}`,
          {},
          signal,
        );
        if (signal?.aborted) return;
        this.replayCount++;
        const priorErrors = this.integrity.sequenceErrors;
        // Yield between chunks so a large recovery does not freeze input.
        for (let i = 0; i < result.events.length; i += 500) {
          for (const e of result.events.slice(i, i + 500)) {
            if (!isMetric(e)) throw new Error('Malformed replay response');
            this.apply(this.integrity.push(e, Date.now()));
          }
          await new Promise<void>((resolve) => setTimeout(resolve, 0));
          if (signal?.aborted) return;
        }
        if (this.integrity.sequenceErrors > priorErrors) await this.resnapshot(signal);
        this.error.set('');
      } catch (e) {
        if (signal?.aborted) return;
        if (e instanceof ApiError && e.status === 410) {
          try {
            await this.resnapshot(signal);
          } catch (error) {
            this.error.set(`Snapshot recovery failed: ${String(error)}`);
          }
        } else this.error.set(`Gap recovery failed; data may be incomplete: ${String(e)}`);
      }
    })().finally(() => {
      this.recovery = null;
    });
    return this.recovery;
  }
  private async resnapshot(signal?: AbortSignal): Promise<void> {
    const snapshot = await this.auth.api.request<Snapshot>('/metrics/snapshot', {}, signal);
    if (signal?.aborted) return;
    this.applySnapshot(snapshot);
    this.evaluator.clear();
    this.resetCount++;
    this.notice.set(
      'Replay unavailable or sequence invalid. Snapshot restored; alert duration timers reset and chart history reloaded.',
    );
    this.resetHistory.next();
  }
  async loadRules(): Promise<void> {
    try {
      this.rulesChanged(await this.auth.api.request<Rule[]>('/rules'));
      this.rulesError.set('');
    } catch (e) {
      this.rulesError.set(`Rules unavailable; alert evaluation may be incomplete: ${String(e)}`);
    }
  }
  rulesChanged(rules: Rule[], confirmed = true): void {
    this.rules.set(rules);
    if (confirmed) {
      this.confirmedRules = rules;
      this.evaluator.clear();
    }
  }
}
