import { Injectable, WritableSignal, computed, effect, inject, signal } from '@angular/core';
import { Observable, Subject, filter, map } from 'rxjs';
import { Auth } from './auth.service';
import { CONFIG, Incident, MetricEvent, Point, Rule, Service } from './models';
import { ConnectionState } from './coordinator';
import { StreamClient } from './stream-client';
import { DisplayBatch, FromStreamWorker, Selection, emptyCounters } from './stream-protocol';
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
  readonly counters = signal(emptyCounters());
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
  private detailBatches = new Subject<DisplayBatch['detail']>();
  private selection: Selection = { id: 0, serviceId: null };
  private confirmedRules: Rule[] = [];
  private client?: StreamClient;
  private controller?: AbortController;
  private active = false;
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
    window.addEventListener('offline', () => {
      this.connection.update((c) => ({ ...c, offline: true, connected: false }));
      this.client?.send({ type: 'online', online: false });
    });
    window.addEventListener('online', () => {
      if (this.client) this.client.send({ type: 'online', online: true });
      else if (this.active) void this.start();
    });
    setInterval(() => this.clock.set(Date.now()), CONFIG.workerCheckMs);
  }
  row(id: string): WritableSignal<MetricEvent | null> {
    return this.rows.get(id) ?? signal(null);
  }
  /** A selection generation prevents late batches leaking across fast route switches. */
  watchService(serviceId: string): Observable<Point[]> {
    return new Observable((subscriber) => {
      const selection = { id: this.selection.id + 1, serviceId };
      this.selection = selection;
      const subscription = this.detailBatches
        .pipe(
          filter((batch) => batch.id === selection.id && batch.serviceId === serviceId),
          map((batch) => batch.points),
        )
        .subscribe(subscriber);
      this.client?.send({ type: 'select', selection });
      return () => {
        subscription.unsubscribe();
        if (this.selection.id === selection.id) {
          this.selection = { id: selection.id + 1, serviceId: null };
          this.client?.send({ type: 'select', selection: this.selection });
        }
      };
    });
  }
  private async start(): Promise<void> {
    this.client?.close();
    this.client = undefined;
    this.controller?.abort();
    this.controller = new AbortController();
    const signal = this.controller.signal;
    this.error.set('');
    this.connection.set({ kind: 'ws', connected: false, retryAt: 0, offline: !navigator.onLine });
    try {
      if (typeof Worker === 'undefined')
        throw new Error('This browser does not support Web Workers');
      const accessToken = this.auth.api.session?.accessToken;
      if (!accessToken) throw new Error('Please log in');
      this.client = new StreamClient(
        this.auth.api,
        (message) => {
          if (!signal.aborted) this.receive(message);
        },
        (message) => {
          if (signal.aborted) return;
          this.error.set(message);
          this.connection.update((c) => ({ ...c, connected: false, retryAt: 0 }));
          this.client = undefined;
        },
      );
      this.client.send({
        type: 'start',
        accessToken,
        online: navigator.onLine,
        rules: this.confirmedRules,
        selection: this.selection,
      });
      const services = await this.auth.api.request<Service[]>('/services', {}, signal);
      if (signal.aborted) return;
      this.services.set(services);
      void this.loadRules();
    } catch (error) {
      if (!signal.aborted) {
        this.client?.close();
        this.client = undefined;
        this.error.set(`Fleet load failed: ${String(error)}`);
        this.connection.update((c) => ({ ...c, connected: false }));
      }
    }
  }
  retry(): void {
    void this.start();
  }
  private stop(): void {
    this.active = false;
    this.controller?.abort();
    this.client?.close();
    this.client = undefined;
    this.latest.clear();
    this.services.set([]);
    this.rules.set([]);
    this.confirmedRules = [];
    this.incidents.set([]);
    this.notice.set('');
    this.error.set('');
    this.rulesError.set('');
    this.lastEvent.set(0);
    this.counters.set(emptyCounters());
    for (const row of this.rows.values()) row.set(null);
  }
  private receive(message: FromStreamWorker): void {
    switch (message.type) {
      case 'connection':
        this.connection.set(message.state);
        break;
      case 'error':
        this.error.set(message.message);
        break;
      case 'reset':
        this.notice.set(message.message);
        this.resetHistory.next();
        break;
      case 'batch':
        for (const event of message.changes) {
          this.latest.set(event.serviceId, event);
          this.rows.get(event.serviceId)?.set(event);
        }
        if (message.changes.length) this.revision.update((n) => n + 1);
        this.lastEvent.set(message.lastEvent);
        this.counters.set(message.counters);
        if (message.incidents.length) {
          this.incidents.update((current) =>
            [...message.incidents, ...current].slice(0, CONFIG.incidentLimit),
          );
          const newest = message.incidents[0];
          this.notice.set(`${newest.rule} fired for ${newest.serviceId}`);
        }
        if (message.detail.points.length) this.detailBatches.next(message.detail);
        break;
    }
  }
  async loadRules(): Promise<void> {
    const signal = this.controller?.signal;
    try {
      const rules = await this.auth.api.request<Rule[]>('/rules', {}, signal);
      if (signal?.aborted) return;
      this.rulesChanged(rules);
      this.rulesError.set('');
    } catch (error) {
      if (!signal?.aborted)
        this.rulesError.set(
          `Rules unavailable; alert evaluation may be incomplete: ${String(error)}`,
        );
    }
  }
  rulesChanged(rules: Rule[], confirmed = true): void {
    this.rules.set(rules);
    if (confirmed) {
      this.confirmedRules = rules;
      this.client?.send({ type: 'rules', rules });
    }
  }
}
