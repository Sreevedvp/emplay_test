import { backoff, FallbackPolicy, TransportKind } from './algorithms';
import { CONFIG, Snapshot } from './models';
import { isMetric } from './integrity';
import { Transport, TransportFactory, TransportSink } from './transports';
export interface ConnectionState {
  kind: TransportKind;
  connected: boolean;
  retryAt: number;
  offline: boolean;
}
/** No Angular dependency: deterministic fake transports can exercise this state machine. */
export class Coordinator {
  private active?: Transport;
  private probe?: Transport;
  private retry?: ReturnType<typeof setTimeout>;
  private upgrade?: ReturnType<typeof setInterval>;
  private watchdog?: ReturnType<typeof setInterval>;
  private pingAt = 0;
  private stopped = true;
  private generation = 0;
  private policy = new FallbackPolicy();
  state: ConnectionState = { kind: 'ws', connected: false, retryAt: 0, offline: false };
  constructor(
    private factory: TransportFactory,
    private receive: (raw: string) => boolean,
    private snapshot: (s: Snapshot) => void,
    private changed: (state: ConnectionState) => void,
    private recover: () => void,
  ) {}
  start(): void {
    this.stopped = false;
    this.connect('ws');
    this.upgrade = setInterval(() => this.tryUpgrade(), CONFIG.upgradeMs);
    this.watchdog = setInterval(() => {
      if (
        this.state.connected &&
        this.state.kind !== 'poll' &&
        Date.now() - this.pingAt > CONFIG.heartbeatTimeout
      )
        this.failed(false);
    }, 500);
  }
  stop(): void {
    this.stopped = true;
    this.generation++;
    this.active?.close();
    this.probe?.close();
    this.probe = undefined;
    clearTimeout(this.retry);
    clearInterval(this.upgrade);
    clearInterval(this.watchdog);
  }
  offline(offline: boolean): void {
    this.state = { ...this.state, offline, connected: false, retryAt: 0 };
    this.generation++;
    this.active?.close();
    this.probe?.close();
    this.probe = undefined;
    clearTimeout(this.retry);
    this.changed(this.state);
    if (!offline && !this.stopped) this.connect('ws');
  }
  private publish(patch: Partial<ConnectionState>): void {
    this.state = { ...this.state, ...patch };
    this.changed(this.state);
  }
  private connect(kind: TransportKind): void {
    if (this.stopped || this.state.offline) return;
    this.active?.close();
    const generation = ++this.generation;
    this.publish({ kind, connected: false, retryAt: 0 });
    this.active = this.factory(kind, {
      opened: () => {
        if (generation !== this.generation) return;
        this.pingAt = Date.now();
        this.publish({ connected: true });
        this.recover();
      },
      message: (raw) => {
        if (generation !== this.generation) return;
        if (this.receive(raw)) {
          this.pingAt = Date.now();
          this.policy.success();
        }
      },
      snapshot: (s) => {
        if (generation === this.generation) {
          this.policy.success();
          this.snapshot(s);
        }
      },
      failed: (_e, timeout) => {
        if (generation === this.generation) this.failed(timeout ?? false);
      },
    });
  }
  private failed(timeout: boolean): void {
    this.generation++;
    this.active?.close();
    const delay = backoff(this.policy.failures + 1);
    const next = this.policy.failed(this.state.kind, timeout);
    this.publish({ connected: false, retryAt: Date.now() + delay });
    this.retry = setTimeout(() => this.connect(next), delay);
  }
  private tryUpgrade(): void {
    const higher = this.policy.higher(this.state.kind);
    if (!higher || this.stopped || this.state.offline || this.probe || !this.state.connected)
      return;
    let promoted = false;
    const localGeneration = this.generation;
    const sink: TransportSink = {
      opened: () => undefined, // Promote only after valid data/ping, never a half-open handshake.
      message: (raw) => {
        if (
          this.stopped ||
          this.state.offline ||
          (!promoted && localGeneration !== this.generation)
        )
          return;
        let valid = false;
        try {
          const value: unknown = JSON.parse(raw);
          valid =
            isMetric(value) ||
            (!!value &&
              typeof value === 'object' &&
              'type' in value &&
              value.type === 'ping' &&
              'ts' in value &&
              typeof value.ts === 'number' &&
              Number.isFinite(value.ts));
        } catch {
          /* normal malformed counter owns logging */
        }
        if (!promoted && valid) {
          promoted = true;
          clearTimeout(probeTimeout);
          this.active?.close();
          this.generation++;
          this.active = candidate;
          this.pingAt = Date.now();
          this.probe = undefined;
          this.policy.success();
          this.publish({ kind: higher, connected: true, retryAt: 0 });
          this.recover();
        }
        if (promoted && this.receive(raw)) this.pingAt = Date.now();
      },
      snapshot: () => undefined,
      failed: () => {
        clearTimeout(probeTimeout);
        if (promoted) this.failed(false);
        else {
          candidate.close();
          this.probe = undefined;
        }
      },
    };
    const probeTimeout = setTimeout(() => {
      if (!promoted) {
        candidate.close();
        this.probe = undefined;
      }
    }, CONFIG.heartbeatTimeout);
    const candidate = this.factory(higher, sink);
    this.probe = candidate;
  }
}
