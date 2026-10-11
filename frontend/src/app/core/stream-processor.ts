import { Integrity, isMetric } from './integrity';
import { RuleEvaluator } from './algorithms';
import { CONFIG, Incident, MetricEvent, Point, Rule, Snapshot } from './models';
import { DisplayBatch, Selection, StreamCounters } from './stream-protocol';
/** Worker-owned state. No Angular, DOM, transport or authentication dependencies. */
export class StreamProcessor {
  readonly integrity = new Integrity();
  private evaluator = new RuleEvaluator();
  private rules: Rule[] = [];
  private changes = new Map<string, MetricEvent>();
  private incidents: Incident[] = [];
  private points = new Map<number, Point>();
  private selection: Selection = { id: 0, serviceId: null };
  private lastEvent = 0;
  private malformed = 0;
  private applied = 0;
  private replays = 0;
  private resets = 0;
  private batchId = 0;
  private awaitingAck: number | null = null;
  private lastCounters = '';

  setRules(rules: Rule[]): void {
    // Re-sending unchanged confirmed rules must not re-arm an active incident.
    if (JSON.stringify(rules) === JSON.stringify(this.rules)) return;
    this.rules = rules;
    this.evaluator.clear();
  }
  select(selection: Selection): void {
    this.selection = selection;
    this.points.clear();
  }
  receive(raw: string, time: number): boolean {
    let value: unknown;
    try {
      value = JSON.parse(raw);
    } catch {
      this.badFrame();
      return false;
    }
    if (
      value &&
      typeof value === 'object' &&
      'type' in value &&
      value.type === 'ping' &&
      'ts' in value &&
      typeof value.ts === 'number' &&
      Number.isFinite(value.ts)
    )
      return true;
    if (!isMetric(value)) {
      this.badFrame();
      return false;
    }
    this.ingest(value, time);
    return false;
  }
  private badFrame(): void {
    this.malformed++;
    if (this.malformed <= 5 || this.malformed % 1000 === 0)
      console.warn('Worker dropped malformed frame', this.malformed);
  }
  ingest(event: MetricEvent, time: number): void {
    for (const ordered of this.integrity.push(event, time)) {
      this.applied++;
      this.changes.set(ordered.serviceId, ordered);
      this.lastEvent = Math.max(this.lastEvent, ordered.ts);
      // Evaluate ALL ordered events before coalescing display updates.
      for (const incident of this.evaluator.evaluate(this.rules, ordered)) {
        this.incidents.push(incident);
        if (this.incidents.length > CONFIG.incidentLimit) this.incidents.shift();
      }
      if (ordered.serviceId === this.selection.serviceId) {
        this.points.set(Math.floor(ordered.ts / 1000), {
          ts: ordered.ts,
          p95: ordered.p95,
          errRate: ordered.errRate,
          rps: ordered.rps,
        });
        // Bound the pending chart buffer even when the UI stops acknowledging batches.
        const cutoff = Math.floor(time / 1000) - CONFIG.windowMs / 1000;
        for (const key of this.points.keys()) if (key < cutoff) this.points.delete(key);
      }
    }
  }
  snapshot(value: Snapshot, recovery = false): void {
    if (
      !value ||
      !Number.isInteger(value.offset) ||
      !Number.isFinite(value.ts) ||
      !Array.isArray(value.metrics) ||
      value.metrics.length !== 2000 ||
      !value.metrics.every(isMetric) ||
      new Set(value.metrics.map((e) => e.serviceId)).size !== 2000
    )
      throw new Error('Invalid snapshot');
    this.integrity.reset(value);
    // A snapshot cannot prove the duration of conditions across the missing interval.
    this.evaluator.clear();
    this.points.clear();
    for (const event of value.metrics) {
      this.changes.set(event.serviceId, event);
      this.lastEvent = Math.max(this.lastEvent, event.ts);
    }
    if (recovery) this.resets++;
  }
  replayed(): void {
    this.replays++;
  }
  counters(): StreamCounters {
    return {
      duplicates: this.integrity.duplicates,
      reordered: this.integrity.reordered,
      malformed: this.malformed,
      replays: this.replays,
      resets: this.resets,
      pending: this.integrity.size,
      sequenceErrors: this.integrity.sequenceErrors,
      applied: this.applied,
    };
  }
  acknowledge(id: number): void {
    if (this.awaitingAck === id) this.awaitingAck = null;
  }
  /** One outstanding UI batch: a busy/hidden tab cannot grow a postMessage queue. */
  takeBatch(): DisplayBatch | null {
    if (this.awaitingAck !== null) return null;
    const counters = this.counters();
    const serialized = JSON.stringify(counters);
    if (
      !this.changes.size &&
      !this.incidents.length &&
      !this.points.size &&
      serialized === this.lastCounters
    )
      return null;
    this.lastCounters = serialized;
    const batch: DisplayBatch = {
      type: 'batch',
      id: ++this.batchId,
      changes: [...this.changes.values()],
      incidents: this.incidents.reverse(),
      detail: { ...this.selection, points: [...this.points.values()] },
      lastEvent: this.lastEvent,
      counters,
    };
    this.awaitingAck = batch.id;
    this.changes.clear();
    this.incidents = [];
    this.points.clear();
    return batch;
  }
}
