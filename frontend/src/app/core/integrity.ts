import { CONFIG, MetricEvent, Snapshot } from './models';
export function isMetric(value: unknown): value is MetricEvent {
  if (!value || typeof value !== 'object') return false;
  const e = value as Record<string, unknown>;
  return (
    e['type'] === 'metric' &&
    typeof e['serviceId'] === 'string' &&
    /^svc-\d{4}$/.test(e['serviceId']) &&
    Number(e['serviceId'].slice(4)) >= 1 &&
    Number(e['serviceId'].slice(4)) <= 2000 &&
    ['healthy', 'degraded', 'down'].includes(String(e['status'])) &&
    ['offset', 'seq', 'ts', 'p95', 'errRate', 'rps'].every(
      (k) => typeof e[k] === 'number' && Number.isFinite(e[k]) && (e[k] as number) >= 0,
    ) &&
    Number.isInteger(e['offset']) &&
    Number.isInteger(e['seq']) &&
    (e['seq'] as number) > 0 &&
    (e['offset'] as number) > 0 &&
    (e['errRate'] as number) <= 100
  );
}
/** Ordered global cursor: never advance over an unaccounted-for offset. */
export class Integrity {
  offset = 0;
  duplicates = 0;
  reordered = 0;
  sequenceErrors = 0;
  overflow = false;
  sequenceFault = false;
  private pending = new Map<number, MetricEvent>();
  private seq = new Map<string, number>();
  private waitingSince = 0;
  reset(snapshot: Snapshot): void {
    this.overflow = false;
    this.sequenceFault = false;
    this.offset = snapshot.offset;
    this.seq = new Map(snapshot.metrics.map((e) => [e.serviceId, e.seq]));
    for (const key of this.pending.keys()) if (key <= this.offset) this.pending.delete(key);
    this.waitingSince = 0;
  }
  clear(): void {
    this.overflow = false;
    this.sequenceFault = false;
    this.pending.clear();
    this.seq.clear();
    this.offset = 0;
    this.waitingSince = 0;
  }
  push(e: MetricEvent, time: number): MetricEvent[] {
    if (e.offset <= this.offset || this.pending.has(e.offset)) {
      this.duplicates++;
      return [];
    }
    if (e.offset > this.offset + 1) this.reordered++;
    if (this.pending.size >= CONFIG.maxPending) {
      this.overflow = true;
      return [];
    }
    this.pending.set(e.offset, e);
    const ready: MetricEvent[] = [];
    let next = this.pending.get(this.offset + 1);
    while (next) {
      const previous = this.seq.get(next.serviceId);
      if (previous !== undefined && next.seq !== previous + 1) {
        this.sequenceErrors++;
        this.sequenceFault = true;
        break; // Snapshot recovery is required; do not silently accept a corrupt sequence.
      }
      this.pending.delete(next.offset);
      this.seq.set(next.serviceId, next.seq);
      this.offset = next.offset;
      ready.push(next);
      next = this.pending.get(this.offset + 1);
    }
    if (!this.pending.size) this.waitingSince = 0;
    else if (!this.waitingSince) this.waitingSince = time;
    return ready;
  }
  gapDue(time: number): boolean {
    return (
      this.sequenceFault ||
      this.overflow ||
      (this.waitingSince > 0 && time - this.waitingSince >= CONFIG.reorderMs)
    );
  }
  get size(): number {
    return this.pending.size;
  }
}
