import { Condition, CONFIG, Incident, MetricEvent, Point, Rule } from './models';
export function backoff(failures: number, random = Math.random()): number {
  return (
    Math.min(CONFIG.retryCap, CONFIG.retryBase * 2 ** Math.min(failures - 1, 10)) *
    (0.5 + random * 0.5)
  );
}
export function retryable(method: string, status: number, key = false): boolean {
  return (
    (method === 'GET' || (method === 'POST' && key)) && [0, 429, 502, 503, 504].includes(status)
  );
}
/** Fixed-width temporal buckets. Each displayed point is the mean of one bucket. */
export function reducePoints(points: Point[], end: number, limit = CONFIG.chartPoints): Point[] {
  const width = CONFIG.windowMs / limit;
  const buckets = new Map<number, { point: Point; n: number }>();
  for (const p of points) {
    if (p.ts <= end - CONFIG.windowMs || p.ts > end) continue;
    const key = Math.min(limit - 1, Math.floor((p.ts - (end - CONFIG.windowMs)) / width));
    const b = buckets.get(key);
    if (b) {
      b.point.p95 += p.p95;
      b.point.errRate += p.errRate;
      b.point.rps += p.rps;
      b.n++;
      b.point.ts = Math.max(b.point.ts, p.ts);
    } else buckets.set(key, { point: { ...p }, n: 1 });
  }
  return [...buckets]
    .sort(([a], [b]) => a - b)
    .map(([, { point: p, n }]) => ({
      ts: p.ts,
      p95: p.p95 / n,
      errRate: p.errRate / n,
      rps: p.rps / n,
    }));
}
function matches(c: Condition, value: number): boolean {
  switch (c.operator) {
    case '>':
      return value > c.threshold;
    case '>=':
      return value >= c.threshold;
    case '<':
      return value < c.threshold;
    case '<=':
      return value <= c.threshold;
  }
}
export class RuleEvaluator {
  private states = new Map<string, { since: (number | null)[]; fired: boolean; last: number }>();
  clear(): void {
    this.states.clear();
  }
  evaluate(rules: Rule[], event: MetricEvent): Incident[] {
    const incidents: Incident[] = [];
    for (const rule of rules) {
      const key = `${rule.id}:${event.serviceId}`;
      let state = this.states.get(key);
      if (!state) {
        state = { since: rule.conditions.map(() => null), fired: false, last: 0 };
        this.states.set(key, state);
      }
      if (event.ts < state.last) continue;
      // A period without observations cannot prove a condition held continuously.
      if (event.ts - state.last > CONFIG.staleMs) {
        state.since.fill(null);
        state.fired = false;
      }
      state.last = event.ts;
      let all = true;
      rule.conditions.forEach((c, i) => {
        if (matches(c, event[c.metric])) state.since[i] ??= event.ts;
        else state.since[i] = null;
        if (
          state.since[i] === null ||
          event.ts - (state.since[i] ?? event.ts) < c.durationSec * 1000
        )
          all = false;
      });
      if (all && !state.fired) {
        state.fired = true;
        incidents.push({
          id: `${key}:${event.seq}`,
          rule: rule.name,
          serviceId: event.serviceId,
          ts: event.ts,
        });
      }
      if (state.since.some((s) => s === null)) state.fired = false;
    }
    return incidents;
  }
}
export type TransportKind = 'ws' | 'sse' | 'poll';
export class FallbackPolicy {
  failures = 0;
  failed(kind: TransportKind, timedOut = false): TransportKind {
    this.failures++;
    if (kind === 'ws' && (this.failures >= 3 || timedOut)) {
      this.failures = 0;
      return 'sse';
    }
    if (kind === 'sse' && this.failures >= 3) {
      this.failures = 0;
      return 'poll';
    }
    return kind;
  }
  success(): void {
    this.failures = 0;
  }
  higher(kind: TransportKind): TransportKind | null {
    return kind === 'poll' ? 'sse' : kind === 'sse' ? 'ws' : null;
  }
}
