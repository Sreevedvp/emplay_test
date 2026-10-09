import { afterEach, describe, expect, it, vi } from 'vitest';
import { FormControl, FormGroup } from '@angular/forms';
import { firstValueFrom, of, throwError } from 'rxjs';
import { ApiClient } from './api';
import { backoff, FallbackPolicy, reducePoints, retryable, RuleEvaluator } from './algorithms';
import { CONFIG, MetricEvent, Rule, Session } from './models';
import { Integrity, isMetric } from './integrity';
import { meaningfulName, metricThreshold, uniqueName } from './validators';
import { Coordinator } from './coordinator';
import { TransportSink } from './transports';
const event = (offset: number, seq = offset, ts = 1000): MetricEvent => ({
  type: 'metric',
  serviceId: 'svc-0001',
  offset,
  seq,
  ts,
  p95: 500,
  errRate: 2,
  rps: 100,
  status: 'healthy',
});
const session: Session = {
  accessToken: 'old',
  refreshToken: 'refresh',
  expiresIn: 60,
  user: { id: 'u-1', email: 'operator@pulse.dev', role: 'operator' },
};
afterEach(() => vi.useRealTimers());
describe('HTTP and auth', () => {
  it('refreshes exactly once for ten simultaneous expired requests and replays each once', async () => {
    let refreshes = 0,
      rejected = 0,
      successful = 0;
    const api = new ApiClient(async (input, init) => {
      if (String(input).endsWith('/auth/refresh')) {
        refreshes++;
        await new Promise((resolve) => setTimeout(resolve, 5));
        return Response.json({ ...session, accessToken: 'new' });
      }
      if (new Headers(init?.headers).get('Authorization') === 'Bearer old') {
        rejected++;
        return Response.json(
          { error: { code: 'token_expired', message: 'Expired' } },
          { status: 401 },
        );
      }
      successful++;
      return Response.json({ ok: true });
    });
    api.session = session;
    const results = await Promise.all(Array.from({ length: 10 }, () => api.request('/services')));
    expect(results).toHaveLength(10);
    expect(refreshes).toBe(1);
    expect(rejected).toBe(10);
    expect(successful).toBe(10);
  });
  it('clears session when refresh fails', async () => {
    const api = new ApiClient(async (input) =>
      Response.json(
        {
          error: {
            code: String(input).endsWith('/auth/refresh') ? 'refresh_invalid' : 'token_expired',
          },
        },
        { status: 401 },
      ),
    );
    api.session = session;
    await expect(api.request('/services')).rejects.toThrow();
    expect(api.session).toBeNull();
  });
  it('retries only safe methods and listed statuses', () => {
    for (const status of [0, 429, 502, 503, 504]) expect(retryable('GET', status)).toBe(true);
    for (const status of [400, 401, 403, 404, 409, 410, 422, 500])
      expect(retryable('GET', status)).toBe(false);
    expect(retryable('POST', 503)).toBe(false);
    expect(retryable('POST', 503, true)).toBe(true);
    expect(retryable('PUT', 503, true)).toBe(false);
  });
  it('honors Retry-After and caps total attempts at three', async () => {
    vi.useFakeTimers();
    const send = vi.fn(async () =>
      Response.json(
        { error: { message: 'Busy' } },
        { status: 429, headers: { 'Retry-After': '2' } },
      ),
    );
    const api = new ApiClient(send);
    api.session = session;
    const result = api.request('/services').catch((e) => e);
    await vi.advanceTimersByTimeAsync(1999);
    expect(send).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(2001);
    expect(send).toHaveBeenCalledTimes(3);
    await result;
  });
  it('does not resend an unkeyed POST or a GET that returns 500', async () => {
    for (const [method, status] of [
      ['POST', 503],
      ['GET', 500],
    ] as const) {
      const send = vi.fn(async () => Response.json({ error: { message: 'failure' } }, { status }));
      const api = new ApiClient(send);
      api.session = session;
      await expect(api.request('/rules', { method })).rejects.toThrow();
      expect(send).toHaveBeenCalledTimes(1);
    }
  });
});
describe('stream ordering', () => {
  it('holds shuffled events and drains in order without duplicates', () => {
    const integrity = new Integrity();
    expect(integrity.push(event(2), 100)).toEqual([]);
    expect(integrity.push(event(1), 200).map((e) => e.offset)).toEqual([1, 2]);
    expect(integrity.push(event(2), 300)).toEqual([]);
    expect(integrity.duplicates).toBe(1);
  });
  it('detects a gap after one second, then accepts replay', () => {
    const integrity = new Integrity();
    integrity.push(event(2), 100);
    expect(integrity.gapDue(1099)).toBe(false);
    expect(integrity.gapDue(1100)).toBe(true);
    integrity.push(event(1), 1200);
    expect(integrity.gapDue(2000)).toBe(false);
    expect(integrity.offset).toBe(2);
  });
  it('rejects invalid sequences without advancing the cursor', () => {
    const integrity = new Integrity();
    integrity.push(event(1), 100);
    integrity.push(event(2, 4), 200);
    expect(integrity.offset).toBe(1);
    expect(integrity.sequenceErrors).toBe(1);
  });
  it('validates malformed data', () => {
    expect(isMetric(event(1))).toBe(true);
    expect(isMetric({ type: 'metric' })).toBe(false);
    expect(isMetric({ ...event(1), p95: NaN })).toBe(false);
    expect(isMetric({ ...event(1), errRate: 101 })).toBe(false);
  });
});
describe('transport recovery', () => {
  it('backs off with jitter and a 30-second cap', () => {
    expect(backoff(1, 0)).toBe(250);
    expect(backoff(1, 1)).toBe(500);
    expect(backoff(20, 1)).toBe(30000);
  });
  it('falls from WS to SSE to polling and resets failures after success', () => {
    const policy = new FallbackPolicy();
    expect(policy.failed('ws')).toBe('ws');
    expect(policy.failed('ws')).toBe('ws');
    expect(policy.failed('ws')).toBe('sse');
    expect(policy.failed('sse')).toBe('sse');
    policy.failed('sse');
    expect(policy.failed('sse')).toBe('poll');
    expect(policy.higher('poll')).toBe('sse');
    expect(policy.higher('sse')).toBe('ws');
    policy.success();
    expect(policy.failures).toBe(0);
    expect(policy.failed('ws', true)).toBe('sse');
  });
  it('uses fake transports to fall back, probe higher, detect silence and pause offline', async () => {
    vi.useFakeTimers();
    const calls: { kind: string; sink: TransportSink; close: ReturnType<typeof vi.fn> }[] = [];
    const coordinator = new Coordinator(
      (kind, sink) => {
        const close = vi.fn();
        calls.push({ kind, sink, close });
        return { close };
      },
      (raw) => JSON.parse(raw).type === 'ping',
      () => undefined,
      () => undefined,
      () => undefined,
    );
    coordinator.start();
    for (let i = 0; i < 6; i++) {
      calls.at(-1)!.sink.failed(new Error('down'));
      await vi.advanceTimersByTimeAsync(2000);
    }
    expect(calls.at(-1)!.kind).toBe('poll');
    calls.at(-1)!.sink.opened();
    await vi.advanceTimersByTimeAsync(30000);
    expect(calls.at(-1)!.kind).toBe('sse');
    calls.at(-1)!.sink.opened();
    calls.at(-1)!.sink.message('{"type":"ping","ts":1}');
    expect(coordinator.state.kind).toBe('sse');
    for (let i = 0; i < 6; i++) {
      await vi.advanceTimersByTimeAsync(5000);
      const active = calls.findLast((c) => c.kind === 'sse')!;
      active.sink.message('{"type":"ping","ts":1}');
    }
    expect(calls.at(-1)!.kind).toBe('ws');
    calls.at(-1)!.sink.opened();
    calls.at(-1)!.sink.message('{"type":"ping","ts":1}');
    expect(coordinator.state.kind).toBe('ws');
    await vi.advanceTimersByTimeAsync(11000);
    expect(coordinator.state.connected).toBe(false);
    coordinator.offline(true);
    const count = calls.length;
    await vi.advanceTimersByTimeAsync(60000);
    expect(calls.length).toBe(count);
    coordinator.offline(false);
    expect(calls.at(-1)!.kind).toBe('ws');
    coordinator.stop();
  });
});
describe('rules and charts', () => {
  it('validates trimmed names and metric-dependent threshold bounds', () => {
    expect(meaningfulName(new FormControl('  '))).toEqual({ meaningfulName: true });
    expect(meaningfulName(new FormControl('Latency'))).toBeNull();
    const group = new FormGroup({
      metric: new FormControl('errRate'),
      threshold: new FormControl(101),
    });
    expect(metricThreshold(group)).toEqual({ metricThreshold: true });
    group.controls.metric.setValue('p95');
    expect(metricThreshold(group)).toBeNull();
  });
  it('debounces async uniqueness and treats server failures as invalid', async () => {
    vi.useFakeTimers();
    for (const [available, expected] of [
      [true, null],
      [false, { nameTaken: true }],
    ] as const) {
      const check = vi.fn(() => of({ available }));
      const validation = firstValueFrom(
        uniqueName(check)(new FormControl('Rule')) as ReturnType<typeof of>,
      );
      expect(check).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(350);
      expect(await validation).toEqual(expected);
    }
    const failed = firstValueFrom(
      uniqueName(() => throwError(() => new Error('offline')))(
        new FormControl('Rule'),
      ) as ReturnType<typeof of>,
    );
    await vi.advanceTimersByTimeAsync(350);
    expect(await failed).toEqual({ nameCheckUnavailable: true });
  });
  it('reduces 15 minutes to at most 300 ordered mean buckets', () => {
    const end = CONFIG.windowMs;
    const points = Array.from({ length: 9000 }, (_, i) => ({
      ts: i * 100,
      p95: 100,
      errRate: 2,
      rps: 40,
    }));
    const result = reducePoints(points, end);
    expect(result.length).toBeLessThanOrEqual(300);
    expect(result[0].p95).toBe(100);
    expect(result.every((p, i) => !i || p.ts > result[i - 1].ts)).toBe(true);
  });
  it('fires once per sustained episode and rearms after recovery', () => {
    const evaluator = new RuleEvaluator();
    const rules: Rule[] = [
      {
        id: 'r-1',
        name: 'Latency',
        createdAt: 0,
        updatedAt: 0,
        conditions: [{ metric: 'p95', operator: '>', threshold: 400, durationSec: 2 }],
      },
    ];
    expect(evaluator.evaluate(rules, event(1, 1, 1000))).toEqual([]);
    expect(evaluator.evaluate(rules, event(2, 2, 3000))).toHaveLength(1);
    expect(evaluator.evaluate(rules, event(3, 3, 4000))).toEqual([]);
    evaluator.evaluate(rules, { ...event(4, 4, 5000), p95: 0 });
    expect(evaluator.evaluate(rules, event(5, 5, 6000))).toEqual([]);
    expect(evaluator.evaluate(rules, event(6, 6, 8000))).toHaveLength(1);
  });
  it('does not count an unobserved gap toward alert duration', () => {
    const evaluator = new RuleEvaluator();
    const rules: Rule[] = [
      {
        id: 'r',
        name: 'Gap',
        createdAt: 0,
        updatedAt: 0,
        conditions: [{ metric: 'p95', operator: '>', threshold: 1, durationSec: 10 }],
      },
    ];
    evaluator.evaluate(rules, event(1, 1, 1000));
    expect(evaluator.evaluate(rules, event(2, 2, 20000))).toEqual([]);
  });
});

describe('recovery edge cases', () => {
  it('flags sequence corruption for snapshot recovery and clears the fault on reset', () => {
    const integrity = new Integrity();
    integrity.push(event(1), 100);
    integrity.push(event(2, 4), 200);
    expect(integrity.sequenceFault).toBe(true);
    expect(integrity.gapDue(200)).toBe(true);
    integrity.reset({ offset: 2, ts: 200, metrics: [event(2, 4)] });
    expect(integrity.sequenceFault).toBe(false);
    expect(integrity.push(event(3, 5), 300)).toHaveLength(1);
  });
  it('bounds pending memory and reports overflow instead of silently advancing', () => {
    const integrity = new Integrity();
    for (let offset = 2; offset <= CONFIG.maxPending + 2; offset++)
      integrity.push(event(offset), 100);
    expect(integrity.size).toBe(CONFIG.maxPending);
    expect(integrity.offset).toBe(0);
    expect(integrity.overflow).toBe(true);
    integrity.clear();
    expect(integrity.overflow).toBe(false);
  });
  it('gives a newly promoted metric-producing transport a full heartbeat grace period', async () => {
    vi.useFakeTimers();
    const sinks: TransportSink[] = [];
    const coordinator = new Coordinator(
      (_kind, sink) => {
        sinks.push(sink);
        return { close: () => undefined };
      },
      (raw) => JSON.parse(raw).type === 'ping',
      () => undefined,
      () => undefined,
      () => undefined,
    );
    coordinator.start();
    sinks.at(-1)!.failed(new Error('timeout'), true);
    await vi.advanceTimersByTimeAsync(1000);
    sinks.at(-1)!.opened();
    for (let i = 0; i < 6; i++) {
      sinks.at(-1)!.message('{"type":"ping","ts":1}');
      await vi.advanceTimersByTimeAsync(5000);
    }
    sinks.at(-1)!.opened();
    sinks.at(-1)!.message(JSON.stringify(event(1)));
    expect(coordinator.state.kind).toBe('ws');
    await vi.advanceTimersByTimeAsync(1000);
    expect(coordinator.state.connected).toBe(true);
    coordinator.stop();
  });
});
