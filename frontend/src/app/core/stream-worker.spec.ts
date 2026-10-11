import { afterEach, describe, expect, it, vi } from 'vitest';
import { ApiClient } from './api';
import { CONFIG, MetricEvent, Rule, Session, Snapshot } from './models';
import { StreamClient, WorkerPort } from './stream-client';
import { StreamProcessor } from './stream-processor';
import { FromStreamWorker, ToStreamWorker } from './stream-protocol';
import { WorkerApi } from './worker-api';
const event = (
  offset: number,
  serviceId = 'svc-0001',
  seq = offset,
  ts = offset * 1000,
): MetricEvent => ({
  type: 'metric',
  offset,
  serviceId,
  seq,
  ts,
  p95: 500,
  errRate: 2,
  rps: 100,
  status: 'degraded',
});
const rule: Rule = {
  id: 'r-1',
  name: 'Latency',
  conditions: [{ metric: 'p95', operator: '>', threshold: 400, durationSec: 0 }],
  createdAt: 0,
  updatedAt: 0,
};
const session: Session = {
  accessToken: 'old',
  refreshToken: 'refresh',
  expiresIn: 60,
  user: { id: 'u-1', email: 'operator@pulse.dev', role: 'operator' },
};
const snapshot = (): Snapshot => ({
  offset: 2000,
  ts: 1000,
  metrics: Array.from({ length: 2000 }, (_, i) =>
    event(i + 1, `svc-${String(i + 1).padStart(4, '0')}`, 1, 1000),
  ),
});
afterEach(() => vi.restoreAllMocks());
describe('worker-owned processor', () => {
  it('parses, orders and deduplicates before coalescing display values', () => {
    const processor = new StreamProcessor();
    processor.select({ id: 1, serviceId: 'svc-0001' });
    processor.receive(JSON.stringify(event(1)), 1000);
    processor.receive(JSON.stringify(event(3)), 3000);
    processor.receive(JSON.stringify(event(3)), 3001);
    expect(processor.integrity.offset).toBe(1);
    processor.receive(JSON.stringify(event(2)), 3002);
    const batch = processor.takeBatch()!;
    expect(batch.changes.map((e) => e.offset)).toEqual([3]);
    expect(batch.detail.points.map((p) => p.ts)).toEqual([1000, 2000, 3000]);
    expect(batch.counters.applied).toBe(3);
    expect(batch.counters.duplicates).toBe(1);
  });
  it('evaluates intermediate events even if only the last value is displayed', () => {
    const processor = new StreamProcessor();
    processor.setRules([rule]);
    processor.receive(JSON.stringify(event(1)), 1000);
    processor.receive(JSON.stringify({ ...event(2), p95: 0 }), 2000);
    processor.receive(JSON.stringify(event(3)), 3000);
    const batch = processor.takeBatch()!;
    expect(batch.changes).toHaveLength(1);
    expect(batch.incidents.map((i) => i.id)).toEqual(['r-1:svc-0001:3', 'r-1:svc-0001:1']);
    processor.acknowledge(batch.id);
    processor.setRules([structuredClone(rule)]);
    processor.receive(JSON.stringify(event(4)), 4000);
    expect(processor.takeBatch()!.incidents).toEqual([]);
  });
  it('keeps duration-based evaluation correct through out-of-order replay', () => {
    const processor = new StreamProcessor();
    processor.setRules([{ ...rule, conditions: [{ ...rule.conditions[0], durationSec: 2 }] }]);
    processor.receive(JSON.stringify(event(1)), 1000);
    processor.receive(JSON.stringify(event(3)), 3000);
    processor.ingest({ ...event(2), p95: 0 }, 3100); // replayed false condition resets duration
    processor.ingest(event(4), 4000);
    expect(processor.takeBatch()!.incidents).toEqual([]);
    processor.ingest(event(5), 5000);
    processor.acknowledge(1);
    expect(processor.takeBatch()!.incidents.map((i) => i.id)).toEqual(['r-1:svc-0001:5']);
  });
  it('bounds unacknowledged display data across 20,000 events', () => {
    const processor = new StreamProcessor();
    processor.setRules([rule]);
    const first = processor.takeBatch()!;
    for (let i = 1; i <= 20000; i++)
      processor.ingest(
        event(
          i,
          `svc-${String(((i - 1) % 2000) + 1).padStart(4, '0')}`,
          Math.floor((i - 1) / 2000) + 1,
          1000 + i,
        ),
        1000 + i,
      );
    expect(processor.takeBatch()).toBeNull();
    processor.acknowledge(first.id + 99);
    expect(processor.takeBatch()).toBeNull();
    processor.acknowledge(first.id);
    const batch = processor.takeBatch()!;
    expect(batch.changes).toHaveLength(2000);
    expect(batch.counters.applied).toBe(20000);
    expect(batch.incidents.length).toBe(CONFIG.incidentLimit);
    expect(batch.detail.points).toEqual([]);
    expect(batch.changes.every((e) => e.seq === 10)).toBe(true);
  });
  it('sends points for the selected service only and preserves selection generations', () => {
    const processor = new StreamProcessor();
    processor.select({ id: 1, serviceId: 'svc-0001' });
    processor.ingest(event(1), 1000);
    const old = processor.takeBatch()!;
    processor.select({ id: 3, serviceId: 'svc-0002' });
    processor.ingest(event(2), 2000);
    processor.ingest(event(3, 'svc-0002', 1), 3000);
    processor.acknowledge(old.id);
    const next = processor.takeBatch()!;
    expect(old.detail.id).toBe(1);
    expect(next.detail.id).toBe(3);
    expect(next.detail.points.map((p) => p.ts)).toEqual([3000]);
  });
  it('bounds selected chart seconds while the main thread is blocked', () => {
    const processor = new StreamProcessor();
    processor.select({ id: 1, serviceId: 'svc-0001' });
    const first = processor.takeBatch()!;
    for (let i = 1; i <= 2000; i++) processor.ingest(event(i), i * 1000);
    processor.acknowledge(first.id);
    expect(processor.takeBatch()!.detail.points.length).toBeLessThanOrEqual(901);
  });
  it('counts bad JSON and missing fields without breaking heartbeat or metrics', () => {
    vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    const processor = new StreamProcessor();
    expect(processor.receive('{bad', 1000)).toBe(false);
    processor.receive('{"type":"metric"}', 1000);
    expect(processor.receive('{"type":"ping","ts":1000}', 1000)).toBe(true);
    processor.receive(JSON.stringify(event(1)), 1000);
    expect(processor.takeBatch()!.counters).toMatchObject({ malformed: 2, applied: 1 });
  });
  it('validates snapshots and resets alert duration after a recovery boundary', () => {
    const processor = new StreamProcessor();
    processor.setRules([rule]);
    processor.snapshot(snapshot());
    expect(processor.takeBatch()!.changes).toHaveLength(2000);
    processor.ingest(event(2001, 'svc-0001', 2, 2000), 2000);
    processor.snapshot(
      {
        ...snapshot(),
        offset: 2001,
        metrics: snapshot().metrics.map((e) =>
          e.serviceId === 'svc-0001' ? event(2001, 'svc-0001', 2, 2000) : e,
        ),
      },
      true,
    );
    expect(processor.counters().resets).toBe(1);
    expect(() => processor.snapshot({ ...snapshot(), metrics: [] })).toThrow('Invalid snapshot');
  });
});
class FakeWorker implements WorkerPort {
  onmessage: WorkerPort['onmessage'] = null;
  onerror: WorkerPort['onerror'] = null;
  onmessageerror: WorkerPort['onmessageerror'] = null;
  messages: { message: ToStreamWorker; transfer?: Transferable[] }[] = [];
  terminate = vi.fn();
  postMessage(message: ToStreamWorker, transfer?: Transferable[]): void {
    this.messages.push({ message, transfer });
  }
  emit(data: FromStreamWorker): void {
    this.onmessage?.({ data } as MessageEvent<FromStreamWorker>);
  }
}
describe('worker/UI boundary', () => {
  it('transfers raw HTTP bytes and parses replay JSON in the worker API', async () => {
    const response = Response.json({ events: [event(1)], offset: 1 });
    const json = vi.spyOn(response, 'json');
    const api = new ApiClient(async () => response);
    api.session = session;
    const worker = new FakeWorker();
    const client = new StreamClient(
      api,
      () => undefined,
      () => undefined,
      () => worker,
    );
    worker.emit({ type: 'request', id: 1, path: '/metrics/replay?since=0' });
    await vi.waitFor(() => expect(worker.messages).toHaveLength(1));
    const reply = worker.messages[0];
    expect(reply.message.type).toBe('response');
    expect(json).not.toHaveBeenCalled();
    if (reply.message.type !== 'response') throw new Error('Expected response');
    expect(reply.transfer).toEqual([reply.message.buffer]);
    let sent: FromStreamWorker | undefined;
    const remote = new WorkerApi('old', (message) => {
      sent = message;
    });
    const pending = remote.request<{ offset: number }>('/metrics/replay?since=0');
    expect(sent?.type).toBe('request');
    remote.accept(reply.message);
    expect((await pending).offset).toBe(1);
    client.close();
  });
  it('shares one refresh between worker streams and concurrent UI requests', async () => {
    let refreshes = 0;
    const api = new ApiClient(async (input, init) => {
      if (String(input).endsWith('/auth/refresh')) {
        refreshes++;
        await new Promise((resolve) => setTimeout(resolve, 5));
        return Response.json({ ...session, accessToken: 'new' });
      }
      if (new Headers(init?.headers).get('Authorization') === 'Bearer old')
        return Response.json(
          { error: { code: 'token_expired', message: 'Expired' } },
          { status: 401 },
        );
      return Response.json({ ok: true });
    });
    api.session = session;
    const worker = new FakeWorker();
    const client = new StreamClient(
      api,
      () => undefined,
      () => undefined,
      () => worker,
    );
    worker.emit({ type: 'refresh', id: 1, rejectedToken: 'old' });
    await Promise.all(Array.from({ length: 10 }, () => api.request('/services')));
    await vi.waitFor(() =>
      expect(worker.messages.some((m) => m.message.type === 'refreshed')).toBe(true),
    );
    expect(refreshes).toBe(1);
    client.close();
  });
  it('acknowledges only consumed batches and shuts down on worker failure', () => {
    const worker = new FakeWorker();
    const received = vi.fn();
    const failed = vi.fn();
    const client = new StreamClient(new ApiClient(), received, failed, () => worker);
    worker.emit(new StreamProcessor().takeBatch()!);
    expect(received).toHaveBeenCalledTimes(1);
    expect(worker.messages[0].message).toEqual({ type: 'ack', id: 1 });
    worker.onerror?.(new ErrorEvent('error', { cancelable: true }));
    expect(worker.terminate).toHaveBeenCalledOnce();
    expect(failed).toHaveBeenCalledOnce();
    client.send({ type: 'online', online: true });
    expect(worker.messages).toHaveLength(1);
  });
  it('aborts HTTP and ignores late responses when the worker/session is closed', async () => {
    let requestSignal: AbortSignal | undefined;
    let finish: ((response: Response) => void) | undefined;
    const api = new ApiClient((_input, init) => {
      requestSignal = init?.signal ?? undefined;
      return new Promise((resolve) => {
        finish = resolve;
      });
    });
    const worker = new FakeWorker();
    const client = new StreamClient(
      api,
      () => undefined,
      () => undefined,
      () => worker,
    );
    worker.emit({ type: 'request', id: 1, path: '/metrics/snapshot' });
    client.close();
    expect(requestSignal?.aborted).toBe(true);
    finish?.(Response.json(snapshot()));
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(worker.messages).toEqual([]);
    expect(worker.terminate).toHaveBeenCalledOnce();
  });
  it('preserves replay-too-old error status across the worker boundary', async () => {
    const api = new ApiClient(async () =>
      Response.json({ error: { code: 'replay_too_old', message: 'Expired' } }, { status: 410 }),
    );
    const worker = new FakeWorker();
    const client = new StreamClient(
      api,
      () => undefined,
      () => undefined,
      () => worker,
    );
    worker.emit({ type: 'request', id: 1, path: '/metrics/replay?since=0' });
    await vi.waitFor(() => expect(worker.messages).toHaveLength(1));
    expect(worker.messages[0].message).toMatchObject({
      type: 'request-error',
      id: 1,
      error: { status: 410, code: 'replay_too_old' },
    });
    client.close();
  });
});

it('uses credentials refreshed by the main thread during snapshot retrieval', async () => {
  const send = vi.fn();
  const api = new WorkerApi('expired', send);
  const request = api.request('/metrics/snapshot');
  const buffer = new ArrayBuffer(2);
  new Uint8Array(buffer).set([123, 125]);
  api.accept({
    type: 'response',
    id: send.mock.calls[0][0].id,
    buffer,
    accessToken: 'renewed',
  });
  await request;
  expect(api.session.accessToken).toBe('renewed');
});
