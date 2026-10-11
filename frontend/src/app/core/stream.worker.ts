/// <reference lib="webworker" />
import { ApiError } from './api';
import { backoff } from './algorithms';
import { Coordinator } from './coordinator';
import { isMetric } from './integrity';
import { CONFIG, Replay, Snapshot } from './models';
import { StreamProcessor } from './stream-processor';
import { FromStreamWorker, ToStreamWorker } from './stream-protocol';
import { createTransportFactory } from './transports';
import { WorkerApi } from './worker-api';

const send = (message: FromStreamWorker): void => postMessage(message);
const processor = new StreamProcessor();
let api: WorkerApi;
let coordinator: Coordinator | undefined;
let online = true;
let started = false;
let recovering: Promise<void> | null = null;
let recoveryFailures = 0;
let recoveryAfter = 0;

async function resnapshot(): Promise<void> {
  processor.snapshot(await api.request<Snapshot>('/metrics/snapshot'), true);
  send({
    type: 'reset',
    message:
      'Replay unavailable or buffer/sequence invalid. Snapshot restored; alert timers reset and chart history reloaded.',
  });
}
function recover(): Promise<void> {
  if (recovering) return recovering;
  if (!online || Date.now() < recoveryAfter) return Promise.resolve();
  recovering = (async () => {
    try {
      if (processor.integrity.overflow || processor.integrity.sequenceFault) await resnapshot();
      else {
        const result = await api.request<Replay>(
          `/metrics/replay?since=${processor.integrity.offset}`,
        );
        if (!Array.isArray(result.events) || !Number.isInteger(result.offset))
          throw new Error('Invalid replay response');
        processor.replayed();
        for (let i = 0; i < result.events.length; i += CONFIG.replayBatch) {
          for (let j = i; j < Math.min(i + CONFIG.replayBatch, result.events.length); j++) {
            const event = result.events[j];
            if (!isMetric(event)) throw new Error('Malformed replay response');
            processor.ingest(event, Date.now());
          }
          await new Promise<void>((resolve) => setTimeout(resolve, 0));
          if (processor.integrity.overflow || processor.integrity.sequenceFault) {
            await resnapshot();
            break;
          }
        }
      }
      recoveryFailures = 0;
      recoveryAfter = 0;
      send({ type: 'error', message: '' });
    } catch (error) {
      if (error instanceof ApiError && error.status === 410) {
        try {
          await resnapshot();
          recoveryFailures = 0;
          recoveryAfter = 0;
          send({ type: 'error', message: '' });
          return;
        } catch (snapshotError) {
          error = snapshotError;
        }
      }
      recoveryAfter = Date.now() + backoff(++recoveryFailures);
      send({
        type: 'error',
        message: `Gap recovery failed; data may be incomplete: ${String(error)}`,
      });
    }
  })().finally(() => {
    recovering = null;
  });
  return recovering;
}
async function start(message: Extract<ToStreamWorker, { type: 'start' }>): Promise<void> {
  if (started) return;
  started = true;
  online = message.online;
  api = new WorkerApi(message.accessToken, send);
  processor.setRules(message.rules);
  processor.select(message.selection);
  try {
    processor.snapshot(await api.request<Snapshot>('/metrics/snapshot'));
    coordinator = new Coordinator(
      createTransportFactory(api),
      (raw) => processor.receive(raw, Date.now()),
      () => void recover(),
      (state) => send({ type: 'connection', state }),
      () => void recover(),
    );
    if (!online) coordinator.offline(true);
    coordinator.start();
    setInterval(() => {
      const batch = processor.takeBatch();
      if (batch) send(batch);
    }, CONFIG.renderMs);
    setInterval(() => {
      if (processor.integrity.gapDue(Date.now())) void recover();
    }, CONFIG.workerCheckMs);
  } catch (error) {
    send({ type: 'fatal', message: `Stream worker could not start: ${String(error)}` });
  }
}
addEventListener('message', ({ data }: MessageEvent<ToStreamWorker>) => {
  if (data.type === 'start') {
    void start(data);
    return;
  }
  if (!started) return;
  switch (data.type) {
    case 'online':
      online = data.online;
      coordinator?.offline(!online);
      break;
    case 'rules':
      processor.setRules(data.rules);
      break;
    case 'select':
      processor.select(data.selection);
      break;
    case 'ack':
      processor.acknowledge(data.id);
      break;
    default:
      api.accept(data);
  }
});
