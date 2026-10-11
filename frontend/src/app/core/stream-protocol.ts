import type { ConnectionState } from './coordinator';
import type { Incident, MetricEvent, Point, Rule } from './models';
export interface StreamCounters {
  duplicates: number;
  reordered: number;
  malformed: number;
  replays: number;
  resets: number;
  pending: number;
  sequenceErrors: number;
  applied: number;
}
export const emptyCounters = (): StreamCounters => ({
  duplicates: 0,
  reordered: 0,
  malformed: 0,
  replays: 0,
  resets: 0,
  pending: 0,
  sequenceErrors: 0,
  applied: 0,
});
export interface Selection {
  id: number;
  serviceId: string | null;
}
export interface DisplayBatch {
  type: 'batch';
  id: number;
  changes: MetricEvent[];
  incidents: Incident[];
  detail: Selection & { points: Point[] };
  lastEvent: number;
  counters: StreamCounters;
}
export interface RemoteError {
  status: number;
  code: string;
  message: string;
}
export type ToStreamWorker =
  | { type: 'start'; accessToken: string; online: boolean; rules: Rule[]; selection: Selection }
  | { type: 'online'; online: boolean }
  | { type: 'rules'; rules: Rule[] }
  | { type: 'select'; selection: Selection }
  | { type: 'ack'; id: number }
  | { type: 'response'; id: number; buffer: ArrayBuffer; accessToken?: string }
  | { type: 'refreshed'; id: number; accessToken: string }
  | { type: 'request-error'; id: number; error: RemoteError };
export type FromStreamWorker =
  | DisplayBatch
  | { type: 'connection'; state: ConnectionState }
  | { type: 'error'; message: string }
  | { type: 'fatal'; message: string }
  | { type: 'reset'; message: string }
  | { type: 'request'; id: number; path: string }
  | { type: 'refresh'; id: number; rejectedToken: string };
