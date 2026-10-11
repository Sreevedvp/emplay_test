export type Status = 'healthy' | 'degraded' | 'down';
export type Metric = 'p95' | 'errRate' | 'rps';
export interface Point {
  ts: number;
  p95: number;
  errRate: number;
  rps: number;
}
export interface MetricEvent extends Point {
  type: 'metric';
  offset: number;
  serviceId: string;
  seq: number;
  status: Status;
}
export interface Service {
  id: string;
  name: string;
  team: string;
  status: Status;
}
export interface Snapshot {
  offset: number;
  ts: number;
  metrics: MetricEvent[];
}
export interface Replay {
  offset: number;
  events: MetricEvent[];
}
export interface User {
  id: string;
  email: string;
  role: 'viewer' | 'operator';
}
export interface Session {
  accessToken: string;
  refreshToken: string;
  expiresIn: number;
  user: User;
}
export interface Condition {
  metric: Metric;
  operator: '>' | '>=' | '<' | '<=';
  threshold: number;
  durationSec: number;
  warning?: number;
}
export interface Rule {
  id: string;
  name: string;
  conditions: Condition[];
  createdAt: number;
  updatedAt: number;
}
export interface Incident {
  id: string;
  rule: string;
  serviceId: string;
  ts: number;
}
export const CONFIG = {
  requestTimeout: 10000,
  attempts: 3,
  staleMs: 10000,
  connectTimeout: 5000,
  heartbeatTimeout: 10500,
  reorderMs: 1000,
  retryBase: 500,
  retryCap: 30000,
  upgradeMs: 30000,
  pollMs: 2000,
  renderMs: 250,
  workerCheckMs: 500,
  replayBatch: 500,
  chartMs: 1000,
  chartPoints: 300,
  windowMs: 900000,
  maxPending: 15000,
  incidentLimit: 200,
} as const;
