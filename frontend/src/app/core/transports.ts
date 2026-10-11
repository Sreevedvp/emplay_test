import { CONFIG, Snapshot } from './models';
import { TransportKind } from './algorithms';
export interface StreamApi {
  session: { accessToken: string } | null;
  refresh(rejectedToken: string): Promise<void>;
  request<T>(path: string, init?: RequestInit, signal?: AbortSignal): Promise<T>;
  openStream(path: string, signal: AbortSignal): Promise<Response>;
}
export interface TransportSink {
  opened(): void;
  message(raw: string): void;
  failed(error: Error, timedOut?: boolean): void;
  snapshot(snapshot: Snapshot): void;
}
export interface Transport {
  close(): void;
}
export type TransportFactory = (kind: TransportKind, sink: TransportSink) => Transport;
/** Factories contain wire-specific details; the coordinator consumes this interface only. */
export function createTransportFactory(api: StreamApi): TransportFactory {
  return (kind, sink) => {
    let closed = false;
    const abort = new AbortController();
    let socket: WebSocket | undefined;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const fail = (e: unknown, timeout = false) => {
      if (!closed) sink.failed(e instanceof Error ? e : new Error(String(e)), timeout);
    };
    if (kind === 'ws') {
      const url = new URL('/api/stream', location.href);
      url.protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
      const token = api.session?.accessToken ?? '';
      url.searchParams.set('access_token', token);
      socket = new WebSocket(url);
      timer = setTimeout(
        () => fail(new Error('WebSocket connect timeout'), true),
        CONFIG.connectTimeout,
      );
      socket.onopen = () => {
        clearTimeout(timer);
        if (!closed) sink.opened();
      };
      socket.onmessage = (e) => {
        if (!closed) sink.message(String(e.data));
      };
      socket.onerror = () => undefined; // onclose owns the one failure notification.
      socket.onclose = (e) => {
        if (closed) return;
        if (e.code === 4401)
          void api.refresh(token).then(() => fail(new Error('Token renewed')), fail);
        else fail(new Error(`WebSocket closed (${e.code})`));
      };
    } else if (kind === 'sse') {
      timer = setTimeout(() => fail(new Error('SSE connect timeout'), true), CONFIG.connectTimeout);
      void (async () => {
        const response = await api.openStream('/stream/sse', abort.signal);
        if (closed) return;
        clearTimeout(timer);
        sink.opened();
        const reader = response.body?.getReader();
        if (!reader) throw new Error('SSE body missing');
        const decoder = new TextDecoder();
        let buffer = '';
        try {
          while (!closed) {
            const part = await reader.read();
            if (part.done) throw new Error('SSE disconnected');
            buffer += decoder.decode(part.value, { stream: true }).replace(/\r/g, '');
            let boundary = buffer.indexOf('\n\n');
            while (boundary >= 0) {
              const event = buffer.slice(0, boundary);
              buffer = buffer.slice(boundary + 2);
              const data = event
                .split('\n')
                .filter((line) => line.startsWith('data:'))
                .map((line) => line.slice(5).trimStart())
                .join('\n');
              if (data) sink.message(data);
              boundary = buffer.indexOf('\n\n');
            }
            if (buffer.length > 1000000) throw new Error('SSE frame too large');
          }
        } finally {
          await reader.cancel().catch(() => undefined);
        }
      })().catch(fail);
    } else {
      let first = true;
      const poll = async () => {
        try {
          const snapshot = await api.request<Snapshot>('/metrics/snapshot', {}, abort.signal);
          if (closed) return;
          if (first) {
            first = false;
            sink.opened();
          }
          sink.snapshot(snapshot);
          timer = setTimeout(() => void poll(), CONFIG.pollMs);
        } catch (e) {
          fail(e);
        }
      };
      void poll();
    }
    return {
      close: () => {
        closed = true;
        clearTimeout(timer);
        abort.abort();
        if (socket) {
          socket.onclose = null;
          socket.close();
        }
      },
    };
  };
}
