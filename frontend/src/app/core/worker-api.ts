import { ApiError } from './api';
import type { StreamApi } from './transports';
import type { FromStreamWorker, ToStreamWorker } from './stream-protocol';
/** The UI owns credentials and one refresh promise; large JSON is decoded only here. */
export class WorkerApi implements StreamApi {
  session: { accessToken: string };
  private nextId = 0;
  private pending = new Map<
    number,
    {
      resolve: (value: ArrayBuffer | string) => void;
      reject: (error: Error) => void;
      cleanup: () => void;
    }
  >();
  constructor(
    accessToken: string,
    private send: (message: FromStreamWorker) => void,
  ) {
    this.session = { accessToken };
  }
  accept(message: ToStreamWorker): void {
    if (
      message.type !== 'response' &&
      message.type !== 'refreshed' &&
      message.type !== 'request-error'
    )
      return;
    const pending = this.pending.get(message.id);
    if (!pending) return;
    this.pending.delete(message.id);
    pending.cleanup();
    if (message.type === 'request-error')
      pending.reject(new ApiError(message.error.status, message.error.code, message.error.message));
    else {
      if (message.accessToken) this.session = { accessToken: message.accessToken };
      pending.resolve(message.type === 'response' ? message.buffer : message.accessToken);
    }
  }
  private call(
    message: { type: 'request'; path: string } | { type: 'refresh'; rejectedToken: string },
    signal?: AbortSignal,
  ): Promise<ArrayBuffer | string> {
    return new Promise((resolve, reject) => {
      if (signal?.aborted) {
        reject(signal.reason);
        return;
      }
      const id = ++this.nextId;
      const abort = () => {
        this.pending.delete(id);
        reject(signal?.reason);
      };
      signal?.addEventListener('abort', abort, { once: true });
      this.pending.set(id, {
        resolve,
        reject,
        cleanup: () => signal?.removeEventListener('abort', abort),
      });
      this.send({ ...message, id });
    });
  }
  async request<T>(path: string, init: RequestInit = {}, signal?: AbortSignal): Promise<T> {
    if (init.method && init.method !== 'GET') throw new Error('Worker requests must be reads');
    const result = await this.call({ type: 'request', path }, signal);
    if (!(result instanceof ArrayBuffer)) throw new Error('Invalid worker HTTP response');
    return JSON.parse(new TextDecoder().decode(result)) as T;
  }
  async refresh(rejectedToken: string): Promise<void> {
    const result = await this.call({ type: 'refresh', rejectedToken });
    if (typeof result !== 'string') throw new Error('Invalid worker refresh response');
    this.session = { accessToken: result };
  }
  async openStream(path: string, signal: AbortSignal): Promise<Response> {
    const send = () =>
      fetch(`/api${path}`, {
        headers: { Authorization: `Bearer ${this.session.accessToken}` },
        signal,
      });
    const token = this.session.accessToken;
    let response = await send();
    if (response.status === 401) {
      await this.refresh(token);
      if (signal.aborted) throw signal.reason;
      response = await send();
    }
    if (!response.ok)
      throw new ApiError(response.status, 'stream_failed', 'Stream connection failed');
    return response;
  }
}
