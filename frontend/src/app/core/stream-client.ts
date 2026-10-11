import { ApiClient, ApiError } from './api';
import { FromStreamWorker, ToStreamWorker } from './stream-protocol';
export interface WorkerPort {
  postMessage(message: ToStreamWorker, transfer?: Transferable[]): void;
  terminate(): void;
  onmessage: ((event: MessageEvent<FromStreamWorker>) => void) | null;
  onerror: ((event: ErrorEvent) => void) | null;
  onmessageerror: ((event: MessageEvent) => void) | null;
}
/** UI-side bridge: shared authenticated HTTP, transferable bodies, and lifecycle isolation. */
export class StreamClient {
  private controller = new AbortController();
  private closed = false;
  private worker: WorkerPort;
  constructor(
    private api: ApiClient,
    private receive: (message: FromStreamWorker) => void,
    private failed: (message: string) => void,
    factory: () => WorkerPort = () =>
      new Worker(new URL('./stream.worker', import.meta.url), { type: 'module' }),
  ) {
    this.worker = factory();
    this.worker.onmessage = ({ data }) => {
      if (this.closed) return;
      if (data.type === 'request' || data.type === 'refresh') {
        void this.respond(data);
        return;
      }
      if (data.type === 'fatal') {
        this.fail(data.message);
        return;
      }
      try {
        this.receive(data);
        if (data.type === 'batch') this.send({ type: 'ack', id: data.id });
      } catch (error) {
        this.fail(`Worker display update failed: ${String(error)}`);
      }
    };
    this.worker.onerror = (event) => {
      event.preventDefault();
      this.fail('Stream worker failed. Last values are retained; retry the fleet connection.');
    };
    this.worker.onmessageerror = () =>
      this.fail('Could not read a stream worker message. Retry the fleet connection.');
  }
  send(message: ToStreamWorker): void {
    if (!this.closed) this.worker.postMessage(message);
  }
  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.controller.abort();
    this.worker.onmessage = null;
    this.worker.onerror = null;
    this.worker.onmessageerror = null;
    this.worker.terminate();
  }
  private fail(message: string): void {
    this.close();
    this.failed(message);
  }
  private async respond(
    message: Extract<FromStreamWorker, { type: 'request' | 'refresh' }>,
  ): Promise<void> {
    try {
      if (message.type === 'refresh') {
        await this.api.refresh(message.rejectedToken);
        if (this.closed) return;
        const accessToken = this.api.session?.accessToken;
        if (!accessToken) throw new ApiError(401, 'refresh_invalid', 'Session ended');
        this.send({ type: 'refreshed', id: message.id, accessToken });
      } else {
        if (
          message.path !== '/metrics/snapshot' &&
          !/^\/metrics\/replay\?since=\d+$/.test(message.path)
        )
          throw new Error('Invalid worker API path');
        // Transfer the bytes. Never JSON.parse or structured-clone a replay array on the UI.
        const buffer = await this.api.request<ArrayBuffer>(
          message.path,
          {},
          this.controller.signal,
          'buffer',
        );
        if (!this.closed)
          this.worker.postMessage(
            {
              type: 'response',
              id: message.id,
              buffer,
              accessToken: this.api.session?.accessToken,
            },
            [buffer],
          );
      }
    } catch (error) {
      if (!this.closed)
        this.send({
          type: 'request-error',
          id: message.id,
          error: {
            status: error instanceof ApiError ? error.status : 0,
            code: error instanceof ApiError ? error.code : 'request_failed',
            message: error instanceof Error ? error.message : String(error),
          },
        });
    }
  }
}
