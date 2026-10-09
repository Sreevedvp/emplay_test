import { CONFIG, Session } from './models';
import { backoff, retryable } from './algorithms';
export class ApiError extends Error {
  constructor(
    public status: number,
    public code: string,
    message: string,
    public retryAfter = 0,
  ) {
    super(message);
  }
}
function abortableDelay(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const abort = () => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', abort);
      resolve();
    }, ms);
    signal.addEventListener('abort', abort, { once: true });
    if (signal.aborted) abort();
  });
}
/** Fetch boundary shared by REST and fetch-based SSE. Both tokens are memory-only. */
export class ApiClient {
  session: Session | null = null;
  private refreshPromise: Promise<void> | null = null;
  private lifetime = new AbortController();
  onSession: (session: Session | null) => void = () => undefined;
  constructor(private fetcher: typeof fetch = (...args) => fetch(...args)) {}
  async login(email: string, password: string): Promise<void> {
    const session = await this.raw<Session>(
      '/auth/login',
      { method: 'POST', body: JSON.stringify({ email, password }) },
      undefined,
      false,
    );
    this.lifetime = new AbortController();
    this.session = session;
    this.onSession(session);
  }
  logout(): void {
    this.session = null;
    this.lifetime.abort();
    this.onSession(null);
  }
  async refresh(rejectedToken: string): Promise<void> {
    if (this.session && this.session.accessToken !== rejectedToken) return;
    if (!this.refreshPromise) {
      const refreshToken = this.session?.refreshToken;
      if (!refreshToken) throw new ApiError(401, 'refresh_invalid', 'Please log in');
      this.refreshPromise = this.raw<Session>(
        '/auth/refresh',
        { method: 'POST', body: JSON.stringify({ refreshToken }) },
        this.lifetime.signal,
        false,
      )
        .then((session) => {
          this.session = session;
          this.onSession(session);
        })
        .catch((error) => {
          this.logout();
          throw error;
        })
        .finally(() => {
          this.refreshPromise = null;
        });
    }
    return this.refreshPromise;
  }
  async request<T>(path: string, init: RequestInit = {}, signal?: AbortSignal): Promise<T> {
    const combined = AbortSignal.any([this.lifetime.signal, ...(signal ? [signal] : [])]);
    const method = init.method ?? 'GET';
    const key = new Headers(init.headers).has('Idempotency-Key');
    for (let attempt = 1; ; attempt++) {
      try {
        return await this.raw<T>(path, init, combined, true);
      } catch (error) {
        if (combined.aborted) throw error;
        const status = error instanceof ApiError ? error.status : 0;
        if (attempt >= CONFIG.attempts || !retryable(method, status, key)) throw error;
        await abortableDelay(
          error instanceof ApiError && error.retryAfter ? error.retryAfter : backoff(attempt),
          combined,
        );
      }
    }
  }
  async openStream(path: string, signal: AbortSignal): Promise<Response> {
    const combined = AbortSignal.any([this.lifetime.signal, signal]);
    const token = this.session?.accessToken ?? '';
    let response = await this.fetcher(`/api${path}`, {
      headers: { Authorization: `Bearer ${token}` },
      signal: combined,
    });
    if (response.status === 401) {
      await this.refresh(token);
      response = await this.fetcher(`/api${path}`, {
        headers: { Authorization: `Bearer ${this.session?.accessToken}` },
        signal: combined,
      });
    }
    if (!response.ok)
      throw new ApiError(response.status, 'stream_failed', 'Stream connection failed');
    return response;
  }
  private async raw<T>(
    path: string,
    init: RequestInit,
    signal: AbortSignal | undefined,
    authenticated: boolean,
  ): Promise<T> {
    const token = this.session?.accessToken ?? '';
    const send = async (access: string): Promise<T> => {
      const deadline = AbortSignal.timeout(CONFIG.requestTimeout);
      const headers = new Headers(init.headers);
      headers.set('Content-Type', 'application/json');
      if (authenticated) headers.set('Authorization', `Bearer ${access}`);
      const response = await this.fetcher(`/api${path}`, {
        ...init,
        headers,
        signal: AbortSignal.any([deadline, ...(signal ? [signal] : [])]),
      });
      if (!response.ok) {
        const body = (await response.json().catch(() => null)) as {
          error?: { code?: string; message?: string };
        } | null;
        const retry = response.headers.get('Retry-After');
        const wait = retry
          ? Number.isFinite(Number(retry))
            ? Number(retry) * 1000
            : Math.max(0, Date.parse(retry) - Date.now())
          : 0;
        throw new ApiError(
          response.status,
          body?.error?.code ?? 'request_failed',
          body?.error?.message ?? `Request failed (${response.status})`,
          wait,
        );
      }
      return response.status === 204 ? (undefined as T) : ((await response.json()) as T);
    };
    try {
      return await send(token);
    } catch (error) {
      if (
        authenticated &&
        error instanceof ApiError &&
        error.status === 401 &&
        error.code === 'token_expired'
      ) {
        await this.refresh(token);
        return send(this.session?.accessToken ?? '');
      }
      throw error;
    }
  }
}
