# How Fleet Pulse works

This guide explains the implementation so you can walk through it and make changes during the assignment review. Read the README first to start the frontend and backend.

## 1. The whole app in one example

1. You sign in as `operator@pulse.dev` with `pulse123`.
2. The frontend stores the returned access and refresh tokens in memory.
3. It loads 2,000 service names and the latest metrics snapshot.
4. It connects to the WebSocket and starts receiving metric events.
5. It validates and orders events, recovers missing offsets, then updates the latest metric for each service.
6. The table publishes those changes four times per second and only renders visible rows.
7. Opening a service loads 15 minutes of REST history, then merges in that service's ordered live events.
8. Saved rules evaluate against every ordered event. Sustained matches add an incident and an in-app notification.
9. If the socket fails, the same pipeline continues over SSE or polling. Components do not implement reconnect logic.

## 2. Where to start reading the source

| File | Responsibility |
| --- | --- |
| `frontend/src/main.ts` | Bootstraps Angular and defines lazy routes and guards |
| `frontend/src/app/app.ts` | Toolbar, connection health, notification, diagnostics, router outlet |
| `frontend/src/app/core/models.ts` | Shared types and central timing/buffer settings |
| `frontend/src/app/core/api.ts` | HTTP requests, timeout, retry, shared refresh, session cancellation |
| `frontend/src/app/core/auth.service.ts` | Angular user signal, login redirect, auth/role guards |
| `frontend/src/app/core/transports.ts` | WebSocket, SSE parser, polling adapters |
| `frontend/src/app/core/coordinator.ts` | Fallback, reconnect, heartbeat, offline, upgrade state machine |
| `frontend/src/app/core/integrity.ts` | Validation, duplicate rejection, ordered offsets, sequence checks |
| `frontend/src/app/core/fleet.store.ts` | Connects pipeline to UI signals, replay, rules and incidents |
| `frontend/src/app/core/algorithms.ts` | Pure retry/backoff, chart reduction, rule evaluation logic |
| `frontend/src/app/core/validators.ts` | Custom synchronous and async form validators |
| `frontend/src/app/features/` | Each screen is one lazy standalone component module |
| `backend/main.py` | FastAPI routes, mock data, auth, streams, and chaos switches |

## 3. Authentication and roles

The login screen uses a Reactive Form. The email must be valid; password must contain at least eight characters. The submit button disables while the form is invalid or a request is pending. Server errors appear in the form.

`Auth` wraps `ApiClient` and exposes the user as an Angular signal. `ApiClient` keeps both tokens in ordinary class memory. Nothing puts tokens in localStorage, sessionStorage, cookies, or a URL except the contract-required WebSocket connection query parameter.

Every normal API request gets an `Authorization: Bearer ...` header. Login/refresh themselves cannot carry a usable access token and are exempt. Fetch SSE also uses a header. Browser WebSocket cannot set arbitrary headers, so it uses `access_token` in the query string.

### Why ten expired requests only refresh once

Suppose ten requests leave with token A, and all receive `401 token_expired`:

- The first handler creates `refreshPromise` and sends the refresh request.
- The other handlers wait on that same promise.
- The response rotates both credentials and stores token B.
- Every waiting request sends its original request again with B.
- A late response rejected with A checks the current token. If B is already stored, it skips refresh and retries with B.

A failed refresh calls logout. A session-level AbortController cancels in-flight authenticated operations. The store closes its transport and clears sensitive session state. The router goes to `/login?returnUrl=...`. After a successful login, it returns to that internal route. The rule-form guard allows forced logout even when the form is dirty.

There are two independent role checks: Angular's `/rules` route guard prevents viewer navigation, and FastAPI rejects mutation requests from viewers with 403. Hiding the Rules link is only a convenience, not the authorization boundary.

## 4. How the backend generates metrics

The backend has a single `State` object and one background generator task. Every service starts with a metric, so the first snapshot always contains 2,000 records. Each generated event:

- increments the shared global `offset` by one;
- increments that service's `seq` by one;
- sets an epoch-millisecond timestamp;
- generates a smooth wave plus noise for plausible metric values;
- calculates status from errors and latency;
- updates the latest record and appends to the replay deque.

The generator runs roughly every 20 ms. At rate 2,000 it produces about 40 events per tick, reducing timer overhead. It rotates through service IDs fairly. Individual stream clients subscribe through bounded queues. A client that cannot keep up is disconnected so it must recover; the backend does not silently drop its queue and pretend everything is healthy.

Replay retains about five minutes. The history endpoint returns one point per second. For times before the backend started, it generates plausible synthetic history; for retained observed seconds, it uses actual metrics. This is intentionally a mock history, not a production time-series database.

## 5. Transport adapters and coordinator

`TransportFactory(kind, sink)` creates an adapter. Every adapter reports:

- `opened`: connected;
- `message`: a raw event string;
- `snapshot`: polling's result;
- `failed`: disconnected or timed out;
- `close`: release resources.

The coordinator uses that interface. It does not know Angular components, rule forms, or chart objects. This lets the tests inject fake adapters and advance fake time without a real network.

### Fallback sequence

```text
WebSocket --3 failures or 5 s connect timeout--> SSE
SSE       --3 failures------------------------> polling
polling   --successful 30 s probe-------------> SSE
SSE       --successful 30 s probe-------------> WebSocket
```

The lower transport stays alive during an upgrade attempt. A successful handshake without an event is not enough to replace it. Reconnection uses exponentially increasing, randomized delays capped at 30 seconds.

The backend sends a ping every five seconds. The coordinator detects about 10.5 seconds without a ping, even if a WebSocket says it is open. Regular metric traffic is not used as proof that the ping contract is healthy. When the browser reports offline, sockets/probes/reconnects stop. The online event immediately restarts at WebSocket.

Fetch-based SSE is used instead of EventSource to support the authorization header and controlled token refresh. The parser supports streaming UTF-8, multiple frames in a read, split frames across reads, SSE comments, and CRLF normalization. Closing the adapter aborts fetch and cancels its reader.

## 6. Correct event ordering and replay

Consider this arrival order:

```text
Received offsets: 101, 103, 103, 102, 104
Applied offsets:  101, 102, 103, 104
```

103 waits in a Map. The repeated 103 increments the duplicate counter. Once 102 arrives, the buffer drains both 102 and 103 in the right order. No chart or rule sees the duplicate or out-of-order arrival.

If 102 does not arrive within one second, the store calls `/metrics/replay?since=101`. That response includes every generated event after 101, including events chaos dropped or replaced with garbage. Replayed duplicates are discarded by the same logic. The per-service sequence check adds another invariant: a service's next accepted sequence must follow the previous sequence.

Only one recovery request runs at a time. Processing yields between groups of 500 events so a recovery burst does not run one enormous synchronous loop. A five-minute replay response still has a potentially expensive JSON decode; that is documented rather than hidden.

If replay returns 410, the old events no longer exist. The store replaces its baseline with a full snapshot, clears alert duration state, reloads open chart history, and tells the user a reset happened. It does not claim the missing alert history was reconstructed. A bounded reorder buffer also forces this explicit reset if an outage lasts long enough to overflow it.

Polling snapshots trigger replay instead of skipping directly to the snapshot offset. That is how polling preserves intermediate events for chart/rule evaluation.

## 7. Why the table stays relatively cheap

There are two separate ideas:

1. **Storage:** keep all 2,000 latest service values.
2. **Rendering:** create components only for visible rows plus a small buffer.

The CDK virtual viewport uses 40 px rows in a 480 px view. Scrolling replaces which rows are attached. It does not create 2,000 DOM rows. Each row reads its own signal. Invisible rows have stored data but no rendered subscriber.

Events update an ordinary Map immediately. Every 250 ms, pending row updates are published to signals. This coalesces repeated events for one row. Rules still see every ordered event before coalescing; performance optimization does not change alert semantics.

Search, status, sort and direction are URL parameters. For example:

```text
/fleet?q=payments&status=degraded&sort=p95&dir=desc
```

A full refresh requires login because tokens are memory-only, then the same URL state is restored. Name sorting does not depend on changing metrics. Metric sorts/status filters explicitly recompute at the render batch boundary.

A status change flashes the status cell. Old metric timestamps mark rows stale, including a rarely updated service even while other services continue streaming.

## 8. Detail route and charts

`/services/:id` starts a cancellable history request and a subscription filtered to the selected service. Route changes clean up both. Each chart dataset is tagged with its service ID; a mismatched ID returns an empty dataset immediately, before a new request finishes.

The live subscription starts before history finishes so events arriving during the fetch are retained. Data is keyed by second. Live values win when history later contains the same second. Old points are evicted after 15 minutes.

For display, `reducePoints` divides that 15-minute window into 300 temporal buckets, averages each bucket's metrics, and orders the results. This means at most 300 points per series. The chart updates once per second and has animation disabled. Three separate Chart.js instances show latency, errors and throughput.

This reduction favors a smooth overview. Very narrow spikes can be smoothed away; a production incident dashboard might choose min/max buckets or LTTB instead. Alert rules never use averaged chart values.

Chart initialization/update is guarded within each chart component. A chart failure displays an error and Retry for that panel. The global stream and fleet table remain independent. REST history failure likewise shows its own Retry.

## 9. Rule form and optimistic saves

The rule form is a Reactive Form with a FormArray of one or more condition FormGroups. Add/remove operates on that array. Metric selection changes the unit and allowable threshold range. Error percentages must be 0–100; latency and throughput must be nonnegative. Duration must be 0–3600 seconds.

The custom validators are:

- `meaningfulName`: trimmed name must contain 3–80 characters;
- `metricThreshold`: a cross-field validator reading both metric and threshold;
- `uniqueName`: asynchronous, waits 350 ms and calls `/rules/check-name`, excluding the current rule ID while editing.

Changing the name cancels the previous async validation subscription and aborts the obsolete HTTP request. If the server cannot validate uniqueness, the form remains invalid and says why; it does not assume a failed check means the name is free. Server validation still handles a race where another client takes the name after validation.

Saving inserts or replaces a visible list entry immediately. A POST includes a fresh idempotency key which stays the same across transport retries. If saving fails, the previous list is restored and the form stays dirty for correction. If saving succeeds, the server's rule replaces the temporary row and the form becomes pristine. Only confirmed rules are used for alert evaluation during optimistic saves.

Trying to leave a dirty rule form asks before discarding it. Settings deliberately uses a Template-driven Form (`ngForm`, `ngModel`) to demonstrate both Angular form styles. Only harmless display preferences use localStorage.

## 10. Browser alert evaluation

Evaluation state is keyed by rule + service. For each condition it remembers when matching started. All conditions must match continuously for their own durations. Example:

```text
Rule: latency > 400 ms for 2 seconds
Time 0: 500 ms -> remember start
Time 1: 520 ms -> still waiting
Time 2: 510 ms -> one incident
Time 3: 530 ms -> already fired, no duplicate
Time 4: 200 ms -> reset episode
```

Different services are evaluated independently. A gap over 10 seconds resets the timer because the client cannot prove what happened during unobserved time. A snapshot recovery resets all timers for the same reason. An incident's identity includes rule ID, service ID, and firing sequence; the evaluator latches a fired episode until a condition becomes false.

Notifications appear in the toolbar region with an accessible live announcement. The fleet screen shows the latest 200 incidents. No OS notification permission is needed.

## 11. HTTP errors and retries

The retry decision is a small pure function. Only GET, or POST with an idempotency key, can retry. Allowed failure statuses are network error (0), 429, 502, 503 and 504. A 500 is intentionally not retried, matching R3's explicit list. Authentication refresh/replay is separate from transient transport retries.

The request deadline is per attempt. Thus a retried GET can take longer than 10 seconds overall. Abort signals stop requests and backoff waits when a page is destroyed or the session ends. `Retry-After: 2` waits two seconds rather than ignoring the server's rate limit.

Do not extend the retry list to all errors casually. Retrying validation failures is useless; retrying mutations without deduplication can create duplicate rules.

## 12. Failure drills you can demonstrate

Reset with `DELETE /chaos` between drills. Commands are in README.

| Drill | What to watch |
| --- | --- |
| `ws=down` | Reconnecting, then Live (SSE); live values continue |
| WS + SSE down | Polling, then successive upgrade probes after reset |
| `ws=silent` | No ping, watchdog disconnects after about 11 s |
| `chaos=shuffle,dupe` | Reordered and duplicate counters rise; applied data remains ordered |
| `ws=drop:20` | Pending events then replay count rises; no silent cursor jump |
| `token_ttl=5` | Log in again; parallel rejected reads share one refresh |
| `http=500:50` | Clear request errors, no inappropriate 500 retry |
| Browser offline | Offline status; old rows become stale; reconnect on online |
| `chaos=garbage:5` | Malformed counter rises, replay fills missing offsets, no crash |

## 13. Useful changes to practice before the review

- Change `CONFIG.staleMs` and explain how it affects health and rule continuity.
- Add another metric: update types, schema validation, backend generator, forms, sorting and chart mapping.
- Change mean buckets to min/max sampling without changing rule evaluation.
- Add a new transport by implementing the factory/sink interface and extending the policy.
- Add a condition operator by extending the type, backend validator, evaluator switch and form options.
- Change the replay retention and explain why clients still need the 410 path.

Run tests after each change. The important invariants are monotonic applied offsets, exactly one concurrent refresh, bounded retained data, and clear communication whenever complete recovery is impossible.
