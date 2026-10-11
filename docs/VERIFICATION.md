# Verification report

Date: 9 October 2026. Local macOS environment, Node 24.15.0 used for Angular 22.2.2, Python 3.13.5 for FastAPI. The default machine Node 22.15.0 was not used for builds because it is below Angular 22's supported minimum.

## Automated checks

| Check | Result |
| --- | --- |
| Angular production build | Passed; strict TypeScript and strict Angular templates |
| Angular ESLint (`npm run lint` / `ng lint`) | Passed, zero errors |
| Prettier (`npm run format:check`) | Passed |
| Vitest (`npm test`) | 20 tests passed |
| Backend pytest | 8 tests passed; one dependency deprecation warning from Starlette/AnyIO |
| Live API/stream smoke script | Passed against localhost:4000 |
| Docker Compose execution | Not run: Docker CLI is not installed in this environment |
| GitHub Actions | Configuration supplied; not run on GitHub because no remote was provided |

The browser was native Google Chrome, controlled through the app's computer-use interface. The in-app browser binding was unreliable, so it was not used as evidence of application correctness.

## Observed live behavior

1. Login as operator loaded 2,000 services and displayed `Live (WebSocket)`.
2. The rendered table accessibility tree contained **18 service rows plus its header** at the top of the 480 px viewport. The remaining service rows were not attached. This is a semantic ARIA table, so count `app-fleet-row` / `[role="row"]`, not only native `tr` elements.
3. Opening `/services/svc-1003` loaded all three charts, each reporting **300 plotted points** and changing live values.
4. WebSocket disabled with `rate=2000`, `chaos=shuffle,dupe,garbage:5`: status transitioned to `Live (SSE)`; charts remained live during several minutes of traffic.
5. Example diagnostics observed during that combined drill: **510,410 duplicates**, **339,938 reordered arrivals**, **27,000 malformed frames**, **165 replays**, **0 snapshot resets**, **0 sequence errors**. Duplicate count includes replay/live overlap, not only server-generated duplicates. Pending events fluctuate normally while shuffled arrivals are buffered.
6. Disabling SSE as well transitioned to `Polling`. Pending events drained to zero, with zero sequence errors.
7. A rule named `Demo latency` (p95 >300 ms for 2 seconds) was created through the actual Reactive Form. It appeared in the list, switched the form to edit mode, and later raised an in-app notification while the app was recovering.
8. Re-enabling both transports climbed back to `Live (WebSocket)` without manually reloading the page.
9. A development rebuild cleared in-memory credentials, and signing in returned to the original service detail route through `returnUrl`.
10. Logging in as viewer with a Rules return URL redirected to Fleet, and the Rules link was absent. Searching for `payments` showed 500 services and updated the URL to `/fleet?q=payments`.
11. Global chaos switches were reset after verification. The example rule may remain in the running mock; restarting the backend clears its in-memory data.

## Live API throughput / integrity smoke

Run from repository root while the backend is running:

```sh
backend/.venv/bin/python backend/smoke.py
```

The script temporarily changes chaos and restores its previous state in `finally`. It measures generator offsets, consumes a real WebSocket, validates both kinds of malformed frames and out-of-order/duplicate deliveries, checks that REST replay is contiguous, and consumes SSE until its heartbeat arrives.

Observed output:

```text
Measured generator throughput: 2002.5 events/s (3-second window)
Chaos: 58 invalid JSON, 63 missing-field frames; duplicates/shuffle and complete replay verified
SSE metric event, offset id and 5-second heartbeat verified
Disabled SSE returns 503; disabled WS handshake covered by pytest
```

This is a short backend throughput sample, **not** proof of frontend frame-rate or memory acceptance at that load.

## Performance acceptance status

| Target | Observed measurement | Status |
| --- | --- | --- |
| Initial JS <350 kB gzip | **104.46 kB gzip** (104,464 bytes), two initial JS files | Meets size target |
| Charts/rules lazy | Separate `detail` and `rules` build chunks | Verified |
| DOM rows <60 | 18 service row elements + header observed at viewport top | Verified for observed view; complete scroll profiling still recommended |
| >50 fps at 4× CPU | Not measured | Unverified |
| No long task >50 ms at 4× CPU | Not measured | Unverified |
| Search/sort latency <100 ms at 4× CPU | Not measured | Unverified |
| Heap growth <20%, minute 5 to minute 30 | Not measured | Unverified |
| No unrelated component rendering | Signal/OnPush architecture implemented; Angular DevTools profile not recorded | Unverified |

Build transfer estimates are not literal gzip measurements. The gzip figure was calculated by compressing each initial JS file independently with Python `gzip.compress(..., compresslevel=9)`, then summing the sizes. CSS and lazy chunks are excluded, matching the initial-JS target.

### Reproduce the outstanding Chrome measurements

1. Serve a production build (prefer Docker/Nginx once Docker is available). Development-mode results are not acceptance measurements.
2. Set global `rate=2000`, reset other chaos, and sign in.
3. In Chrome DevTools Performance settings, select **4× CPU slowdown**. Record at least 30 seconds while typing in search, changing metric sort and scrolling the fleet.
4. Inspect frame intervals and long-task markers. Record FPS, longest main-thread task, and input-to-next-paint latency; export the trace with machine/browser/version information.
5. Count `document.querySelectorAll('app-fleet-row').length` at the top, middle and bottom of the virtual viewport. Include its header in the total if using `[role="row"]`.
6. At minute 5, collect garbage using DevTools and capture a heap snapshot. Repeat at minute 30 under the same rate/view and compare retained heap: `(heap30 - heap5) / heap5 * 100`. Inspect retained subscriptions, chart instances and detached nodes, not only tab process memory.
7. Use Angular DevTools on a development build to inspect component rendering independently. Scroll offscreen, keep name sort active, then watch unrelated service events; confirm only the relevant visible rows and connection clock update.
8. Separately run offline, silent WebSocket, token TTL=5 and HTTP-error drills. Unit tests cover the coordinator/HTTP logic, but their fake timers are not a substitute for actual DevTools network conditions.

No fabricated FPS, latency, or memory numbers are included. Complete these measurements before presenting the assignment as fully performance-qualified.

## Web Worker refactor verification — 9 October 2026

- Frontend suite: **33 tests passed**, including 13 worker/bridge tests.
- Angular production build emitted a separate `stream-worker` bundle (~13.4 kB raw in the first verified worker build). Lint and format checks passed.
- Native Chrome loaded the worker-backed fleet and showed `Processing: Web Worker · display batches every 250 ms`.
- Under 2,000 events/s with `ws=drop:20` and `chaos=shuffle,dupe,garbage:5`, search updated the URL and returned 500 payments services. A captured worker batch reported 33,756 applied events, 10 replays, 1,156 malformed frames, zero resets and zero sequence errors.
- Opening a selected service showed three live 300-point charts during the same chaos run. A later sample reported 66,771 applied events, 21 replays, zero resets and zero sequence errors.
- These are functional smoke observations. No numeric FPS/input-latency improvement or 4× CPU acceptance is claimed without a new performance trace.

- Worker-owned WebSocket → SSE fallback and SSE → polling fallback were verified in native Chrome; all three charts stayed live with 300 points each on both transports. Original chaos settings were restored after the drill.
