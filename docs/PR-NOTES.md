# Suggested PR description

Implement Fleet Pulse with Angular 22 and a FastAPI mock so operators can monitor 2,000 services while the network, transport, and authentication fail. The frontend validates and orders a shared event stream, replays gaps, falls from WebSocket to SSE/polling, and resumes higher transports through probes. Virtualized rows, bounded chart history, browser alert evaluation, guarded forms and explicit stale/error states keep the UI usable.

Trade-off: polling performs snapshot requests plus replay rather than treating snapshots as the full event stream. This uses more HTTP bandwidth, but preserves intermediate events for duration-based alerts and avoids silently skipping chart points. Chart rendering averages into 300 buckets; alert evaluation continues on original ordered events.

Validation: see docs/VERIFICATION.md. Performance targets not measured under Chrome's requested conditions must not be presented as satisfied. The mock is in-memory and is intentionally not production infrastructure.
