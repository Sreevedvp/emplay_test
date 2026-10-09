"""Fleet Pulse's in-memory mock. Run one worker: all transports share this state."""
import asyncio
from collections import deque
from contextlib import asynccontextmanager, suppress
import json
import math
import random
import secrets
import time
from typing import Any

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response, StreamingResponse
from starlette.exceptions import HTTPException


def now():
    return int(time.time() * 1000)


class ApiError(Exception):
    def __init__(self, status, code, message, details=None):
        self.status, self.code, self.message = status, code, message
        self.details = details or []


def error_body(code, message, details=None):
    return {"error": {"code": code, "message": message, "details": details or []}}


DEFAULTS = {"rate": 500, "ws": "up", "sse": "up", "chaos": "", "token_ttl": 60, "http": "", "kill": 0}


class State:
    def __init__(self):
        self.chaos = dict(DEFAULTS)
        self.offset = 0
        self.services = [{"id": f"svc-{i:04d}", "name": f"{['payments', 'search', 'orders', 'identity'][i % 4]}-api-{i}", "team": ['payments', 'search', 'orders', 'identity'][i % 4], "status": "healthy"} for i in range(1, 2001)]
        self.latest = {}
        self.replay = deque()
        self.listeners = set()
        self.tokens = {}
        self.refreshes = {}
        self.rules = {}
        self.idempotency = {}
        for service in self.services:
            self.generate(service['id'])

    def generate(self, service_id):
        old = self.latest.get(service_id)
        self.offset += 1
        t = now()
        wave = math.sin(t / 30000 + int(service_id[4:]))
        p95 = round(180 + 140 * wave + random.random() * 80, 2)
        err = round(max(0, 1.2 * wave + random.random() * 0.6), 2)
        event = {"type": "metric", "offset": self.offset, "serviceId": service_id, "seq": old['seq'] + 1 if old else 1, "ts": t, "p95": p95, "errRate": err, "rps": round(800 + 500 * wave + random.random() * 200), "status": "down" if err > 1.75 else "degraded" if p95 > 330 else "healthy"}
        self.latest[service_id] = event
        self.replay.append(event)
        return event

    def issue(self, user):
        # Prune expired credentials so expiry drills do not grow memory indefinitely.
        t = now()
        self.tokens = {k: v for k, v in self.tokens.items() if v[1] > t - 60000}
        access, refresh = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
        ttl = self.chaos['token_ttl']
        self.tokens[access] = (user, t + ttl * 1000)
        self.refreshes[refresh] = (user, t + 86400000)
        self.refreshes = {k: v for k, v in self.refreshes.items() if v[1] > t}
        return {"accessToken": access, "refreshToken": refresh, "expiresIn": ttl, "user": user}

    def auth(self, token):
        record = self.tokens.get(token)
        if not record:
            raise ApiError(401, 'token_invalid', 'Invalid access token')
        if now() >= record[1]:
            raise ApiError(401, 'token_expired', 'Access token expired')
        return record[0]


state = State()


async def produce():
    credit = 0.0
    previous = time.monotonic()
    cursor = 0
    while True:
        await asyncio.sleep(.02)
        tick = time.monotonic()
        rate = max([state.chaos['rate']] + [item[1] for item in state.listeners])
        credit += min(tick - previous, .2) * rate
        previous = tick
        batch = []
        while credit >= 1:
            cursor = cursor % 2000 + 1
            batch.append(state.generate(f'svc-{cursor:04d}'))
            credit -= 1
        cutoff = now() - 301000
        while state.replay and state.replay[0]['ts'] < cutoff:
            state.replay.popleft()
        for queue, _ in tuple(state.listeners):
            try:
                queue.put_nowait(batch)
            except asyncio.QueueFull:
                # Close slow consumers; they must replay rather than silently lose data.
                while not queue.empty():
                    queue.get_nowait()
                queue.put_nowait(None)


@asynccontextmanager
async def lifespan(_app):
    task = asyncio.create_task(produce())
    yield
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=['http://localhost:4200', 'http://127.0.0.1:4200', 'http://localhost:8080'], allow_methods=['*'], allow_headers=['*'], expose_headers=['Retry-After'])


@app.exception_handler(ApiError)
async def api_error(_request, exc):
    return JSONResponse(error_body(exc.code, exc.message, exc.details), status_code=exc.status)


@app.exception_handler(RequestValidationError)
async def validation_error(_request, exc):
    return JSONResponse(error_body('validation', 'Invalid request', [{"field": '.'.join(map(str, e['loc'])), "message": e['msg']} for e in exc.errors()]), status_code=422)


@app.exception_handler(HTTPException)
async def http_error(_request, exc):
    return JSONResponse(error_body('not_found' if exc.status_code == 404 else 'bad_request', str(exc.detail)), status_code=exc.status_code)


@app.middleware('http')
async def authenticate_and_chaos(request: Request, call_next):
    path = request.url.path
    exempt = path.startswith('/auth/') or path in ['/health', '/chaos'] or request.method == 'OPTIONS'
    if not exempt:
        try:
            token = request.headers.get('authorization', '').removeprefix('Bearer ')
            if path in ['/stream/sse', '/assistant/stream']:
                token = request.query_params.get('access_token', token)
            request.state.user = state.auth(token)
            if path not in ['/stream/sse', '/assistant/stream']:
                chaos = state.chaos['http']
                if chaos.startswith('slow:'):
                    await asyncio.sleep(float(chaos.split(':')[1]) / 1000)
                if chaos == '429':
                    return JSONResponse(error_body('rate_limited', 'Try again later'), 429, headers={'Retry-After': '2'})
                if chaos.startswith('500:') and random.random() * 100 < float(chaos.split(':')[1]):
                    raise ApiError(500, 'server_error', 'Injected server error')
        except ApiError as exc:
            return await api_error(request, exc)
    return await call_next(request)


@app.get('/health')
async def health():
    return {'status': 'ok'}


@app.post('/auth/login')
async def login(body: dict):
    email = body.get('email')
    if email not in ['operator@pulse.dev', 'viewer@pulse.dev'] or body.get('password') != 'pulse123':
        raise ApiError(401, 'invalid_credentials', 'Invalid email or password')
    role = email.split('@')[0]
    return state.issue({'id': 'u-1' if role == 'operator' else 'u-2', 'email': email, 'role': role})


@app.post('/auth/refresh')
async def refresh(body: dict):
    record = state.refreshes.pop(body.get('refreshToken'), None)
    if not record or record[1] <= now():
        raise ApiError(401, 'refresh_invalid', 'Refresh token invalid or already used')
    return state.issue(record[0])


@app.get('/services')
async def services():
    return [{**s, 'status': state.latest[s['id']]['status']} for s in state.services]


@app.get('/metrics/snapshot')
async def snapshot():
    return {'offset': state.offset, 'ts': now(), 'metrics': list(state.latest.values())}


@app.get('/metrics/replay')
async def replay(since: int):
    if since < 0 or since > state.offset:
        raise ApiError(400, 'bad_request', 'Offset outside stream range')
    if state.replay and since < state.replay[0]['offset'] - 1:
        raise ApiError(410, 'replay_too_old', 'Replay window expired')
    # Most recoveries request a short tail; avoid scanning all five minutes.
    events = []
    for event in reversed(state.replay):
        if event['offset'] <= since:
            break
        events.append(event)
    events.reverse()
    return {'events': events, 'offset': state.offset}


@app.get('/metrics/history')
async def history(serviceId: str, minutes: int = 15):
    if serviceId not in state.latest:
        raise ApiError(404, 'not_found', 'Service not found')
    if minutes < 1 or minutes > 60:
        raise ApiError(422, 'validation', 'minutes must be 1–60')
    end = now() // 1000 * 1000
    # Deterministic synthetic pre-start history, overlaid with actual generated seconds.
    actual = {e['ts'] // 1000 * 1000: e for e in state.replay if e['serviceId'] == serviceId}
    points = []
    for ts in range(end - minutes * 60000, end, 1000):
        wave = math.sin(ts / 30000 + int(serviceId[4:]))
        e = actual.get(ts, {'p95': 220 + 140 * wave, 'errRate': max(0, 1.2 * wave + .3), 'rps': 900 + 500 * wave})
        points.append({'ts': ts, **{k: e[k] for k in ['p95', 'errRate', 'rps']}})
    return {'serviceId': serviceId, 'points': points}


def operator(request):
    if request.state.user['role'] != 'operator':
        raise ApiError(403, 'forbidden', 'Operator role required')


def validate_rule(body, exclude=None):
    details = []
    name = body.get('name', '')
    if not isinstance(name, str) or not name.strip() or len(name) > 80:
        details.append({'field': 'name', 'message': 'Name must contain 1–80 characters'})
    conditions = body.get('conditions')
    if not isinstance(conditions, list) or not 1 <= len(conditions) <= 20:
        details.append({'field': 'conditions', 'message': 'Supply 1–20 conditions'})
    else:
        for i, c in enumerate(conditions):
            valid = isinstance(c, dict) and c.get('metric') in ['p95', 'errRate', 'rps'] and c.get('operator') in ['>', '>=', '<', '<=']
            if valid:
                threshold, duration = c.get('threshold'), c.get('durationSec')
                valid = isinstance(threshold, (float, int)) and math.isfinite(threshold) and threshold >= 0 and isinstance(duration, (float, int)) and math.isfinite(duration) and 0 <= duration <= 3600
                if valid and c['metric'] == 'errRate':
                    valid = threshold <= 100
            if not valid:
                details.append({'field': f'conditions.{i}', 'message': 'Invalid metric, operator, threshold or duration'})
    if details:
        raise ApiError(422, 'validation', 'Invalid rule', details)
    if any(r['name'].casefold() == name.strip().casefold() and r['id'] != exclude for r in state.rules.values()):
        raise ApiError(409, 'name_taken', 'Rule name already exists')


@app.get('/rules')
async def rules():
    return list(state.rules.values())


@app.get('/rules/check-name')
async def check_name(name: str, excludeId: str = ''):
    return {'available': not any(r['name'].casefold() == name.strip().casefold() and r['id'] != excludeId for r in state.rules.values())}


@app.get('/rules/{rule_id}')
async def get_rule(rule_id: str):
    if rule_id not in state.rules:
        raise ApiError(404, 'not_found', 'Rule not found')
    return state.rules[rule_id]


@app.post('/rules', status_code=201)
async def create_rule(request: Request, body: dict):
    operator(request)
    key = request.headers.get('idempotency-key')
    scoped_key = (request.state.user['id'], key)
    if key and scoped_key in state.idempotency:
        return state.idempotency[scoped_key]
    validate_rule(body)
    rule = {**body, 'name': body['name'].strip(), 'id': f'r-{secrets.token_hex(6)}', 'createdAt': now(), 'updatedAt': now()}
    state.rules[rule['id']] = rule
    if key:
        state.idempotency[scoped_key] = rule
    return rule


@app.put('/rules/{rule_id}')
async def update_rule(rule_id: str, request: Request, body: dict):
    operator(request)
    old = await get_rule(rule_id)
    validate_rule(body, rule_id)
    rule = {**body, 'name': body['name'].strip(), 'id': rule_id, 'createdAt': old['createdAt'], 'updatedAt': now()}
    state.rules[rule_id] = rule
    return rule


@app.delete('/rules/{rule_id}', status_code=204)
async def delete_rule(rule_id: str, request: Request):
    operator(request)
    await get_rule(rule_id)
    del state.rules[rule_id]
    return Response(status_code=204)


def validated_switches(body):
    result = {}
    for key, value in body.items():
        if key not in DEFAULTS:
            raise ApiError(400, 'bad_request', f'Unknown switch: {key}')
        if key in ['rate', 'token_ttl', 'kill']:
            try:
                value = int(value)
            except (ValueError, TypeError):
                raise ApiError(400, 'bad_request', f'{key} must be an integer')
            lo, hi = {'rate': (1, 5000), 'token_ttl': (1, 86400), 'kill': (0, 86400)}[key]
            if not lo <= value <= hi:
                raise ApiError(400, 'bad_request', f'{key} must be {lo}–{hi}')
        else:
            valid = isinstance(value, str)
            if key == 'ws':
                valid = valid and (value in ['up', 'down', 'silent'] or percentage(value, 'drop:'))
            if key == 'sse':
                valid = value in ['up', 'down']
            if key == 'chaos':
                valid = valid and all(x in ['', 'shuffle', 'dupe'] or percentage(x, 'garbage:') for x in value.split(','))
            if key == 'http':
                valid = valid and (value in ['', '429'] or percentage(value, '500:') or (value.startswith('slow:') and value[5:].isdigit() and int(value[5:]) <= 60000))
            if not valid:
                raise ApiError(400, 'bad_request', f'Invalid {key} switch')
        result[key] = value
    return result


def percentage(value, prefix):
    try:
        return value.startswith(prefix) and 0 <= float(value[len(prefix):]) <= 100
    except ValueError:
        return False


@app.get('/chaos')
async def chaos_get():
    return state.chaos


@app.post('/chaos')
async def chaos_post(body: dict):
    state.chaos.update(validated_switches(body))
    return state.chaos


@app.delete('/chaos')
async def chaos_reset():
    state.chaos = dict(DEFAULTS)
    return state.chaos


def connection_overrides(params):
    return validated_switches({k: v for k, v in params.items() if k in DEFAULTS})


async def feed(kind, overrides):
    queue = asyncio.Queue(maxsize=100)
    registration = (queue, overrides.get('rate', 1))
    state.listeners.add(registration)
    started = last_ping = time.monotonic()
    pending = []
    try:
        while True:
            switches = {**state.chaos, **overrides}
            t = time.monotonic()
            if switches[kind] == 'down' or (switches['kill'] and t - started >= switches['kill']):
                break
            try:
                batch = await asyncio.wait_for(queue.get(), timeout=.1)
                if batch is None:
                    break
            except asyncio.TimeoutError:
                batch = []
            if kind == 'ws' and switches['ws'] == 'silent':
                pending.clear()
                continue
            if t - last_ping >= 5:
                yield json.dumps({'type': 'ping', 'ts': now()}), 'ping', None
                last_ping = t
            flags = switches['chaos'].split(',')
            garbage = next((float(f.split(':')[1]) for f in flags if f.startswith('garbage:')), 0)
            drop = float(switches['ws'].split(':')[1]) if switches['ws'].startswith('drop:') else 0
            for event in batch:
                if random.random() * 100 < drop:
                    continue
                payload = json.dumps(event, separators=(',', ':'))
                if random.random() * 100 < garbage:
                    payload = '{bad JSON' if random.random() < .5 else '{"type":"metric"}'
                due = t + random.random() * .9 if 'shuffle' in flags else t
                pending.append((due, payload, event['offset']))
                if 'dupe' in flags and random.random() < .1:
                    pending.append((t + random.random() * .9, payload, event['offset']))
            ready = [p for p in pending if p[0] <= t]
            pending = [p for p in pending if p[0] > t]
            for _, payload, offset in sorted(ready):
                yield payload, 'metric', offset
    finally:
        state.listeners.discard(registration)


@app.websocket('/stream')
async def websocket(ws: WebSocket):
    try:
        state.auth(ws.query_params.get('access_token', ws.headers.get('authorization', '').removeprefix('Bearer ')))
        overrides = connection_overrides(ws.query_params)
    except ApiError:
        await ws.accept()
        await ws.close(code=4401)
        return
    if {**state.chaos, **overrides}['ws'] == 'down':
        # ASGI denial extension produces the required HTTP 503 handshake response.
        await ws.send_denial_response(JSONResponse(error_body('unavailable', 'WebSocket disabled'), 503))
        return
    await ws.accept()
    try:
        async for payload, _, _ in feed('ws', overrides):
            await ws.send_text(payload)
        await ws.close(code=1012)
    except (WebSocketDisconnect, RuntimeError):
        pass


@app.get('/stream/sse')
async def sse(request: Request):
    overrides = connection_overrides(request.query_params)
    if {**state.chaos, **overrides}['sse'] == 'down':
        raise ApiError(503, 'unavailable', 'SSE disabled')

    async def body():
        yield ': connected\n\n'
        async for payload, kind, offset in feed('sse', overrides):
            yield f'event: {kind}\n' + (f'id: {offset}\n' if offset else '') + f'data: {payload}\n\n'
    return StreamingResponse(body(), media_type='text/event-stream', headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


@app.post('/telemetry', status_code=202)
async def telemetry(body: dict):
    return {'accepted': len(body.get('errors', []))}


@app.get('/assistant/stream')
async def assistant(q: str = ''):
    async def body():
        for word in f'Mock assistant: inspect latency, errors and connection health for your question: {q}'.split():
            yield f'event: token\ndata: {json.dumps({"text": word + " "})}\n\n'
            await asyncio.sleep(.05)
        yield 'event: done\ndata: {}\n\n'
    return StreamingResponse(body(), media_type='text/event-stream')
