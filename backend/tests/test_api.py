import pytest
from fastapi.testclient import TestClient
from backend import main


@pytest.fixture
def client():
    main.state = main.State()
    with TestClient(main.app) as c:
        yield c


def credentials(client, role='operator'):
    result = client.post('/auth/login', json={'email': f'{role}@pulse.dev', 'password': 'pulse123'})
    assert result.status_code == 200
    return result.json()


def headers(client, role='operator'):
    return {'Authorization': f"Bearer {credentials(client, role)['accessToken']}"}


def test_auth_rotation_and_roles(client):
    tokens = credentials(client)
    assert client.get('/services').status_code == 401
    assert client.post('/auth/refresh', json={'refreshToken': tokens['refreshToken']}).status_code == 200
    assert client.post('/auth/refresh', json={'refreshToken': tokens['refreshToken']}).json()['error']['code'] == 'refresh_invalid'
    assert client.post('/rules', headers=headers(client, 'viewer'), json={}).status_code == 403


def test_snapshot_history_replay(client):
    h = headers(client)
    snapshot = client.get('/metrics/snapshot', headers=h).json()
    assert len(snapshot['metrics']) == 2000
    assert len(client.get('/services', headers=h).json()) == 2000
    history = client.get('/metrics/history?serviceId=svc-0001&minutes=15', headers=h).json()
    assert len(history['points']) == 900
    replay = client.get(f"/metrics/replay?since={snapshot['offset']}", headers=h).json()
    assert all(e['offset'] > snapshot['offset'] for e in replay['events'])
    assert client.get('/metrics/history?serviceId=svc-0001&minutes=61', headers=h).status_code == 422
    main.state.replay.popleft()
    assert client.get('/metrics/replay?since=0', headers=h).status_code == 410


def test_rule_crud_unique_and_idempotency(client):
    h = {**headers(client), 'Idempotency-Key': 'same'}
    body = {'name': 'High latency', 'conditions': [{'metric': 'p95', 'operator': '>', 'threshold': 500, 'durationSec': 30, 'custom': 'kept'}], 'extra': 'preserved'}
    first = client.post('/rules', headers=h, json=body)
    assert first.status_code == 201
    assert client.post('/rules', headers=h, json=body).json() == first.json()
    assert len(client.get('/rules', headers=h).json()) == 1
    rule_id = first.json()['id']
    assert client.get('/rules/check-name?name=High%20latency', headers=h).json() == {'available': False}
    assert client.get(f'/rules/check-name?name=High%20latency&excludeId={rule_id}', headers=h).json() == {'available': True}
    assert client.post('/rules', headers={**h, 'Idempotency-Key': 'different'}, json=body).status_code == 409
    assert client.put(f'/rules/{rule_id}', headers=h, json={**body, 'name': 'Changed'}).status_code == 200
    assert client.delete(f'/rules/{rule_id}', headers=h).status_code == 204
    assert client.get(f'/rules/{rule_id}', headers=h).status_code == 404


def test_chaos_and_error_envelope(client):
    h = headers(client)
    assert client.post('/chaos', json={'rate': 5001}).status_code == 400
    assert client.post('/chaos', json={'chaos': 'shuffle,dupe,garbage:5'}).status_code == 200
    client.post('/chaos', json={'http': '429'})
    response = client.get('/services', headers=h)
    assert response.status_code == 429 and response.headers['retry-after'] == '2'
    assert response.json()['error']['details'] == []
    assert client.get('/health').status_code == 200
    client.post('/chaos', json={'http': '500:100'})
    assert client.get('/services', headers=h).status_code == 500
    client.delete('/chaos')
    assert client.get('/services', headers=h).status_code == 200


def test_websocket_and_expiration(client):
    tokens = credentials(client)
    with client.websocket_connect(f"/stream?access_token={tokens['accessToken']}") as ws:
        event = ws.receive_json()
        assert event['type'] == 'metric' and event['seq'] > 0
    user, _ = main.state.tokens[tokens['accessToken']]
    main.state.tokens[tokens['accessToken']] = (user, 0)
    response = client.get('/services', headers={'Authorization': f"Bearer {tokens['accessToken']}"})
    assert response.json()['error']['code'] == 'token_expired'


def test_transport_disable_contract(client):
    tokens = credentials(client)
    h = {'Authorization': f"Bearer {tokens['accessToken']}"}
    client.post('/chaos', json={'ws': 'down', 'sse': 'down'})
    assert client.get('/stream/sse', headers=h).status_code == 503
    from starlette.testclient import WebSocketDenialResponse
    with pytest.raises(WebSocketDenialResponse) as denied:
        with client.websocket_connect(f"/stream?access_token={tokens['accessToken']}"):
            pass
    assert denied.value.status_code == 503
    # A connection-local override takes precedence over the global switch.
    with client.websocket_connect(f"/stream?access_token={tokens['accessToken']}&ws=up") as ws:
        assert ws.receive_json()['type'] == 'metric'


def test_rule_validation_and_preserved_fields(client):
    h = headers(client)
    invalid = {'name': 'Bad rate', 'conditions': [{'metric': 'errRate', 'operator': '>', 'threshold': 101, 'durationSec': 1}]}
    response = client.post('/rules', headers=h, json=invalid)
    assert response.status_code == 422
    assert response.json()['error']['details'][0]['field'] == 'conditions.0'
    valid = {'name': 'Valid rate', 'extra': {'source': 'test'}, 'conditions': [{**invalid['conditions'][0], 'threshold': 50, 'custom': True}]}
    saved = client.post('/rules', headers=h, json=valid).json()
    assert saved['extra'] == valid['extra'] and saved['conditions'][0]['custom'] is True


def test_token_ttl_and_stream_auth_code(client):
    client.post('/chaos', json={'token_ttl': 5})
    assert credentials(client)['expiresIn'] == 5
    from starlette.websockets import WebSocketDisconnect
    with client.websocket_connect('/stream?access_token=unknown') as ws:
        with pytest.raises(WebSocketDisconnect) as disconnected:
            ws.receive_json()
    assert disconnected.value.code == 4401
