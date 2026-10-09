"""Live contract/drill check. Run against the local mock on :4000; restores chaos."""
import asyncio
import json
import time

import httpx
from websockets.asyncio.client import connect


async def main():
    async with httpx.AsyncClient(base_url='http://127.0.0.1:4000', timeout=15) as client:
        previous = (await client.get('/chaos')).json()
        try:
            await client.delete('/chaos')
            await client.post('/chaos', json={'rate': 2000})
            session = (await client.post('/auth/login', json={'email': 'operator@pulse.dev', 'password': 'pulse123'})).json()
            headers = {'Authorization': f"Bearer {session['accessToken']}"}
            first = (await client.get('/metrics/snapshot', headers=headers)).json()
            start = time.monotonic()
            await asyncio.sleep(3)
            second = (await client.get('/metrics/snapshot', headers=headers)).json()
            rate = (second['offset'] - first['offset']) / (time.monotonic() - start)
            assert rate > 1800, rate
            print(f'Measured generator throughput: {rate:.1f} events/s (3-second window)')
            uri = f"ws://127.0.0.1:4000/stream?access_token={session['accessToken']}"
            async with connect(uri + '&ws=drop:20&chaos=shuffle,dupe,garbage:5') as ws:
                received = []
                invalid_json = missing_fields = 0
                until = time.monotonic() + 2
                while time.monotonic() < until:
                    raw = await asyncio.wait_for(ws.recv(), 2)
                    try:
                        event = json.loads(raw)
                        if event.get('type') == 'metric':
                            if 'offset' not in event:
                                missing_fields += 1
                            else:
                                received.append(event['offset'])
                    except json.JSONDecodeError:
                        invalid_json += 1
                assert invalid_json > 0 and missing_fields > 0
                assert len(set(received)) < len(received), 'Expected duplicate deliveries'
                assert received != sorted(received), 'Expected reordered deliveries'
                replay = (await client.get(f'/metrics/replay?since={min(received)-1}', headers=headers)).json()
                offsets = [event['offset'] for event in replay['events']]
                assert offsets == list(range(min(received), replay['offset'] + 1))
                print(f'Chaos: {invalid_json} invalid JSON, {missing_fields} missing-field frames; duplicates/shuffle and complete replay verified')
            async with client.stream('GET', '/stream/sse', headers=headers) as response:
                assert response.status_code == 200
                lines = response.aiter_lines()
                saw_metric = saw_id = saw_ping = False
                async for line in lines:
                    saw_metric |= line == 'event: metric'
                    saw_id |= line.startswith('id: ')
                    saw_ping |= line == 'event: ping'
                    if saw_metric and saw_id and saw_ping:
                        break
                print('SSE metric event, offset id and 5-second heartbeat verified')
            await client.post('/chaos', json={'ws': 'down', 'sse': 'down'})
            assert (await client.get('/stream/sse', headers=headers)).status_code == 503
            print('Disabled SSE returns 503; disabled WS handshake covered by pytest')
        finally:
            await client.post('/chaos', json=previous)


if __name__ == '__main__':
    asyncio.run(main())
