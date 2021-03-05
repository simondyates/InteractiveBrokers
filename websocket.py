from collections import defaultdict
import asyncio
import ssl
import websockets
import ast

ssl_context = ssl.SSLContext(ssl.CERT_REQUIRED)
ssl_context.load_verify_locations('localhost.pem')

fields = {'31': 'Last', '88': 'BidSz', '84': 'Bid', '86': 'Ask', '85': 'AskSz'}
quotes = defaultdict(lambda: {k: None for k in fields.values()})

async def receive_messages(websocket):
    for i in range(10):
        response = await websocket.recv()
        response = response.decode('utf-8')
        try:
            response = ast.literal_eval(response)
        except:
            response = {}
        for key in [k for k in response.keys() if k in fields.keys()]:
            quotes[response['conid']][fields[key]] = response[key]

async def ib_websocket_connection(conids):
    uri = "wss://localhost:5000/v1/api/ws"

    async with websockets.connect(uri, ssl=ssl_context) as websocket:
        _ = await websocket.recv()
        
        # Send market data subscription request
        for conid in conids:
            await websocket.send('smd+'+conid+'+{"fields":["31","88", "84", "86", "85"]}')

        await receive_messages(websocket)

conids = ['265598', '272093']
asyncio.get_event_loop().run_until_complete(ib_websocket_connection(conids))
#asyncio.get_event_loop().run_forever()