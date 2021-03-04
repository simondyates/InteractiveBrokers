
import asyncio
import pathlib
import ssl
import websockets
import os

ssl_context = ssl.SSLContext(ssl.CERT_REQUIRED)
localhost_pem = pathlib.Path(__file__).with_name("localhost.pem")
ssl_context.load_verify_locations(localhost_pem)

# To allow https connection
if (not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None)):
    ssl._create_default_https_context = ssl._create_unverified_context

async def receive_messages(websocket):
    while True:
        try:
            response = await websocket.recv()
        except websockets.ConnectionClosed:
            print(f"Terminated")
            break

        print(f"< {response}")

async def ib_websocket_connection():

    uri = "wss://localhost:5000/v1/api/ws"

    async with websockets.connect(uri, ssl=ssl_context) as websocket:
        _ = await websocket.recv()
        # Send market data subscription request
        await websocket.send('smd+265598+{"fields":["31","83"]}')
        await receive_messages(websocket)

asyncio.get_event_loop().run_until_complete(ib_websocket_connection())
asyncio.get_event_loop().run_forever()