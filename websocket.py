from collections import defaultdict
import asyncio
import ssl
import websockets
import json
from queue import SimpleQueue
import threading
import pandas as pd
import time
from IBClient import IBClient
import urllib3
import sys

ll_path = '/home/ubuntu/Dropbox/DataSci/PycharmProjects/LeadLag/Algoseek'
if ll_path not in sys.path:
    sys.path.append(ll_path)
from utils import s3_to_pickle, s3_from_pickle

fields = {'31': 'Last', '7296': 'Close', '87': 'Volume'}
quotes = defaultdict(lambda: {k: 0 for k in fields.values()})
q = SimpleQueue()

async def ib_websocket_async(conids):
    uri = "wss://localhost:5000/v1/api/ws"
    ssl_context = ssl.SSLContext(ssl.CERT_REQUIRED)
    ssl_context.load_verify_locations('localhost.pem')
    async with websockets.connect(uri, ssl=ssl_context) as websocket:
        _ = await websocket.recv()
        # Send market data subscription request
        for conid in conids:
            await websocket.send('smd+'+conid+'+{"fields":["31","7296", "87"]}')
        while True:
            msg = await websocket.recv()
            print(msg)
            q.put(msg)
        # Handle disconnect here

def ib_websocket(conids):
    loop = asyncio.new_event_loop()
    return loop.run_until_complete(ib_websocket_async(conids))

def parse_message():
    while True:
        # Check and store queue length
        response = json.loads(q.get())
        for key in [k for k in response.keys() if k in fields.keys()]:
            quotes[response['conid']][fields[key]] = response[key]

if __name__ == '__main__':
    urllib3.disable_warnings()

    # Define universe to listen to
    betas = s3_from_pickle('Betas/30min Betas 20200901 to 20201130.pkl')
    universe = betas.columns.append(betas.index)

    conid_db = s3_from_pickle('Conids/conids.pkl')
    conid_db.name = 'conid'

    # Select the conids we want, and handle missing ones
    conids = pd.DataFrame(index=universe).join(conid_db).squeeze()
    missing = conids[conids.isna()].index.to_list()
    if len(missing):
        print(f'Missing {missing}')
        conids = conids[conids.notna()]
        # Handle the fact that Nans will have recast the dtype to float
        conids = conids.astype('int')
        conids = conids.astype('str')

    ib = IBClient()
    ib.connect()

    start_time = pd.Timestamp.now(tz='US/Eastern')
    stop_time = start_time + pd.Timedelta('20s')
    conids = ['265598', '272093']
    listener = threading.Thread(target=ib_websocket, args=[conids], daemon=True)
    listener.start()
    parser = threading.Thread(target=parse_message, daemon=True)
    parser.start()
    now_time = pd.Timestamp.now(tz='US/Eastern')
    #if now_time >= start_time:
    #    time.sleep((stop_time - now_time).seconds)
    time.sleep(5)
    #sys.exit()