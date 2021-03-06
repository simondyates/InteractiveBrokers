from collections import defaultdict
import asyncio
import ssl
import websockets
import ast
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

ssl_context = ssl.SSLContext(ssl.CERT_REQUIRED)
ssl_context.load_verify_locations('localhost.pem')

fields = {'31': 'Last', '88': 'BidSz', '84': 'Bid', '86': 'Ask', '85': 'AskSz'}
quotes = defaultdict(lambda: {k: None for k in fields.values()})
q = SimpleQueue()

async def ib_websocket_async(conids):
    uri = "wss://localhost:5000/v1/api/ws"
    async with websockets.connect(uri, ssl=ssl_context) as websocket:
        _ = await websocket.recv()
        # Send market data subscription request
        for conid in conids:
            await websocket.send('smd+'+conid+'+{"fields":["31","88", "84", "86", "85"]}')
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
        response = q.get()
        response = response.decode('utf-8')
        try:
            response = ast.literal_eval(response)
        except:
            response = {}
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
    #conids = ['265598', '272093']
    listener = threading.Thread(target=ib_websocket, args=[conids], daemon=True)
    listener.start()
    parser = threading.Thread(target=parse_message, daemon=True)
    parser.start()
    now_time = pd.Timestamp.now(tz='US/Eastern')
    #if now_time >= start_time:
    #    time.sleep((stop_time - now_time).seconds)
    time.sleep(5)
    #sys.exit()