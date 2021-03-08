import urllib3
import boto3
import asyncio
import websockets
import ssl
import time
import pickle
import pandas as pd
from IBClient import IBClient
from queue import Queue
import threading

def s3_from_pickle(obj_name, bucket_name='derived-stock-data'):
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket_name, obj_name)
    return pickle.loads(obj.get(RequestPayer='requester')['Body'].read())

async def ib_websocket_async(conids):
    uri = "wss://localhost:5000/v1/api/ws"
    ssl_context = ssl.SSLContext(ssl.CERT_REQUIRED)
    ssl_context.load_verify_locations('localhost.pem')
    async with websockets.connect(uri, ssl=ssl_context) as websocket:
        _ = await websocket.recv()
        # Send market data subscription request
        for conid in conids:
            await websocket.send('smd+'+conid+'+{"fields":["31","87"]}')
        while True:
            msg = await websocket.recv()
            q.put(msg)
        # Handle disconnect here

def ib_websocket(conids):
    loop = asyncio.new_event_loop()
    return loop.run_until_complete(ib_websocket_async(conids))

def save_file():
    while True:
        with open('/home/ubuntu/tmp.txt', 'ab') as f:
            sz = q.qsize()
            print(f'Queue Size: {sz}')
            for i in range(sz):
                f.write(q.get())
                f.write(b'\n')
                q.task_done()
        time.sleep(10)

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
    q = Queue()
    listener = threading.Thread(target=ib_websocket, args=[conids], daemon=True)
    listener.start()
    time.sleep(1)
    saver = threading.Thread(target=save_file, daemon=True)
    saver.start()
    time.sleep(30)
    q.join()
