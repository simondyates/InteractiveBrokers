import asyncio
import boto3
from IBClient import IBClient
import json
import pandas as pd
from pandas.tseries.offsets import CustomBusinessDay
import pandas_market_calendars as mcal
import pickle
from queue import Queue
import ssl
import threading
import time
import urllib3
import websockets


def s3_from_pickle(obj_name, bucket_name='derived-stock-data'):
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket_name, obj_name)
    return pickle.loads(obj.get(RequestPayer='requester')['Body'].read())


def symbol_to_conid(tickers):
    conid_db = s3_from_pickle('Conids/conids.pkl')
    conid_db.name = 'ConId'
    return pd.DataFrame(index=tickers).join(conid_db).squeeze()


def get_current_conids():
    betas = s3_from_pickle('Betas/30min Betas 20200901 to 20201130.pkl')
    universe = betas.columns.append(betas.index)
    # Select the conids we want, and handle missing ones
    conids = symbol_to_conid(universe)
    missing = conids[conids.isna()].index.to_list()
    if len(missing):
        print(f'Missing {missing}')
        conids = conids[conids.notna()]
        # Handle the fact that Nans will have recast the dtype to float
        conids = conids.astype('int')
    return conids


def get_closing_prices(ib, conids, dt):
    print('Getting closing prices')
    t = int(dt.value // 1e6)
    response = ib.market_data_history(conids, '', '3d', '1d')
    d = {conids[i['symbol']]: [d['c'] for d in i['data'] if d['t'] == t][0]
         for i in response if i is not None and i['symbol'] in conids.index}
    print(f'Received {len(d)} closing prices')
    return d


async def ib_websocket_async(conids):
    uri = "wss://localhost:5000/v1/api/ws"
    ssl_context = ssl.SSLContext(ssl.CERT_REQUIRED)
    ssl_context.load_verify_locations('localhost.pem')
    async with websockets.connect(uri, ssl=ssl_context) as websocket:
        _ = await websocket.recv()
        # Send market data subscription request
        for conid in conids:
            await websocket.send('smd+'+str(conid)+'+{"fields":["31","87"]}')
        min_ = pd.Timestamp.utcnow().minute
        while listen:
            msg = await websocket.recv()
            q.put(msg)
            if (m := pd.Timestamp.utcnow().minute) != min_:
                min_ = m
                await websocket.send('ech+hb')
                print(f'Heartbeat sent at minute {m}')
        # Handle disconnect here

def ib_websocket(conids):
    loop = asyncio.new_event_loop()
    return loop.run_until_complete(ib_websocket_async(conids))


def process_message(quote_dict):
    # {"server_id":"q0","conid":265598,"_updated":1615299902126,"6119":"q0","31":"118.75","6509":"RpB","topic":"smd+265598"}
    # {"server_id":"q0","conid":265598,"_updated":1615300714892,"6119":"q0","87":"12.2M","87_raw":1.22E7,"31":"119.46","6509":"RpB","topic":"smd+265598"}
    start_t = keys[0]
    stop_t = keys[-1]
    max_sz = 0
    while (sz := q.qsize()) > 0 or listen:
        if sz > max_sz:
            max_sz = sz
            print(f'Queue Max: {sz}')
        for i in range(sz):
            response = q.get()
            try:
                response = json.loads(response)
            except:
                response = {}
            if 'conid' in response.keys():
                t = response['_updated']
                if start_t <= t <= stop_t:
                    minute = int(60000 * (t // 60000))
                    conid = response['conid']
                    d = quote_dict[minute][conid]
                    write_lock.acquire()
                    if '31' in response.keys():
                        # We do not assume our messages arrived in order
                        if t < d['FirstTradeTime']:
                            d['FirstTradeTime'] = t
                            d['FirstTradePrice'] = response['31']
                        if t > d['LastTradeTime']:
                            d['LastTradeTime'] = t
                            d['FirstTradePrice'] = response['31']
                    if '87_raw' in response.keys():
                        d['CumVolume'] = response['87_raw']
                    write_lock.release()
            q.task_done()

def save_data(quote_dict):
    start_t = pd.Timestamp(keys[0], unit='ms', tz='UTC') + pd.Timedelta('2min') # df might still be empty at start+1min
    stop_t = pd.Timestamp(keys[-1], unit='ms', tz='UTC') + pd.Timedelta('7min') # last save will be 4:05 approx since index.max is 3:59
    while True:
        if start_t <= pd.Timestamp.utcnow() <= stop_t:
            start = time.time()
            print(f'Saving file, queue size is {q.qsize():,.0f}')
            write_lock.acquire()
            pkl = pickle.dumps(quote_dict)
            write_lock.release()
            with open(f'/home/ubuntu/DayData/{today:%Y%m%d}.pkl', 'wb') as f:
                f.write(pkl)
            duration = time.time() - start
            print(f'Save took {duration:0.0f} seconds')
        elif pd.Timestamp.utcnow() >= stop_t:
            break
        time.sleep(60)


if __name__ == '__main__':
    # Get today's trading hours
    NYSE = mcal.get_calendar('NYSE')
    nyse_bday = CustomBusinessDay(holidays=NYSE.holidays().holidays)
    today = pd.Timestamp.now().date()
    yesterday = today - nyse_bday
    days = NYSE.schedule(yesterday, today)
    start_t = days.iloc[1, 0] # Today's market open (UTC)
    end_t = days.iloc[1, 1]  # Today's market close (UTC)

    # Initialise data structures to hold results, and conid universe
    q = Queue() # For passing raw messages between threads
    conids = get_current_conids()

    # Log on to IB
    urllib3.disable_warnings()
    ib = IBClient()
    ib.connect()

    try:
        quotes = pd.read_pickle(f'/home/ubuntu/DayData/{today:%Y%m%d}.pkl')
        print('Found a day file')
    except:
        keys = range(int(start_t.value // 1e6), int(end_t.value // 1e6), 60000)
        quote_dict = {k: {c: {'FirstTradePrice': 0, 'FirstTradeTime': float('inf'),
                              'LastTradePrice': 0, 'LastTradeTime': 0,
                              'YestClose': 0, 'CumVolume': 0} for c in conids} for k in keys}

        closes = get_closing_prices(ib, conids, days.iloc[0, 0]) # Prev bus day open
        for d in quote_dict.keys():
            for k in closes.keys():
                quote_dict[d][k]['YestClose'] = closes[k]

    listen = True
    listener = threading.Thread(target=ib_websocket, args=[conids])
    listener.start()
    write_lock = threading.Lock()
    processor = threading.Thread(target=process_message, args=[quote_dict])
    processor.start()
    saver = threading.Thread(target=save_data, args=[quote_dict])
    saver.start()
    saver.join()
    print('Shutting down')
    listen = False
