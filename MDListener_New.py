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
    return conids.astype('str')


def get_closing_prices(ib, conids, dt):
    t = int(dt.value // 1e6)
    response = ib.market_data_history(conids, '', '3d', '1d')
    d = {i['symbol']: [d['c'] for d in i['data'] if d['t'] == t][0] for i in response}
    s = pd.Series(d, name='Close')
    s.index.name = 'ConId'
    return s


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


def process_message(quotes):
    # {"server_id":"q0","conid":265598,"_updated":1615299902126,"6119":"q0","31":"118.75","6509":"RpB","topic":"smd+265598"}
    # {"server_id":"q0","conid":265598,"_updated":1615300714892,"6119":"q0","87":"12.2M","87_raw":1.22E7,"31":"119.46","6509":"RpB","topic":"smd+265598"}
    max_sz = 0
    while True:
        sz = q.qsize()
        if sz > max_sz:
            max_sz = sz
            print(f'Queue Max: {sz}')
        for i in range(sz):
            response = json.loads(q.get())
            if 'conid' in response.keys():
                conid = str(response['conid'])
                t = pd.to_datetime(response['_updated'], utc=True, unit='ms')
                minute = t.replace(second=0, microsecond=0)
                if '31' in response.keys():
                    # We do not assume our messages arrived in order
                    if t < quotes.loc[(minute, conid), 'FirstTradeTime']:
                        quotes.loc[(minute, conid), ['FirstTradeTime', 'FirstTradePrice']] = [t, response['31']]
                    if t > quotes.loc[(minute, conid), 'LastTradeTime']:
                        quotes.loc[(minute, conid), ['LastTradeTime', 'LastTradePrice']] = [t, response['31']]
                if '87_raw' in response.keys():
                    quotes.loc[(minute, conid), 'CumVolume'] = response['87_raw']
            q.task_done()

def save_data(quotes):
    while True:
        time.sleep(60)
        start = time.time()
        quotes.to_pickle(f'/home/ubuntu/DayData/{today:%Y%m%d}.pkl')
        duration = time.time() - start
        print(f'Save took {duration:0.0f} seconds')

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
    conids = ['265598', '272093']  # REMOVE ONCE COMPLETE

    # Log on to IB
    urllib3.disable_warnings()
    ib = IBClient()
    ib.connect()

    try:
        quotes = pd.read_pickle(f'/home/ubuntu/DayData/{today:%Y%m%d}.pkl')
        print('Found a day file')
    except:
        quote_idx = pd.date_range(start_t, end_t, freq='1min', tz='utc') # double-inclusive
        quote_midx = pd.MultiIndex.from_product([quote_idx, conids], names=['Timestamp', 'ConId'])
        quote_cols = ['FirstTradePrice', 'FirstTradeTime', 'LastTradePrice', 'LastTradeTime', 'CumVolume']
        quotes = pd.DataFrame(index=quote_midx, columns=quote_cols)
        quotes['FirstTradeTime'] = end_t # Initialise to effectively infinity
        quotes['LastTradeTime'] = start_t # Initialise to effectively zero
        closes = get_closing_prices(ib, conids, days.iloc[0, 0]) # Prev bus day open
        closes.index = symbol_to_conid(closes.index).astype('str')
        quotes = quotes.join(closes)

    listener = threading.Thread(target=ib_websocket, args=[conids], daemon=True)
    listener.start()
    processor = threading.Thread(target=process_message, args=[quotes], daemon=True)
    processor.start()
    saver = threading.Thread(target=save_data, args=[quotes], daemon=True)
    saver.start()
