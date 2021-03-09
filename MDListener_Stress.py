import json
import pandas as pd
from queue import Queue
import random
import threading
import time


def get_current_conids(n=1500):
    return list(map(str, random.sample(range(100000), n)))


def ib_websocket(conids, rate=250):
    while not stop_listening:
        conid = random.choice(conids)
        t = int(pd.Timestamp.utcnow().value // 1e6)
        msg = json.dumps({"server_id": "q0", "conid": conid, "_updated": t, "6119": "q0",
                          "87": "12.2M", "87_raw": 1.22E7, "31": "119.46", "6509": "RpB", "topic": "smd+265598"})
        q.put(msg)
        time.sleep(1/rate)


def process_message(quotes):
    # {"server_id":"q0","conid":265598,"_updated":1615299902126,"6119":"q0","31":"118.75","6509":"RpB","topic":"smd+265598"}
    # {"server_id":"q0","conid":265598,"_updated":1615300714892,"6119":"q0","87":"12.2M","87_raw":1.22E7,"31":"119.46","6509":"RpB","topic":"smd+265598"}
    max_sz = 0
    while (sz := q.qsize()) > 0 or not stop_listening:
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


if __name__ == '__main__':
    start_t = pd.Timestamp.utcnow().replace(second=0, microsecond=0)
    end_t = start_t + pd.Timedelta('30min')

    # Initialise data structures to hold results, and conid universe
    q = Queue() # For passing raw messages between threads
    conids = get_current_conids()

    quote_idx = pd.date_range(start_t, end_t, freq='1min', tz='utc') # double-inclusive
    quote_midx = pd.MultiIndex.from_product([quote_idx, conids], names=['Timestamp', 'ConId'])
    quote_cols = ['FirstTradePrice', 'FirstTradeTime', 'LastTradePrice', 'LastTradeTime', 'CumVolume']
    quotes = pd.DataFrame(index=quote_midx, columns=quote_cols)
    quotes['FirstTradeTime'] = end_t # Initialise to effectively infinity
    quotes['LastTradeTime'] = start_t # Initialise to effectively zero

    stop_listening = False
    listener = threading.Thread(target=ib_websocket, args=[conids])
    listener.start()
    processor = threading.Thread(target=process_message, args=[quotes])
    processor.start()

    time.sleep(30)
    stop_listening = True
    q.join()