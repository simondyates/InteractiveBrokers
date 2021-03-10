import json
import pandas as pd
from queue import Queue
import random
import threading
import time


def get_current_conids(n=1500):
    return random.sample(range(100000), n)


def ib_websocket(conids, rate=1000):
    while not stop_listening:
        conid = random.choice(conids)
        t = int(pd.Timestamp.utcnow().value // 1e6)
        msg = json.dumps({"server_id": "q0", "conid": conid, "_updated": t, "6119": "q0",
                          "87": "12.2M", "87_raw": 1.22E7, "31": "119.46", "6509": "RpB", "topic": "smd+265598"})
        q.put(msg)
        time.sleep(1/rate)


def process_message(quote_dict):
    # {"server_id":"q0","conid":265598,"_updated":1615299902126,"6119":"q0","31":"118.75","6509":"RpB","topic":"smd+265598"}
    # {"server_id":"q0","conid":265598,"_updated":1615300714892,"6119":"q0","87":"12.2M","87_raw":1.22E7,"31":"119.46","6509":"RpB","topic":"smd+265598"}
    max_sz = 0
    while (sz := q.qsize()) > 0 or not stop_listening:
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
                minute = int(60000 * (t // 60000))
                conid = response['conid']
                d = quote_dict[minute][conid]
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
                q.task_done()


if __name__ == '__main__':
    start_t = int(pd.Timestamp('2021-03-10 14:30:00', tz='UTC').value // 1e6)
    end_t = int(pd.Timestamp('2021-03-10 21:00:00', tz='UTC').value // 1e6)

    # Initialise data structures to hold results, and conid universe
    q = Queue() # For passing raw messages between threads
    conids = get_current_conids()
    keys = range(start_t, end_t, 60000)
    quote_dict = {k: {c: {'FirstTradePrice':0, 'FirstTradeTime': float('inf'),
                          'LastTradePrice': 0, 'LastTradeTime': 0,
                          'YestClose': 0, 'CumVolume': 0} for c in conids} for k in keys}

    stop_listening = False
    listener = threading.Thread(target=ib_websocket, args=[conids])
    listener.start()
    processor = threading.Thread(target=process_message, args=[quote_dict])
    processor.start()

    time.sleep(10)
    stop_listening = True
    #q.join()