import subprocess
from time import sleep
import os
import re
import urllib3
import asyncio
from aiohttp import ClientSession
import pandas as pd

ib_gateway_host = r"https://127.0.0.1"
ib_gateway_port = r"5000"
ib_gateway_path = ib_gateway_host + ":" + ib_gateway_port
api_version = 'v1/'
client_portal_folder = './clientportal'
header = {'Content-Type': 'application/json'}

def get_gateway_pid():
    # Determine if the IB gateway is running and return pid if so
    out = subprocess.run(['lsof', '-i', 'tcp:5000'], capture_output=True)
    pid = re.match('^[^\d]*(\d+)', out.stdout.decode('utf-8'))
    if pid is None:
        return None
    else:
        return pid.group(1)

def start_sso():
    # consider refactoring with .run the next time you call this
    client = subprocess.Popen(args=['bin/run.sh', 'root/conf.yaml'], cwd='./clientportal', preexec_fn=os.setsid)
    sleep(2)
    _ = input("\nPress Enter once you've logged in successfully.")

def is_authenticated():
    # Checks to see if an existing sso session is authenticated
    content = make_request(endpoint='iserver/auth/status', req_type='POST')
    if content is None:
        return None
    else:
        return content['authenticated'] # bool

def reauthenticate():
    # Reauthenticates an existing session.
    content = make_request(endpoint=r'iserver/reauthenticate', req_type='POST')
    if content is None:
        return None
    else:
        return content['message'] == 'triggered'

def tickle():
    # Pings the server to keep the session from timing out
    content = make_request(endpoint='tickle', req_type='POST')
    if content is None:
        return None
    else:
        return content['ssoExpires'] # time in ms to session expiry

def build_url(endpoint):
    return ib_gateway_path + '/' + api_version + r'portal/' + endpoint

async def make_request_async(endpoint, req_type, session, params=None):
    url = build_url(endpoint=endpoint)

    # SCENARIO 1: No payload.
    if params is None:
        response = await session.request(method=req_type, url=url, headers=header, ssl=False)

    # SCENARIO 2: Payload.
    else:
        response = await session.request(method=req_type, url=url, headers=header, params=params, ssl=False)

        # Check to see if it was successful
    return await response.json()

async def symbol_search_async(symbol_list):
    # Return IB conids matching the symbols in the list
    endpoint = 'iserver/secdef/search'
    req_type = 'POST'
    tasks = []
    async with ClientSession() as session:
        for sym in symbol_list:
            tasks.append(make_request_async(endpoint, req_type, session, {'symbol': sym}))
        return await asyncio.gather(*tasks)

def symbol_search(symbol_list):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(symbol_search_async(tickers))

def filter_sym_search(search_results):
    # Select the primary US conid from a list of lists, ignoring null responses
    US_exchanges = ['NYSE', 'NASDAQ', 'ARCA', 'BATS']
    results = [None if len(j)==0 else j[0] for j in
               [[el['conid'] for el in search_results[i] if el != 'error' and el['description'] in US_exchanges]
                for i in range(len(search_results))]]
    return results

async def market_data_history_async(conids, exchange, period, bar):
    # This *ought* to need semaphor to limit concurrent requests to 5, but so far I haven't seen errors
    endpoint = 'iserver/marketdata/history'
    req_type = 'GET'
    tasks = []
    async with ClientSession() as session:
        for conid in conids:
            tasks.append(make_request_async(endpoint=endpoint, req_type=req_type, session=session,
                                 params={'conid': conid, 'exchange': exchange, 'period': period, 'bar': bar}))
        return await asyncio.gather(*tasks)

def market_data_history(conids, exchange, period, bar):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(market_data_history_async(conids, exchange, period, bar))

urllib3.disable_warnings()
if get_gateway_pid() is None:
    start_sso()
sp5 = pd.read_csv('../LeadLag/Data/SPX_weights_07-24-2020.csv')
tickers = sp5['Symbol']
tickers = [tck.replace('.', ' ') for tck in tickers]
multi = symbol_search(tickers)
conids = filter_sym_search(multi)
bars = market_data_history(conids, 'SMART', '1min', '1min')
