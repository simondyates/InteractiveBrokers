import subprocess
import os
import re
import urllib3
import requests
import pandas as pd

ib_gateway_host = r"https://localhost"
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
    _ = input('Are you done?')

def build_url(endpoint):
    return ib_gateway_path + '/' + api_version + r'portal/' + endpoint

def make_request(endpoint, req_type, params=None):
    url = build_url(endpoint=endpoint)

    # SCENARIO 1: POST without a payload.
    if req_type == 'POST' and params is None:
        response = requests.post(url, headers=header, verify=False)

    # SCENARIO 2: POST with a payload.
    elif req_type == 'POST' and params is not None:
        response = requests.post(url, headers=header, json=params, verify=False)

    # SCENARIO 3: GET without parameters.
    elif req_type == 'GET' and params is None:
        response = requests.get(url, headers=header, verify=False)

    # SCENARIO 4: GET with parameters.
    elif req_type == 'GET' and params is not None:
        response = requests.get(url, headers=header, params=params, verify=False)

    # Check to see if it was successful
    if response.ok:
        return response.json()

    # if it was a bad request print it out.
    elif not response.ok and url != 'https://localhost:5000/v1/portal/iserver/account':
        print('')
        print('-' * 80)
        print("BAD REQUEST - STATUS CODE: {}".format(response.status_code))
        print("RESPONSE URL: {}".format(response.url))
        print("RESPONSE HEADERS: {}".format(response.headers))
        print("RESPONSE TEXT: {}".format(response.text))
        print('-' * 80)
        print('')
        return None

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

def symbol_search(symbol):
    # Return IB conids matching the symbol
    endpoint = 'iserver/secdef/search'
    req_type = 'POST'
    payload = {'symbol': symbol}
    content = make_request(endpoint=endpoint, req_type=req_type, params=payload)
    return content

def symbol_search_iter(sym_list):
    return [symbol_search(sym) for sym in sym_list]

def filter_sym_search(search_results):
    # Select the primary US conid from a list of lists, ignoring null responses
    # Assumes None responses have been removed first
    US_exchanges = ['NYSE', 'NASDAQ', 'ARCA']
    results = [None if len(j)==0 else j[0] for j in
               [[el['conid'] for el in search_results[i] if el['description'] in US_exchanges]
                for i in range(len(search_results))]]
    return results

def market_data_history(conid, exchange, period, bar):
    # Define request components
    endpoint = 'iserver/marketdata/history'
    req_type = 'GET'
    params = {
        'conid': conid,
        'exchange': exchange,
        'period': period,
        'bar': bar
    }
    content = make_request(endpoint=endpoint, req_type=req_type, params=params)
    return content

urllib3.disable_warnings()
if get_gateway_pid() is None:
    start_sso()
sp5 = pd.read_csv('../LeadLag/Data/SPX_weights_07-24-2020.csv')
tickers = sp5['Symbol']
multi = symbol_search_iter(tickers)
errors = [i for i, l in enumerate(multi) if l is None]
print(filter_sym_search(multi))
