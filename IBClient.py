import subprocess
import re
from time import sleep
import webbrowser
import os
import signal
import requests
import warnings
import asyncio
from aiohttp import ClientSession
import json

class IBClient(object):
    def __init__(self):
        self._ib_gateway_url = r'https://127.0.0.1:5000'
        self._ib_gateway_path = self._ib_gateway_url + r'/v1/portal/'
        self._client_portal_folder = './clientportal.gw'
        self._header = {
            'accept': 'application/json',
            'Content-Type': 'application/json'}
        self.pid = None
        self.is_authenticated = False
        self.accounts_queried = False

    def get_gateway_pid(self):
        # Determine if the IB gateway is running and return pid if so
        out = subprocess.run(['lsof', '-i', 'tcp:5000'], capture_output=True)
        out_str = out.stdout.decode('utf-8')
        loc = str.find(out_str, 'java')
        if loc == -1:
            self.pid = None
            return self.pid
        match_ = re.match('^[^\d]*(\d+)', out_str[loc:])
        if match_ is not None:
            self.pid = int(match_.group(1))
            return self.pid
        else:
            self.pid = None
            return self.pid

    def stop_sso(self):
        if self.pid is not None:
            os.kill(self.pid, signal.SIGTERM)

    def check_authenticated(self):
        # Checks to see if an existing sso session is authenticated
        try:
            content = self._make_request(endpoint='iserver/auth/status', req_type='POST')
            print('OK')
            return content['authenticated']
        except:
            print('Not OK')
            return False

    def _make_request(self, endpoint, req_type, params=None):
        url = self._ib_gateway_path + endpoint

        if req_type == 'POST' and params is not None:
            response = requests.post(url, headers=self._header, data=json.dumps(params), verify=False)
        elif req_type == 'POST' and params is None:
            response = requests.post(url, headers=self._header, verify=False)
        elif req_type == 'GET' and params is not None:
            response = requests.get(url, headers=self._header, params=params, verify=False)
        elif req_type == 'GET' and params is None:
            response = requests.get(url, headers=self._header, verify=False)

        if response.status_code == 200:
            return response.json()
        else:
            warnings.warn(f'Received error {response.status_code}')
            return response.json()

    def connect(self):
        # Determines whether there's already a valid connection and connects if not
        self.pid = self.get_gateway_pid()
        self.is_authenticated = self.check_authenticated()
        if (self.pid is None) or (self.is_authenticated == False):
            print('No gateway')
            subprocess.Popen(args=['bin/run.sh', 'root/conf.yaml'], cwd='./clientportal.gw', preexec_fn=os.setsid)
            webbrowser.open(self._ib_gateway_url, new=2)
            _ = input("\nPress Enter once you've logged in successfully.")
            self.pid = self.get_gateway_pid()
            self.is_authenticated = self.check_authenticated()
        return self.is_authenticated

    def reauthenticate(self):
        # I have no idea why IB provide this functionality but I implemented it anyway
        content = self._make_request(endpoint=r'iserver/reauthenticate', req_type='POST')
        if content is None:
            return None
        else:
            return content

    def tickle(self):
        # Pings the server to keep the session from timing out
        content = self._make_request(endpoint='tickle', req_type='POST')
        if content is None:
            return None
        else:
            return content['ssoExpires'] # time in ms to session expiry

    async def _make_request_async(self, endpoint, req_type, session, params=None):
        url = self._ib_gateway_path + endpoint

        if params is None:
            response = await session.request(method=req_type, url=url, headers=self._header, ssl=False)
        else:
            response = await session.request(method=req_type, url=url, headers=self._header, params=params, ssl=False)

        if response.status == 200:
            return await response.json()
        else:
            warnings.warn(f'Received error {response.status}')
            if response.status in [400, 401]:
                # Try to fix the problem
                bool = self.reauthenticate()
                if bool:
                    print('Seemingly successful reconnect, retrying')
                    response = await session.request(method=req_type, url=url, headers=self._header, params=params,
                                                     ssl=False)
                    if response.status == 200:
                        print('Success')
                        return await response.json()
                print('Apparently not')
        return None

    async def _symbol_search_async(self, symbol_list):
        # Return IB conids matching the symbols in the list
        endpoint = 'iserver/secdef/search'
        req_type = 'POST'
        tasks = []
        async with ClientSession() as session:
            for sym in symbol_list:
                tasks.append(self._make_request_async(endpoint, req_type, session, {'symbol': sym}))
            return await asyncio.gather(*tasks)

    def symbol_search(self, symbol_list):
        # regular function to run the async event loop
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._symbol_search_async(symbol_list))

    async def _market_data_history_async(self, conids, exchange, period, bar):
        # This *ought* to need semaphore to limit concurrent requests to 5, but so far I haven't seen errors
        endpoint = 'iserver/marketdata/history'
        req_type = 'GET'
        tasks = []
        async with ClientSession() as session:
            for conid in conids:
                tasks.append(self._make_request_async(endpoint=endpoint, req_type=req_type, session=session,
                                     params={'conid': conid, 'exchange': exchange, 'period': period, 'bar': bar}))
            return await asyncio.gather(*tasks, return_exceptions=True)

    def market_data_history(self, conids, exchange, period, bar):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._market_data_history_async(conids, exchange, period, bar))


    def get_accounts(self):
        # Need to test and see if it makes sense to set attribs from here
        endpoint = 'portfolio/accounts'
        req_type = 'GET'
        return self._make_request(endpoint=endpoint, req_type=req_type)

    def market_data_live(self, conids, fields=None):
        endpoint = 'iserver/marketdata/snapshot'
        req_type = 'GET'
        conid_str =  ','.join(map(lambda i: str(i), conids))
        if fields is not None:
            field_str = ','.join(map(lambda i: str(i), fields))
        if not self.accounts_queried:
            self.get_accounts()
        params = {'conids': conid_str, 'fields': field_str}
        response = self._make_request(endpoint=endpoint, req_type=req_type, params=params)
        # Call repeatedly until all fields present
        bools = [str(j) in response[i].keys() for i in range(len(response)) for j in fields]
        timeout = 100
        while (not all(bools)) and (timeout > 0):
            sleep(.25)
            response = self._make_request(endpoint=endpoint, req_type=req_type, params=params)
            bools = [str(j) in response[i].keys() for i in range(len(response)) for j in fields]
            timeout -= 1
        self._make_request(endpoint='iserver/marketdata/unsubscribeall', req_type='GET')
        return response

    def get_positions(self, id, period='1D'):
        # Need to see what return looks like and page through for more than 30 positions
        pageId = 0
        endpoint = f'portfolio/{id}/positions/{pageId}'
        req_type = 'GET'
        params = {'period': period}
        return self._make_request(endpoint=endpoint, req_type=req_type, params=params)

    def place_order(self, id, conid, cOID, orderType, outsideRTH, price, side, quantity, tif, useAdaptive):
        endpoint = f'iserver/account/{id}/order'
        req_type = 'POST'
        params = {'conid': conid, 'secType': str(conid)+':STK', 'cOID': cOID,
                  'orderType': orderType, 'outsideRTH': outsideRTH, 'price': price, 'side': side,
                  'quantity': quantity, 'tif': tif, 'useAdaptive': useAdaptive}
        return self._make_request(endpoint=endpoint, req_type=req_type, params=params)

    def reply_order(self, id, confirmed):
        endpoint = f'iserver/reply/{id}'
        req_type = 'POST'
        params = {'confirmed': confirmed}
        return self._make_request(endpoint=endpoint, req_type=req_type, params=params)