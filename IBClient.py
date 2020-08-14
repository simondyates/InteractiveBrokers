import subprocess
import re
from time import sleep
import webbrowser
import os
import signal
import requests
import asyncio
from aiohttp import ClientSession

class IBClient(object):
    def __init__(self):
        self._ib_gateway_url = r'https://127.0.0.1:5000'
        self._ib_gateway_path = self._ib_gateway_url + r'/v1/portal/'
        self._client_portal_folder = './clientportal'
        self._header = {'Content-Type': 'application/json'}
        self.pid = None
        self.is_authenticated = False

    def get_gateway_pid(self):
        # Determine if the IB gateway is running and return pid if so
        out = subprocess.run(['lsof', '-i', 'tcp:5000'], capture_output=True)
        match_ = re.match('^[^\d]*(\d+)', out.stdout.decode('utf-8'))
        if match_ is not None:
            self.pid = int(match_.group(1))
        return self.pid

    def stop_sso(self):
        if self.pid is not None:
            os.kill(self.pid, signal.SIGTERM)

    def check_authenticated(self):
        # Checks to see if an existing sso session is authenticated
        content = self._make_request(endpoint='iserver/auth/status', req_type='POST')
        if content is None:
            self.is_authenticated = False
        else:
            self.is_authenticated = True
        return self.is_authenticated

    def _make_request(self, endpoint, req_type, params=None):
        url = self._ib_gateway_path + endpoint

        if req_type == 'POST' and params is not None:
            response = requests.post(url, headers=self._header, json=params, verify=False)
        elif req_type == 'POST' and params is None:
            response = requests.post(url, headers=self._header, verify=False)
        elif req_type == 'GET' and params is not None:
            response = requests.get(url, headers=self._header, json=params, verify=False)
        elif req_type == 'GET' and params is None:
            response = requests.get(url, headers=self._header, verify=False)

        if response.status_code == 200:
            return response.json()
        else:
            return None

    def connect(self):
        # Determines whether there's already a valid connection and connects if not
        if self.check_authenticated():
            return True
        elif self.get_gateway_pid() is None:
            subprocess.Popen(args=['bin/run.sh', 'root/conf.yaml'], cwd='./clientportal', preexec_fn=os.setsid)
        else:
            webbrowser.open(self._ib_gateway_url, new=2)
        sleep(2)
        _ = input("\nPress Enter once you've logged in successfully.")
        self.is_authenticated = self.check_authenticated()
        return self.is_authenticated

    def reauthenticate(self):
        # I have no idea what this does
        content = self._make_request(endpoint=r'iserver/reauthenticate', req_type='POST')
        if content is None:
            return None
        else:
            return True

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
            return await asyncio.gather(*tasks)

    def market_data_history(self, conids, exchange, period, bar):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._market_data_history_async(conids, exchange, period, bar))

