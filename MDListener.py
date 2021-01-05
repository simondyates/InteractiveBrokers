import pandas as pd
import os
from IBClient import IBClient
from IBUtils import data_bars_to_df
import time
import urllib3
import sys
ll_path = '/home/ubuntu/Dropbox/DataSci/PycharmProjects/LeadLag/Algoseek'
if ll_path not in sys.path:
    sys.path.append(ll_path)
from utils import s3_to_pickle, s3_from_pickle

def test_connection():
    result = ib.market_data_history([int(conids[0])], '', '1min', '1min')
    if result is None:
        print('None Response')
    else:
        result = ib.tickle()
        print(f'Connection alive for next {result/60000:.2f} minutes.')

def update_intraday():
    global day_data
    print(f'Querying IB at {pd.Timestamp.now():%H:%M}')
    try:
        bool = ib.check_authenticated()
        if not bool:
            ib.reauthenticate()
    except:
        ib.connect()
    data = ib.market_data_history(conids, '', '1min', '1min') # Changed exchange field from SMART
    df = data_bars_to_df(data)
    if df is None:
        print('Received empty response')
        return None # Give up on this one but stay alive to try again later
    first_time = df.index.get_level_values(0)[0]
    df = df.loc[first_time]
    idx = pd.MultiIndex.from_product([[first_time], df.index])
    df.index = idx
    day_data = pd.concat([day_data, df])
    s3_to_pickle(day_data, f'DayData/{pd.Timestamp.now():%Y-%m-%d}.pkl')
    return None

def get_next_minute_ten(dt):
    if dt.minute == 59:
        return pd.Timestamp(year=dt.year, month=dt.month, day=dt.day,
                                   hour=dt.hour + 1, minute=0, second=10,
                                   tz='US/Eastern')  # Add 10 seconds just to be careful
    else:
        return pd.Timestamp(year=dt.year, month=dt.month, day=dt.day,
                                   hour=dt.hour, minute=dt.minute + 1, second=10,
                                   tz='US/Eastern')  # Add 10 seconds just to be careful

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
        conids = conids.astype('uint64')

    # Connect and (re-)initialise day_data df
    ib = IBClient()
    ib.connect()
    dayfile = f'DayData/{pd.Timestamp.now():%Y-%m-%d}.pkl'
    try:
        day_data = s3_from_pickle(dayfile)
    except:
        day_data = pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume'])

    # Loop until start of day, testing connection health every minute
    now_dt = pd.Timestamp.now(tz='US/Eastern')
    print(now_dt)
    start_dt = pd.Timestamp(year=now_dt.year, month=now_dt.month, day=now_dt.day, hour=9, minute=30, tz='US/Eastern')
    end_dt = pd.Timestamp(year=now_dt.year, month=now_dt.month, day=now_dt.day, hour=16, minute=0, tz='US/Eastern')
    while now_dt <= start_dt:
        next_minute = get_next_minute_ten(now_dt)
        time.sleep((next_minute - now_dt).seconds)
        test_connection()
        now_dt = pd.Timestamp.now(tz='US/Eastern')

    # Loop until end of day, saving 1 minute bar data
    while now_dt < end_dt:
        next_minute = get_next_minute_ten(now_dt)
        time.sleep((next_minute - now_dt).seconds)
        update_intraday()
        now_dt = pd.Timestamp.now(tz='US/Eastern')
