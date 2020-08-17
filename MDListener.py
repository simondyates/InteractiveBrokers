import pandas as pd
from IBClient import IBClient
from IBUtils import data_bars_to_df
import time

def update_intraday():
    print(f'Querying IB at {pd.Timestamp.now():%H:%M}')
    data = ib.market_data_history(conids, 'SMART', '1min', '1min')
    df = data_bars_to_df(data)
    first_time = df.index.get_level_values(0)[0]
    df = df.loc[first_time]
    idx = pd.MultiIndex.from_product([[first_time], df.index])
    df.index = idx
    df = pd.concat([day_data, df])
    df.to_pickle(f'/Volumes/share/StockData/DayData/{pd.Timestamp.now():%Y-%m-%d}.pkl')
    return df

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
    betas = pd.read_pickle('/Volumes/share/StockData/Betas/30min Betas Apr-14 to Jul-14 R2K last 353.pkl')
    universe = betas.index.union(betas.columns)
    universe = [t for t in universe if t not in ['CRC', 'SBBX']]
    conid_db = pd.read_pickle('./data/conids.pkl')
    conids = conid_db.loc[universe]
    ib = IBClient()
    ib.connect()

    day_data = pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume'])
    now_dt = pd.Timestamp.now(tz='US/Eastern')
    print(now_dt)
    start_dt = pd.Timestamp(year=now_dt.year, month=now_dt.month, day=now_dt.day, hour=9, minute=30, tz='US/Eastern')
    end_dt = pd.Timestamp(year=now_dt.year, month=now_dt.month, day=now_dt.day, hour=16, minute=0, tz='US/Eastern')
    while now_dt <= start_dt:
        now_dt = pd.Timestamp.now(tz='US/Eastern')
        next_minute = get_next_minute_ten(now_dt)
        time.sleep((next_minute - now_dt).seconds)
    while now_dt < end_dt:
        now_dt = pd.Timestamp.now(tz='US/Eastern')
        next_minute = get_next_minute_ten(now_dt)
        time.sleep((next_minute - now_dt).seconds)
        update_intraday()
