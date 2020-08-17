import pandas as pd
import warnings

def filter_US_conids(search_results):
    # Select the primary US conid from a list of lists, returning None for errors
    US_exchanges = ['NYSE', 'NASDAQ', 'AMEX', 'ARCA', 'BATS']
    results = [None if len(j) == 0 else j[0] for j in
           [[el['conid'] for el in search_results[i] if el != 'error' and el['description'] in US_exchanges]
           for i in range(len(search_results))]]
    errors = [i for i, t in enumerate(results) if t is None]
    if len(errors) > 0:
        warnings.warn(f'None objects in locations {errors}.')
    return results

def data_bars_to_df(bars):
    # Takes a market_data_history() response and converts it to a dataframe
    # Remove any None responses and warn
    errors = [i for i, t in enumerate(bars) if t is None]
    if len(errors) > 0:
        warnings.warn(f'None objects in locations {errors}.')
        bars = [b for b in bars if b is not None]
    # Create a dictionary that's almost the multi-indexed df we want
    bar_dict = {(pd.to_datetime(j['t'], utc=True, unit='ms'), i['symbol']):
            [j['o'], j['h'], j['l'], j['c'], j['v']]
            for i in bars for j in i['data']}
    # Put it into a df
    idx = pd.MultiIndex.from_tuples(bar_dict.keys())
    cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    df = pd.DataFrame(bar_dict.values(), index=idx, columns=cols)
    return(df)