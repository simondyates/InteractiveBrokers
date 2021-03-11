import pandas as pd
from IBClient import IBClient
from IBUtils import filter_US_conids
import urllib3
import sys
import time
ll_path = '/home/ubuntu/Dropbox/DataSci/PycharmProjects/LeadLag/Algoseek'
if ll_path not in sys.path:
    sys.path.append(ll_path)
from utils import s3_to_pickle, s3_from_pickle

def get_conids(tickers):
    ib = IBClient()
    ib.connect()
    time.sleep(3)
    all_conids = ib.symbol_search(tickers)
    return filter_US_conids(all_conids)

def dupe_spaced(conids):
    # I want BRK B, BRK.B and BRK/B all to have an entry
    spaced = conids[conids.index.str.contains(' ')]
    dotted = spaced.copy()
    dotted.index = dotted.index.str.replace(' ', '.')
    slashed = spaced.copy()
    slashed.index = slashed.index.str.replace(' ', '/')
    return conids.append([dotted, slashed])

def divide_chunks(tickers, size):
    # Divide the tickers into chunks of smaller sizes
    for i in range(0, len(tickers), size):
        yield tickers[i:i + size]

def save_conids(tickers, size, revalidate=False):
    # Read in current conid.pkl and add new tickers if needed
    if not revalidate:
        try:
            conids = s3_from_pickle('Conids/conids.pkl')
            tickers = [t for t in tickers if t not in conids.index]
        except:
            # If there's no S3 conid file we need to revalidate all tickers
            print('No conid file found despite revalidate=False')
            revalidate = True
    tickers = [t.replace('.', ' ').replace('/', ' ') for t in tickers]
    if len(tickers) > 0:
        for tcks in divide_chunks(tickers, size):
            print('Querying IB')
            new_conids = get_conids(tcks)
            # Filter out none values
            tcks = [tcks[i] for i in range(len(new_conids)) if new_conids[i] is not None]
            new_conids = [new_conids[i] for i in range(len(new_conids)) if new_conids[i] is not None]
            new_conids = pd.Series(new_conids, index=tcks, dtype='uint64')
            new_conids = dupe_spaced(new_conids)
            if not revalidate:
                conids = conids.append(new_conids)
                s3_to_pickle(conids, 'Conids/conids.pkl')
            else:
                s3_to_pickle(new_conids, 'Conids/conids.pkl')


if __name__ == '__main__':
    urllib3.disable_warnings()
    stocks = s3_from_pickle('Universes/Current/Stocks.pkl')
    etfs = s3_from_pickle('Universes/Current/ETFs.pkl')
    tickers = stocks + etfs
    save_conids(tickers, 500, False)