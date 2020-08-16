import pandas as pd
from IBClient import IBClient
from IBUtils import filter_US_conids
import urllib3

def get_conids(tickers):
    ib = IBClient()
    ib.connect()
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

def save_conids(tickers, size):
    # Read in current conid.pkl and add new tickers if needed
    conids = pd.read_pickle('./data/conids.pkl')
    tickers = [t for t in tickers if t not in conids.index]
    tickers = [t.replace('.', ' ').replace('/', ' ') for t in tickers]
    if len(tickers) > 0:
        for tcks in divide_chunks(tickers, size):
            print('Querying IB')
            new_conids = get_conids(tcks)
            new_conids = pd.Series(new_conids, index=tcks, dtype='uint64')
            new_conids = dupe_spaced(new_conids)
            conids = conids.append(new_conids)
            conids.to_pickle('./data/conids.pkl')

def main():
    urllib3.disable_warnings()
    betas = pd.read_pickle('/Volumes/share/StockData/Betas/30min Betas Apr-14 to Jul-14 R2K last 353.pkl')
    tickers = ['GTX']
    save_conids(tickers, 500)

if __name__ == '__main__':
    main()