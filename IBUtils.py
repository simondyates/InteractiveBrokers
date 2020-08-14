def filter_US_conids(search_results):
    # Select the primary US conid from a list of lists, returning None for errors
    US_exchanges = ['NYSE', 'NASDAQ', 'AMEX', 'ARCA', 'BATS']
    results = [None if len(j) == 0 else j[0] for j in
               [[el['conid'] for el in search_results[i] if el != 'error' and el['description'] in US_exchanges]
                for i in range(len(search_results))]]
    return results