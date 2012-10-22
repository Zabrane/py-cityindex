#!/usr/bin/env python

from __future__ import absolute_import

import csv
import math
import sys
import threading

import cityindex
import base


def main(opts, args, api, streamer):
    if not args:
        print 'Need at least one symbol to lookup.'
        return

    api.login()
    if opts.spread:
        method = api.list_spread_markets
    else:
        method = api.list_cfd_markets

    for i in xrange(len(args)):
        args[i] += opts.suffix or ''

    writer = csv.writer(sys.stdout, quoting=csv.QUOTE_ALL)
    lock = threading.Lock()

    def write(*row):
        with lock:
            writer.writerow(row)

    def dump(pr):
        sprd = pr['Offer'] - pr['Bid']
        s, market = markets[pr['MarketId']]
        write(pr['TickDate'].strftime('%H:%M:%S.%f'),
              s.upper(),
              pr['Price'], sprd, pr['Bid'], pr['Offer'],
              pr['High'], pr['Low'],
              pr['Change'])

    markets, unknown = base.threaded_lookup(method, args)
    if unknown:
        print '# Unknown:', ', '.join(unknown)

    for market_id, (ric, market) in markets.iteritems():
        streamer.prices.listen(dump, market_id)

    write('Date', 'RIC', 'Price', 'Spread', 'Bid', 'Offer', 'High', 'Low', 'Change')
    raw_input()
    streamer.stop()


if __name__ == '__main__':
    base.main_wrapper(main)
