#!/usr/bin/env python

from __future__ import absolute_import

import csv
import math
import sys
import threading

import cityindex
import base


def main(opts, args, api, streamer, searcher):
    if not args:
        print 'Need at least one symbol to lookup.'
        return

    api.login()
    for i in xrange(len(args)):
        args[i] += opts.suffix or ''

    writer = csv.writer(sys.stdout, quoting=csv.QUOTE_ALL)
    lock = threading.Lock()

    def write(*row):
        with lock:
            writer.writerow(row)

    def dump(pr):
        if not pr['MarketId']:
            return
        sprd = (pr['Offer'] or 0) - (pr['Bid'] or 0)
        s, market = markets[pr['MarketId']]
        write(pr['TickDate'].strftime('%H:%M:%S.%f'),
              pr['MarketId'], pr['Name'],
              pr['Price'], sprd, pr['Bid'], pr['Offer'],
              pr['High'], pr['Low'], pr['Change'])

    markets, unknown = base.threaded_lookup(searcher, args)
    if unknown:
        print '# Unknown:', ', '.join(unknown)

    for market_id, (ric, market) in markets.iteritems():
        streamer.prices.listen(dump, market_id)

    write('Date', 'MarketId', 'Name', 'Price', 'Spread',
          'Bid', 'Offer', 'High', 'Low', 'Change')
    raw_input()
    streamer.stop()


if __name__ == '__main__':
    base.main_wrapper(main)
