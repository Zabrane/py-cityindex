#!/usr/bin/env python

"""Given a list of RICs on the command line, find the corresponding CityIndex
market ID and print it to stdout.

PYTHONPATH=. examples/ric_lookup.py --username=.. --password=.. --cfd --suffix=.L  $(< examples/ftse.syms ) 
"""

from __future__ import absolute_import

import csv
import datetime
import optparse
import re
import sys
import threading

import cityindex
import base


def filename_for(market):
    subbed = re.sub('[ ()]+', '_', market['Name'])
    pfx = 'cfd' if 'CFD' in market['Name'] else 'sb'
    return 'CityIndex_%s_%s_%d.csv' % (pfx, subbed, market['MarketId'])


def tsformat(bar):
    dt = datetime.datetime.fromtimestamp(bar['BarDate'])
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def main(opts, args, api, streamer, searcher):
    if not args:
        print 'Need at least one symbol to lookup.'
        return

    api.login()
    for i in xrange(len(args)):
        args[i] += opts.suffix or ''

    def fetch(market):
        bars = api.market_bars(market['MarketId'],
            span=opts.span, bars=opts.bars)['PriceBars']

        filename = filename_for(market)
        with file(filename, 'w') as fp:
            writer = csv.writer(fp, quoting=csv.QUOTE_ALL)
            write = writer.writerow
            write(('UtcTime', 'Open', 'High', 'Low', 'Close'))
            for bar in bars:
                write((tsformat(bar),
                      bar['Open'], bar['High'], bar['Low'],
                      bar['Close']))

        print 'Wrote %d bars for %d/%s to %r (first:%r, last:%r)' %\
            (len(bars), market['MarketId'], market['Name'], filename,
             bars and tsformat(bars[0]), bars and tsformat(bars[-1]))

    markets, unknown = base.threaded_lookup(searcher, args)
    for unk in unknown:
        print 'Can\'t find %r' % (unk,)

    tp = base.ThreadPool()
    for s, market in markets.values():
        tp.put(fetch, market)
    tp.join()

if __name__ == '__main__':
    base.main_wrapper(main)
