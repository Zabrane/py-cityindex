#!/usr/bin/env python


from __future__ import absolute_import

import csv
import datetime
import optparse
import re
import sys
import threading

import cityindex
import base


def tsformat(bar):
    dt = datetime.datetime.fromtimestamp(bar['BarDate'])
    return dt.strftime('%Y-%m-%d %H:%M:%S')



def pad_bars(bars, interval):
    if not bars:
        return

    out = []
    last = None
    for bar in bars:
        if last is not None:
            while bar['BarDate'] > (last + interval):
                price = out[-1]['Close']
                padded = {
                    'BarDate': out[-1]['BarDate'] + interval,
                    'Open': price,
                    'High': price,
                    'Low': price,
                    'Close': price
                }
                out.append(padded)
                last = padded['BarDate']
        out.append(bar)
        last = bar['BarDate']
    return out



def main(opts, args, api, streamer, searcher):
    if not args:
        print 'Need at least one symbol to lookup.'
        return

    for i in xrange(len(args)):
        args[i] += opts.suffix or ''

    def fetch(market):
        bars = api.market_bars(market['MarketId'],
            interval=opts.interval.upper(),
            span=opts.span, bars=opts.bars)['PriceBars']

        if opts.raw:
            pad_count = 0
        else:
            orig = len(bars)
            bars = pad_bars(bars, interval=opts.span * 60)
            pad_count = len(bars) - orig
            if opts.chop:
                bars = bars[-opts.bars:]

        filename = base.filename_for(opts, market,
            kind='bars_%s%s' % (opts.span, opts.interval[0].upper()))

        with file(filename, 'w') as fp:
            writer = csv.writer(fp, quoting=csv.QUOTE_ALL)
            write = writer.writerow
            write(('UtcTime', 'Open', 'High', 'Low', 'Close'))
            for bar in bars:
                write((tsformat(bar),
                      bar['Open'], bar['High'], bar['Low'],
                      bar['Close']))

        print 'Wrote %d bars for %d/%s to %r (padded:%d, first:%r, last:%r)' %\
            (len(bars), market['MarketId'], market['Name'], filename,
             pad_count, bars and tsformat(bars[0]),
                        bars and tsformat(bars[-1]))

    markets, unknown = base.threaded_lookup(searcher, args)
    for unk in unknown:
        print 'Can\'t find %r' % (unk,)

    tp = base.ThreadPool()
    for s, market in markets.values():
        tp.put(fetch, market)
    tp.join()

if __name__ == '__main__':
    base.main_wrapper(main)
