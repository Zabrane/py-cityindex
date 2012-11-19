#!/usr/bin/env python

from __future__ import absolute_import

import csv
import datetime
import math
import sys
import time
import threading
import os

import cityindex
import base


HEADER = ('Date', 'UnixTime', 'RecvTime', 'MarketId', 'Price',
          'Bid', 'Offer', 'Direction', 'High', 'Low', 'Change', 'AuditId')


def reset_tz():
    os.environ['TZ'] = 'UTC'
    time.tzset()


def format_ts(ts):
    dt = datetime.datetime.fromtimestamp(ts)
    msecs = '.%03d' % (dt.microsecond / 1000.0)
    return dt.strftime('%H:%M:%S') + msecs


def format_float(f, prec=6):
    return '%.*f' % (prec, f)


def make_writer(fp):
    return csv.writer(fp, quoting=csv.QUOTE_ALL)


def price_to_row(market, price):
    prec = market['PriceDecimalPlaces']
    return (format_ts(price['TickDate']),
            format_float(price['TickDate'], 2),
            format_float(time.time()),
            market['MarketId'],
            format_float(price['Price'], prec),
            format_float(price['Bid'], prec),
            format_float(price['Offer'], prec),
            price['Direction'],
            format_float(price['High'], prec),
            format_float(price['Low'], prec),
            format_float(price['Change'], prec),
            price['AuditId'])


def main(opts, args, api, streamer, searcher):
    if not args:
        print 'Need at least one symbol to lookup.'
        return

    api.login()
    for i in xrange(len(args)):
        args[i] += opts.suffix or ''

    stdout = make_writer(sys.stdout)
    stdout.writerow(HEADER)
    market_writer_map = {}

    def dump(price):
        if not price['MarketId']:
            return
        s, market = markets[price['MarketId']]
        row = price_to_row(market, price)
        stdout.writerow(row)
        market_writer_map[price['MarketId']].writerow(row)

    markets, unknown = base.threaded_lookup(searcher, args)
    if unknown:
        print '# Unknown:', ', '.join(unknown)

    for market_id, (ric, market) in markets.iteritems():
        fp = file(base.filename_for(opts, market, kind='ticks'), 'a, 1')
        writer = csv.writer(fp, quoting=csv.QUOTE_ALL)
        market_writer_map[market_id] = writer
        if os.path.getsize(fp.name) == 0:
            writer.writerow(HEADER)
        streamer.prices.listen(dump, market_id)

    raw_input()
    streamer.stop()


if __name__ == '__main__':
    base.main_wrapper(main)
