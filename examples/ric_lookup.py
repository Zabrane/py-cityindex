#!/usr/bin/env python

"""Given a list of RICs on the command line, find the corresponding CityIndex
market ID and print it to stdout.

PYTHONPATH=. examples/ric_lookup.py --username=.. --password=.. --cfd --suffix=.L  $(< examples/ftse.syms ) 
"""

from __future__ import absolute_import

import csv
import optparse
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

    write('RIC', 'MarketId', 'Description')

    def lookup(ric):
        matches = searcher(ric)
        if matches:
            market = matches[0]
            write(ric.upper(), market['MarketId'], market['Name'])
        else:
            write(ric.upper(), '-', '-')

    tp = base.ThreadPool()
    for ric in args:
        tp.put(lookup, ric)
    tp.join()

if __name__ == '__main__':
    base.main_wrapper(main)
