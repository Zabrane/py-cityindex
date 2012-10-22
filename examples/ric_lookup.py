#!/usr/bin/env python

"""Given a list of RICs on the command line, find the corresponding CityIndex
market ID and print it to stdout.

PYTHONPATH=. examples/ric_lookup.py --username=.. --password=.. --cfd --suffix=.L  $(< examples/ftse.syms ) 
"""

from __future__ import absolute_import

import Queue
import csv
import logging
import optparse
import sys
import threading

import cityindex


LOG = logging.getLogger('ric_lookup')


class ThreadPool(object):
    def __init__(self):
        self.queue = Queue.Queue()
        self.threads = [threading.Thread(target=self._main)
                        for x in range(15)]
        for thread in self.threads:
            thread.setDaemon(True)
            thread.start()

    def put(self, func, *args, **kwargs):
        self.queue.put((func, args, kwargs))

    def join(self):
        self.queue.join()

    def _run_one(self):
        func, args, kwargs = self.queue.get()
        try:
            func(*args, **kwargs)
        except:
            LOG.exception('While invoking %r(*%r, **%r)', func, args, kwargs)
        self.queue.task_done()

    def _main(self):
        while True:
            self._run_one()


def main():
    parser = optparse.OptionParser()
    parser.add_option('--username', help='CityIndex username')
    parser.add_option('--password', help='CityIndex password')
    parser.add_option('--cfd', action='store_true',
                      help='Search for CFD markets.')
    parser.add_option('--spread', action='store_true',
                      help='Search for spread bet markets.')
    parser.add_option('--suffix',
        help='Append suffix to each symbol (e.g. .O=NASDAQ, .N=NYSE')

    opts, args = parser.parse_args()
    if not ((opts.username and opts.password)\
            and (opts.cfd or opts.spread)):
        parser.print_help()
        print
        print 'Need username, password, and --cfd or --spread'
        sys.exit(1)

    if not args:
        print 'Need at least one symbol to lookup.'
        return

    api = cityindex.CiApiClient(cityindex.LIVE_API_URL,
        opts.username, opts.password)
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

    write('RIC', 'MarketId', 'Description')

    def lookup(ric):
        matches = method(code=ric)
        if matches:
            market = matches[0]
            write(ric.upper(), market['MarketId'], market['Name'])
        else:
            write(ric.upper(), '-', '-')

    tp = ThreadPool()
    for ric in args:
        tp.put(lookup, ric)
    tp.join()


if __name__ == '__main__':
    main()
