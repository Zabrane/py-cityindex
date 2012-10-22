#!/usr/bin/env python

"""Given a list of RICs on the command line, find the corresponding CityIndex
market ID and print it to stdout.

PYTHONPATH=. examples/ric_lookup.py --username=.. --password=.. --cfd --suffix=.L  $(< examples/ftse.syms ) 
"""

from __future__ import absolute_import

import csv
import optparse
import sys

import cityindex


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

    writer = csv.writer(sys.stdout, quoting=csv.QUOTE_ALL)
    write = writer.writerow
    write(('RIC', 'MarketId', 'Description'))

    for ric in args:
        ric += opts.suffix or ''
        matches = method(code=ric)
        if matches:
            market = matches[0]
            write((ric.upper(), market['MarketId'], market['Name']))
        else:
            write((ric.upper(), '-', '-'))


if __name__ == '__main__':
    main()
