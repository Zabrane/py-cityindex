#!/usr/bin/env python

from __future__ import absolute_import

import optparse
import logging
import signal
import sys
import time

import cityindex


def on_price_update(price):
    print 'Price update:', price


def main():
    parser = optparse.OptionParser()
    parser.add_option('--username', help='Username')
    parser.add_option('--password', help='Password')
    parser.add_option('--debug', action='store_true', help='Debug logging')

    opts, args = parser.parse_args()
    if not (opts.username and opts.password):
        parser.print_help()
        print 'Must specify username and password'
        return

    level = logging.DEBUG if opts.debug else logging.INFO
    logging.basicConfig(level=level)

    api = cityindex.CiApiClient(cityindex.LIVE_API_URL,
        opts.username, opts.password)
    api.login()

    streamer = cityindex.CiStreamingClient(cityindex.LIVE_STREAM_URL, api)
    for market in api.list_cfd_markets(max_results=1):
        streamer.prices.listen(on_price_update, 9949)
        streamer.prices.listen(on_price_update, 99500)
        streamer.prices.listen(on_price_update, 99498)

    #time.sleep(^0)
    raw_input()
    streamer.stop()


if __name__ == '__main__':
    main()
