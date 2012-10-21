#!/usr/bin/env python

from __future__ import absolute_import

import logging
import signal
import sys
import time

import cityindex


def on_price_update(price):
    print 'Price update:', price


def main():
    if len(sys.argv) != 3:
        print 'Usage: %s USERNAME PASSWORD' % sys.argv[0]
        raise SystemExit(1)

    logging.basicConfig(level=logging.DEBUG)

    api = cityindex.CiApiClient(cityindex.LIVE_API_URL,
        sys.argv[1], sys.argv[2])
    api.login()

    streamer = cityindex.CiStreamingClient(cityindex.LIVE_STREAM_URL, api)
    for market in api.list_cfd_markets(max_results=1):
        streamer.prices.listen(on_price_update, market['MarketId'])

    time.sleep(10)
    streamer.stop()


if __name__ == '__main__':
    main()
