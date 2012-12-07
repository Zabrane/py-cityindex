#!/usr/bin/env python

from __future__ import absolute_import

import csv
import math
import sys
import threading

import cityindex
import base

API = None
MARKET_CACHE = {}


def get_market(market_id):
    market = MARKET_CACHE.get(market_id)
    if not market:
        market = API.market_info(market_id)
        MARKET_CACHE[market_id] = market
    return market


def on_order_update(order):
    market = get_market(order['MarketId'])
    order['Market'] = market['Name']
    order['Dir'] = 'BUY' if order['Direction'] else 'SELL'
    #print ('%(CurrencyISO)s %(Type)s %(Dir)s ORDER %(OrderId)d: '
           #'%(Market)r: %(Status)s') % order
    print 'ORDER STATUS',
    from pprint import pprint
    pprint(order)


def on_account_margin(margin):
    print 'ACCOUNT MARGIN',
    from pprint import pprint
    pprint(margin)
    print


def on_trade_margin(margin):
    print 'TRADE MARGIN',
    from pprint import pprint
    pprint(margin)
    print


def main(opts, args, api, streamer, searcher):
    global API
    API = api

    streamer.orders.listen(on_order_update)
    streamer.account_margin.listen(on_account_margin)
    #streamer.trade_margin.listen(on_trade_margin)

    raw_input()
    streamer.stop()


if __name__ == '__main__':
    base.main_wrapper(main)
