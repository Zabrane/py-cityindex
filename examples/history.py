#!/usr/bin/env python

from __future__ import absolute_import

import csv
import datetime
import sys

import cityindex
import base


def tsformat(ts):
    dt = datetime.datetime.fromtimestamp(ts)
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def main(opts, args, api, streamer, searcher):
    keys = ('ExecutedDateTimeUtc', 'TradingAccountId', 'OrderId',
            'OpeningOrderIds', 'MarketId', 'MarketName', 'Direction',
            'OriginalQuantity', 'Quantity', 'Price', 'TradingAccountId',
            'Currency', 'RealisedPnl', 'RealisedPnlCurrency',
            'LastChangedDateTimeUtc')
    dates = ('LastChangedDateTimeUtc', 'ExecutedDateTimeUtc')

    out = []
    for account in api.trading_accounts:
        trades = api.list_trade_history(account['TradingAccountId'])
        for trade in trades['TradeHistory'] + trades['SupplementalOpenOrders']:
            if trade['OrderId'] in trade['OpeningOrderIds']:
                trade['OpeningOrderIds'].remove(trade['OrderId'])
            trade['OpeningOrderIds'] = ';'.join(map(str, trade['OpeningOrderIds']))
            for df in dates:
                trade[df] = tsformat(trade[df])
            out.append([trade[k] for k in keys])

    writer = csv.writer(sys.stdout, quoting=csv.QUOTE_ALL)
    writer.writerow(keys)
    for row in sorted(out):
        writer.writerow(row)

if __name__ == '__main__':
    base.main_wrapper(main)
