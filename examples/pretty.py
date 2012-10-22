#!/usr/bin/env python

from __future__ import absolute_import

import math
import os
import sys
import threading
import time

import urwid

import cityindex
import base


class Table(object):
    def __init__(self, row_key):
        self.row_key = row_key
        self._fields = []
        self._field_pile_map = {}
        self._field_formatter_map = {}
        self._row_fields_map = {}
        self._row_data = {}
        self._columns = urwid.Columns([], dividechars=2)

    def add_field(self, name, formatter):
        self._fields.append(name)
        title = urwid.Text([('title', name)])
        pile = urwid.Pile([title])
        self._field_pile_map[name] = pile
        self._field_formatter_map[name] = formatter
        self._columns.widget_list.append(pile)

    def end_fields(self):
        for field in self._fields:
            self._field_pile_map[field].widget_list.append(urwid.Divider())

    def append(self, row):
        key = self.row_key(row)
        fields = []
        for field in self._fields:
            fields.append(urwid.Text(''))
            self._field_pile_map[field].widget_list.append(fields[-1])
        self._row_fields_map[key] = fields
        self.update(row)

    def update(self, row):
        key = self.row_key(row)
        prev = self._row_data.get(key, {})

        fields = self._row_fields_map[key]
        for idx, field in enumerate(self._fields):
            last = prev.get(field)
            cur = row.setdefault(field)
            formatter = self._field_formatter_map[field]
            attrs = formatter(last, cur)
            if attrs is None:
                attrs = ''
            elif isinstance(attrs, (int, float)):
                if last < cur:
                    attrs = [('uptick', unicode(attrs))]
                elif last > cur:
                    attrs = [('downtick', unicode(attrs))]
                else:
                    attrs = unicode(attrs)
            elif not isinstance(attrs, (list, tuple)):
                attrs = unicode(attrs)
            fields[idx].set_text(attrs)
        self._row_data[key] = row



def main(opts, args, api, streamer):
    if not args:
        print 'Need at least one symbol to lookup.'
        return

    api.login()
    if opts.spread:
        method = api.list_spread_markets
    else:
        method = api.list_cfd_markets

    for i in xrange(len(args)):
        args[i] += opts.suffix or ''

    palette = [
        ('title', 'yellow', 'default', 'bold'),
        ('uptick', 'white', 'dark green', 'bold'),
        ('downtick', 'white', 'dark red', 'bold'),
        ('I say', 'light red,bold', 'default', 'bold'),
    ]

    reply = urwid.Text(u"")
    button = urwid.Button(u'Exit')
    div = urwid.Divider()
    pile = urwid.Pile([reply, button])
    top = urwid.Filler(pile, valign='top')

    markets = None
    data = {}

    def on_exit_clicked(button):
        raise urwid.ExitMainLoop()

    #urwid.connect_signal(ask, 'change', on_ask_change)
    urwid.connect_signal(button, 'click', on_exit_clicked)

    table = Table(lambda price: price['MarketId'])
    table.add_field('TickDate', lambda _, cur: cur and cur.strftime('%H:%M:%S.%f'))
    for field in 'RIC', 'Price', 'Bid', 'Offer', 'High', 'Low', 'Change':
        table.add_field(field, lambda last, cur: cur)
    table.end_fields()

    pp = urwid.Pile([table._columns, urwid.Divider(), pile])
    top = urwid.Filler(pp, valign='top')
    loop = urwid.MainLoop(top, palette)

    waker = base.ThreadWaker()
    loop.watch_file(waker.fileno(), waker.callback)

    def on_price_update(price):
        if not price['MarketId']:
            return
        ric, market = markets[price['MarketId']]
        price['RIC'] = ric.upper()
        waker.put(table.update, price)

    markets, unknown = base.threaded_lookup(method, args)
    for market_id, (ric, market) in markets.iteritems():
        table.append({'MarketId': market_id})
        streamer.prices.listen(on_price_update, market_id)

    loop.run()
    streamer.stop()


if __name__ == '__main__':
    base.main_wrapper(main)
