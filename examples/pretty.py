#!/usr/bin/env python

from __future__ import absolute_import

import datetime
import logging
import urwid

import cityindex
import lightstreamer
import base


PALETTE = [
    ('title', 'yellow', 'default', 'bold'),
    ('uptick', 'white', 'dark green', 'bold'),
    ('downtick', 'white', 'dark red', 'bold'),
    ('debug', 'light blue', 'default', 'default'),
    ('info', 'dark green', 'default', 'default'),
    ('I say', 'light red,bold', 'default', 'bold'),
    ('online', 'dark green', 'default', 'default'),
    ('reconnecting', 'yellow', 'default', 'default'),
    ('offline', 'dark red', 'default', 'default'),
]


class Table(object):
    def __init__(self, row_key):
        self.row_key = row_key
        self._fields = []
        self._align_map = {}
        self._field_pile_map = {}
        self._field_formatter_map = {}
        self._row_fields_map = {}
        self._row_data = {}
        self._cols = []

    def add_field(self, name, formatter, width=None, align='right'):
        self._fields.append(name)
        title = urwid.Text([('title', name)], align=align)
        self._align_map[name] = align
        pile = urwid.Pile([title])
        self._field_pile_map[name] = pile
        self._field_formatter_map[name] = formatter
        if width:
            pile = ('fixed', width, pile)
        self._cols.append(pile)

    def end_fields(self):
        for field in self._fields:
            self._field_pile_map[field].widget_list.append(urwid.Divider())
        self._columns = urwid.Columns(self._cols, dividechars=2)

    def append(self, row):
        key = self.row_key(row)
        fields = []
        for field in self._fields:
            fields.append(urwid.Text('', self._align_map[field], wrap='clip'))
            self._field_pile_map[field].widget_list.append(fields[-1])
        self._row_fields_map[key] = fields
        self.update(row)

    def update(self, row):
        key = self.row_key(row)
        prev = self._row_data.get(key, row)

        fields = self._row_fields_map[key]
        for idx, field in enumerate(self._fields):
            last = prev.get(field)
            cur = row.setdefault(field)
            if last is None and cur is None:
                fields[idx].set_text('')
                return
            formatter = self._field_formatter_map[field]
            attrs = formatter(last, cur)
            if isinstance(attrs, (int, float)):
                attrs = '%.2f' % attrs
                if last and last < cur:
                    attrs = [('uptick', unicode(attrs))]
                elif last and last > cur:
                    attrs = [('downtick', unicode(attrs))]
                else:
                    attrs = [('default', unicode(attrs))]
            elif not isinstance(attrs, (list, tuple)):
                attrs = [('default', unicode(attrs or ''))]
            fields[idx].set_text(attrs)
        self._row_data[key] = row



class LogTailer(object):
    class Handler(logging.Handler):
        def __init__(self, text):
            logging.Handler.__init__(self)
            self.formatter = logging.Formatter(
                '%(asctime)s %(levelname).1s %(name)s: %(message)s',
                datefmt='%H:%M:%S')
            self.hist = []
            self.text = text

        def emit(self, record):
            self.hist = self.hist[-20:] + [record]
            attrs = []
            for record in reversed(self.hist):
                if record.levelno == logging.DEBUG:
                    color = 'debug'
                else:
                    color = 'info'
                attrs.append((color, self.format(record) + '\n'))
            self.text.set_text(attrs)

    def __init__(self):
        self.text = urwid.Text(u'')
        logging.getLogger().handlers = [self.Handler(self.text)]


def tsformat(ts):
    dt = datetime.datetime.fromtimestamp(ts)
    return dt.strftime('%H:%M:%S')


def main(opts, args, api, streamer, searcher):
    if not args:
        print 'Need at least one symbol to lookup.'
        return

    for i in xrange(len(args)):
        args[i] += opts.suffix or ''

    tailer = LogTailer()

    button = urwid.Button(u'Exit')
    div = urwid.Divider()
    pile = urwid.Pile([button, div, tailer.text])
    top = urwid.Filler(pile, valign='top')

    markets = None
    data = {}

    def on_exit_clicked(button):
        raise urwid.ExitMainLoop()

    urwid.connect_signal(button, 'click', on_exit_clicked)

    table = Table(lambda price: price['MarketId'])
    table.add_field('TickDate', (lambda _, cur: tsformat(cur)), width=8)
    table.add_field('Name', (lambda last, cur: cur), width=35)
    for field in 'Price', 'Spread', 'SprdPct', 'Bid', 'Offer', 'High', 'Low', 'Change':
        table.add_field(field, (lambda _, cur: cur), width=9)
    table.end_fields()

    status = urwid.Text('', align='right')
    pp = urwid.Pile([status, urwid.Divider(), table._columns, urwid.Divider(), pile])
    top = urwid.Filler(pp, valign='top')
    loop = urwid.MainLoop(top, PALETTE)

    waker = base.ThreadWaker()
    loop.watch_file(waker.fileno(), waker.callback)

    def redraw_state(map):
        bits = []
        attrmap = {lightstreamer.STATE_CONNECTING: 'reconnecting',
                   lightstreamer.STATE_CONNECTED: 'online',
                   lightstreamer.STATE_DISCONNECTED: 'offline',
                   lightstreamer.STATE_RECONNECTING: 'reconnecting'}

        for short, name in ('TRADING', cityindex.AS_TRADING), \
                           ('STREAMING', cityindex.AS_STREAMING), \
                           ('ACCOUNT', cityindex.AS_ACCOUNT):
            attr = attrmap.get(map.get(name))
            bits.append((attr, short + '   '))
        status.set_text(bits)

    streamer.on_state(lambda map: waker.put(redraw_state, map))

    def on_price_update(price):
        if price['MarketId']:
            ric, market = markets[price['MarketId']]
            price['RIC'] = ric.upper()
            price['Spread'] = price['Bid'] - price['Offer']
            price['SprdPct'] = (price['Spread'] / price['Price']) * 100
            waker.put(table.update, price)

    markets, unknown = base.threaded_lookup(searcher, args)
    for market_id, (ric, market) in markets.iteritems():
        table.append({'MarketId': market_id})
        streamer.prices.listen(on_price_update, market_id)

    loop.run()
    streamer.stop()


if __name__ == '__main__':
    base.main_wrapper(main)
