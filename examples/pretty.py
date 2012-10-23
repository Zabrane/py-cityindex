#!/usr/bin/env python

from __future__ import absolute_import

import logging
import urwid

import cityindex
import base


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
            fields.append(urwid.Text('', self._align_map[field]))
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
        ('debug', 'light blue', 'default', 'default'),
        ('info', 'dark green', 'default', 'default'),
        ('I say', 'light red,bold', 'default', 'bold'),
    ]

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
    table.add_field('TickDate', lambda _, cur: cur and cur.strftime('%H:%M:%S'),
        width=8)
    table.add_field('Name', (lambda last, cur: cur), width=20)
    for field in 'Price', 'Bid', 'Offer', 'High', 'Low', 'Change':
        table.add_field(field, (lambda _, cur: cur), width=9)
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
