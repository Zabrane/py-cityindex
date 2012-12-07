
import Queue
import errno
import json
import operator
import time

import numpy
import flask
import werkzeug.serving

import base


SEARCHER = None
API = None
STREAMER = None

MCACHE = {}
BCACHE = {}
WINDOW = 60 * 60

MODIFIERS = sorted('cfd binary bet daily options'.split())
INTERVALS = 'MINUTE HOUR DAY'.split()
INTERVAL_MAP = {
    'MINUTE': 60,
    'HOUR': 60 * 60,
    'DAY': 86400
}

app = flask.Flask(__name__)


class QuietRequestHandler(werkzeug.serving.WSGIRequestHandler):
    def finish(self, *args, **kwargs):
        try:
            werkzeug.serving.WSGIRequestHandler.finish(self, *args, **kwargs)
        except IOError, e:
            if e[0] == errno.EPIPE:
                return
            raise


class Channel(object):
    self = None
    def __init__(self):
        self.id = int(time.time() * 1000)
        self.subscriptions = {}
        self.queue = Queue.Queue(3)
        self.queue.put(('id', self.id))
        Channel.self = self

    def _generate(self):
        while True:
            obj = json.dumps(self.queue.get())
            yield '<script>window.parent.deliverMsg(%s);</script>\n' % obj

    def make_response(self):
        return flask.Response(self._generate(), mimetype='text/html')

    @classmethod
    def put(cls, chan, obj):
        if not cls.self:
            return False
        return cls.self.real_put(chan, obj)

    def real_put(self, chan, obj):
        try:
            self.queue.put((chan, obj), block=False)
            return True
        except Queue.Full:
            return False


class PriceSubscription(object):
    def __init__(self, market_ids):
        self.market_ids = market_ids
        STREAMER.prices.listen(self.on_price, market_ids)

    def on_price(self, bar):
        if not Channel.put('price', bar):
            self.unsubscribe()

    def unsubscribe(self):
        print 'Unlistening %r from %r' % (self.on_price, self.market_ids)
        STREAMER.prices.unlisten(self.on_price, self.market_ids)


class AccountMarginSubscription(object):
    def __init__(self, _):
        STREAMER.account_margin.listen(self.on_margin)

    def on_margin(self, margin):
        if not Channel.put('account', margin):
            self.unsubscribe()

    def unsubscribe(self):
        print 'Unlistening %r' % (self.on_price)
        STREAMER.account_margin.unlisten(self.on_margin)


def jsonify(o):
    return flask.Response(
        json.dumps(o, indent=None if flask.request.is_xhr else 2),
        mimetype='application/json')


@app.route('/')
def on_index():
    return flask.render_template('index.tmpl', opts=OPTS, mods=MODIFIERS,
        intervals=INTERVALS, getattr=getattr)


@app.route('/channel')
def on_channel():
    return Channel().make_response()

MAP = {
    'price': PriceSubscription,
    'account': AccountMarginSubscription
}


@app.route('/subscribe')
def subscribe():
    t = flask.request.args['t']
    s = tuple(flask.request.args['s'].split(','))
    if Channel.self:
        Channel.self.subscriptions[(t, s)] = MAP[t](s)
    return ''


@app.route('/unsubscribe')
def unsubscribe():
    t = flask.request.args['t']
    s = tuple(flask.request.args['s'].split(','))
    if not Channel.self:
        return 'false'
    sub = Channel.self.subscriptions.pop((t, s), None)
    if sub:
        sub.unsubscribe()
    return str(sub is not None)


@app.route('/markets')
def on_markets():
    old = operator.attrgetter(*MODIFIERS)(OPTS)
    for k in MODIFIERS:
        setattr(OPTS, k, flask.request.args.get(k) == '1')
    if old != operator.attrgetter(*MODIFIERS)(OPTS):
        MCACHE.clear()
    s = flask.request.args['s']
    t, o = MCACHE.get(s, (0, 0))
    if (not t) or (time.time() - WINDOW) > t:
        o = SEARCHER(s)
        MCACHE[s] = time.time(), o
    return jsonify(o)


def make_bin(bars):
    ar = numpy.empty(5 * len(bars))
    base_open = len(bars)
    base_high = len(bars) * 2
    base_low = len(bars) * 3
    base_close = len(bars) * 4

    for idx, bar in enumerate(bars):
        ar[idx] = bar['BarDate']
        ar[base_open + idx] = bar['Open']
        ar[base_high + idx] = bar['High']
        ar[base_low + idx] = bar['Low']
        ar[base_close + idx] = bar['Close']
    return ar.tostring()


@app.route('/bars')
def on_bars():
    market_id = int(flask.request.args['s'])
    interval = flask.request.args.get('interval', 'MINUTE')
    bars = int(flask.request.args.get('bars', '1440'))
    span = int(flask.request.args.get('span', '1'))

    key = market_id, interval, span

    old = BCACHE.setdefault(key, [0, []])
    if old:
        import math
        cnt = int(math.ceil((time.time() - old[0]) / INTERVAL_MAP[interval]))
    else:
        cnt = max(1000, bars)
    cnt = min(cnt, bars)

    result = API.market_bars(market_id, bars=cnt,
        interval=interval, span=span)
    new = result['PriceBars'] + [result['PartialPriceBar']]
    while new and old[1] and old[1][-1]['BarDate'] >= new[0]['BarDate']:
        old[1].pop()
    old[1].extend(new)
    old[0] = time.time()

    bars = old[1][-bars:]
    if flask.request.args.get('f') == 'bin':
        return flask.Response(make_bin(bars))
    return jsonify(bars)


def main(opts, args, api, streamer, searcher):
    global API, SEARCHER, STREAMER, OPTS
    API = api
    SEARCHER = searcher
    STREAMER = streamer
    OPTS = opts

    app.run(debug=True, threaded=True, request_handler=QuietRequestHandler)

if __name__ == '__main__':
    base.main_wrapper(main)
