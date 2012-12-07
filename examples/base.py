
import Queue
import json
import logging
import optparse
import os
import re
import shlex
import sys
import threading

import cityindex
import lightstreamer


LOG = logging.getLogger('base')
CONF_PATH = os.path.expanduser('~/.py-cityindex.conf')
SESSION_PATH = os.path.expanduser('~/.py-cityindex.session')


def filename_for(opts, market, kind):
    subbed = re.sub('[ /()]+', '_', market['Name'])
    return 'CityIndex_%s_%s_%s_%d.csv' % (
        'cfd' if 'CFD' in market['Name'] else 'bet',
        kind,
        subbed,
        market['MarketId']
    )


class ThreadWaker(object):
    def __init__(self):
        self.rpipe,\
        self.wpipe = os.pipe()
        self.queue = Queue.Queue()

    def callback(self):
        os.read(self.rpipe, 1)
        func, args, kwargs = self.queue.get()
        lightstreamer.run_and_log(func, *args, **kwargs)

    def fileno(self):
        return self.rpipe

    def put(self, func, *args, **kwargs):
        self.queue.put((func, args, kwargs))
        os.write(self.wpipe, ' ')


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
        import time
        while not self.queue.empty():
            time.sleep(1)
        self.queue.join()
        # blocks SIGINT:
        #self.queue.join()

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


def threaded_lookup(searcher, strs):
    markets = {}
    unknown = []

    def lookup(s):
        matches = searcher(s)
        if matches:
            for match in matches:
                markets[match['MarketId']] = s, match
        else:
            unknown.append(s)

    tp = ThreadPool()
    for s in strs:
        tp.put(lookup, s)
    tp.join()
    return markets, unknown


def parse_options():
    parser = optparse.OptionParser()
    parser.add_option('--raw', action='store_true', default=False,
        help='Don\'t pad missing bars, return only what API returns.')
    parser.add_option('--bars', type='int', default=1440,
        help='Number of bars to dump')
    parser.add_option('--interval', choices=('minute', 'hour', 'day'),
        default='minute', help='Bar interval')
    parser.add_option('--chop', action='store_true', default=False,
        help='Prune padded bars to match requested bar count.')
    parser.add_option('--span', type='int', default=1,
        help='Span of a single bar')
    parser.add_option('--bycode', action='store_true',
        help='Search by code instead of name')
    parser.add_option('--options', action='store_true',
        default=False, help='Match options')
    parser.add_option('--daily', action='store_true',
        help='Select daily markets only')
    parser.add_option('--username', help='CityIndex username')
    parser.add_option('--password', help='CityIndex password')
    parser.add_option('--debug', action='store_true')
    parser.add_option('--cfd', action='store_const',
                      const='cfd', help='Search for CFD markets.')
    parser.add_option('--binary', action='store_true',
                      help='Search for binary option markets.')
    parser.add_option('--bet', action='store_true',
                      help='Search for spread bet markets.')
    parser.add_option('--suffix',
        help='Append suffix to each symbol (e.g. .O=NASDAQ, .N=NYSE')

    args = sys.argv[1:]
    if os.path.exists(CONF_PATH):
        with file(CONF_PATH) as fp:
            args = shlex.split(fp.readline()) + args

    return parser.parse_args(args)


def main_wrapper(main):
    opts, args = parse_options()
    if not ((opts.username and opts.password)\
            and (opts.cfd or opts.bet)):
        print
        print 'Need username, password, and --cfd or --bet'
        sys.exit(1)

    level = logging.DEBUG if opts.debug else logging.INFO
    logging.basicConfig(level=level)
    logging.getLogger('requests').setLevel(logging.WARN)

    key = ':'.join((opts.username, str(hash(opts.password))))
    if os.path.exists(SESSION_PATH):
        with file(SESSION_PATH) as fp:
            session_cache = json.load(fp)
    else:
        session_cache = {}

    api = cityindex.CiApiClient(opts.username, opts.password,
        session_id=session_cache.get(key))
    if not api.session_id:
        api.login()
    session_cache[key] = api.session_id

    with file(SESSION_PATH, 'w') as fp:
        json.dump(session_cache, fp, sort_keys=True, indent=True)

    streamer = cityindex.CiStreamingClient(api)

    method = api.list_spread_markets if opts.bet else api.list_cfd_markets
    kwarg = 'code' if opts.bycode else 'name'
    def searcher(s):
        hits = api.market_search(s,
            by_code=opts.bycode,
            by_name=not opts.bycode,
            spread=opts.bet,
            cfd=opts.cfd,
            binary=opts.binary,
            options=opts.options)

        prefix = s.split()[0].lower()
        hits = filter(lambda m: m['Name'].lower().startswith(prefix), hits)

        if opts.daily:
            quarter_re = re.compile(' (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|'
                                      'Oct|Nov|Dec) [0-9]+ | Spread')
            hits = filter(lambda m: not quarter_re.search(m['Name']), hits)
        return hits

    main(opts, args, api, streamer, searcher)
