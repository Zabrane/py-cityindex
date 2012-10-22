
import Queue
import logging
import optparse
import os
import sys
import threading

import cityindex
import lightstreamer


LOG = logging.getLogger('base')


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
        self.queue.join()

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


def threaded_lookup(method, strs):
    markets = {}
    unknown = []

    def lookup(s):
        matches = method(code=s)
        if matches:
            markets[matches[0]['MarketId']] = s, matches[0]
        else:
            unknown.append(s)

    tp = ThreadPool()
    for s in strs:
        tp.put(lookup, s)
    tp.join()
    return markets, unknown


def main_wrapper(main):
    parser = optparse.OptionParser()
    parser.add_option('--username', help='CityIndex username')
    parser.add_option('--password', help='CityIndex password')
    parser.add_option('--debug', action='store_true')
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

    level = logging.DEBUG if opts.debug else logging.INFO
    logging.basicConfig(level=level)
    logging.getLogger('requests').setLevel(logging.WARN)

    api = cityindex.CiApiClient(opts.username, opts.password)
    streamer = cityindex.CiStreamingClient(api)
    main(opts, args, api, streamer)
