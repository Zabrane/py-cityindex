#
# py-CityIndex
# Copyright (C) 2012 David Wilson
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

import json
import logging
import re
import sys
import urllib
import urllib2
import urlparse

import lightstreamer


# Production.
LIVE_API_URL = 'https://ciapi.cityindex.com/tradingapi/'
LIVE_STREAM_URL = 'https://push.cityindex.com/lightstreamer/'

# Pre-production.
TEST_API_URL = 'https://ciapipreprod.cityindextest9.co.uk/tradingapi/'
TEST_STREAM_URL = 'https://pushpreprod.cityindextest9.co.uk/lightstreamer/'

# IFX Poland operator ID.
OPERATOR_IFX_POLAND = 'AC2347'


#
# Lightstreamer data adapter names.
#

# Item ID CLIENTACCOUNTMARGIN.ALL, TRADEMARGIN.All, ORDERS.ALL
ADAPTER_SET_ACCOUNT = 'STREAMINGCLIENTACCOUNT'

# Item ID "AC{operatorId}". Stream of default prices for the given operator.
ADAPTER_SET_DEFAULT = 'CITYINDEXSTREAMINGDEFAULTPRICES'

# Item ID NEWS.HEADLINES.{category}:
#   Stream of current news headlines. Try NEWS.HEADLINES.UK.
# Item ID PRICE.{marketIds}:
#   Stream of current prices. Try PRICES.PRICE.154297 (GBP/USD (per 0.0001)
#   CFD) which prices Mon - Fri 24hrs.
ADAPTER_SET_STREAMING = 'CITYINDEXSTREAMING'

# Item ID = QUOTE.ALL; Stream of quotes.
ADAPTER_SET_TRADING = 'STREAMINGTRADINGACCOUNT'


# 
ADAPTER_PRICES = 'PRICES'



#
# Lightstreamer topic names.
#

TOPIC_CLIENT_MARGIN = 'CLIENTACCOUNTMARGIN.ALL'
TOPIC_QUOTES = 'QUOTE.ALL'


#
# Lightstreamer field names.
#
from decimal import Decimal

def conv_dt(s):
    from datetime import datetime
    return datetime.utcfromtimestamp(float(_json_fixup('"%s"' % s)))


PRICE_TYPE_MAP = (
    ('MarketId', int),
    ('TickDate', conv_dt),
    ('Bid', Decimal),
    ('Offer', Decimal),
    ('Price', Decimal),
    ('High', Decimal),
    ('Low', Decimal),
    ('Change', Decimal),
    ('Direction', int),
    ('AuditId', unicode),
    ('StatusSummary', int)
)

PRICE_FIELDS = tuple(p[0] for p in PRICE_TYPE_MAP)

ACCOUNT_MARGIN_FIELDS = '''
    Cash Margin MarginIndicator NetEquity OpenTradeEquity TradeableFunds
    PendingFunds TradingResouce TotalMarginRequirement CurrencyId CurrencyISO
'''.split()


# Order status values.
ORDER_STATUS_PENDING = 1
ORDER_STATUS_ACCEPTED = 2
ORDER_STATUS_OPEN = 3
ORDER_STATUS_CANCELLED = 4
ORDER_STATUS_REJECTED = 5
ORDER_STATUS_SUSPENDED = 6
ORDER_STATUS_YELLOW_CARD = 8
ORDER_STATUS_CLOSED = 9
ORDER_STATUS_RED_CARD = 10
ORDER_STATUS_TRIGGERED = 11

ORDER_STATUS_MAP = {
    ORDER_STATUS_PENDING: 'Pending',
    ORDER_STATUS_ACCEPTED: 'Accepted',
    ORDER_STATUS_OPEN: 'Open',
    ORDER_STATUS_CANCELLED: 'Cancelled',
    ORDER_STATUS_REJECTED: 'Rejected',
    ORDER_STATUS_SUSPENDED: 'Suspended',
    ORDER_STATUS_YELLOW_CARD: 'Yellow Card',
    ORDER_STATUS_CLOSED: 'Closed',
    ORDER_STATUS_RED_CARD: 'Red Card',
    ORDER_STATUS_TRIGGERED: 'Triggered'
}






# Bizarre format is "/Date(UNIXTIMEINMS[-OFFSET])/" where [-OFFSET] is
# optional.
MS_DATE_RE = '["\']\\\\/Date\\(([0-9]+?(?:-[^)]+)?)\\)\\\\/["\']'

def _json_fixup(s):
    """Replace crappy Microsoft JSON date with a Javascript-ish
    milliseconds-since-epoch. "\/Date(1343067900000)\/" becomes 1343067900.0.
    """
    repl = lambda match: str(float(match.group(1)) / 1000)
    return re.sub(MS_DATE_RE, repl, s)



class CiApiClient:
    JSON_TYPE = 'application/json; charset=utf-8'

    def __init__(self, url, username, password):
        self.url = url
        self.username = username
        self.password = password
        self.log = logging.getLogger('CiApiClient')
        self.session_id = None
        self._client_account_id = None

    def _request(self, path):
        req = urllib2.Request(urlparse.urljoin(self.url, path))
        if self.session_id:
            req.add_header('UserName', self.username)
            req.add_header('session', self.session_id)
        return req

    def _open_raise(self, req):
        self.log.debug('%s %s (dlen=%d)', req.get_method(), req.get_full_url(),
            len(req.get_data() or ''))
        try:
            fp = urllib2.urlopen(req)
        except urllib2.HTTPError, e:
            raise ValueError('%d: %s' %\
                (e.getcode(), e.read()))

        raw = fp.read()
        try:
            return json.loads(_json_fixup(raw))
        except ValueError, e:
            raise ValueError('%r (%r)' % (e, raw))

    def _post(self, path, dct):
        req = self._request(path)
        req.add_header('Content-Type', self.JSON_TYPE)
        req.add_data(json.dumps(dct))
        return self._open_raise(req)

    def _get(self, path, dct=None):
        if dct:
            path += '?' + urllib.urlencode(dct)
        req = self._request(path)
        return self._open_raise(req)

    @property
    def client_account_id(self):
        if not self._client_account_id:
            dct = self._get('useraccount/UserAccount/ClientAndTradingAccount')
            self._client_account_id = dct['ClientAccountId']
        return self._client_account_id

    def login(self):
        dct = self._post('session', {
            'UserName': self.username,
            'Password': self.password
        })
        self.session_id = dct['Session']

    def market_search(self, q):
        return self._get('market/search', {
            'clientAccountId': self.client_account_id,
            'searchByMarketCode': 'true',
            'searchByMarketName': 'true',
            'spreadProductType': 'false',
            'cfdProductType': 'true',
            'binaryProductType': 'false',
            'query': 'Wall Street',
            'maxResults': '1000',
            'useMobileShortName': 'false'
        })

    def tag_lookup(self):
        return self._get('market/taglookup')['Tags']

    def search_with_tags(self, query='', tag_id=None, maxResults=1000):
        dct = {
            'maxResults': maxResults
        }
        if query:
            dct['query'] = query
        if tag_id:
            dct['tagId'] = tag_id
        return self._get('market/searchwithtags', dct)['Markets']

    def market_info(self, market_id):
        return self._get('market/%s/information' % market_id)

    def market_bars(self, market_id, interval='MINUTE', span=10, bars=75):
        out = self._get('market/%s/barhistory' % market_id, {
            'interval': interval,
            'span': span,
            'PriceBars': bars
        })
        return out

    def headlines(self):
        return self._get('news/headlines')

    def headlines_with_source(self, source, category, max_results=50):
        return self._get('news/%s/%s' % (source, category), {
            'MaxResults': max_results
        })

    def list_cfd_markets(self, name=None, code=None, max_results=200):
        return self._get('cfd/markets', {
            'MarketName': name or '',
            'MarketCode': code or '',
            'ClientAccountId': self.client_account_id,
            'MaxResults': max_results
        })['Markets']



class RowFactory(object):
    def __init__(self, field_map):
        self.field_map = field_map

    def __call__(self, row):
        return dict((k, conv(row[i]))
                    for i, (k, conv) in enumerate(self.field_map))


price_factory = RowFactory(PRICE_TYPE_MAP)




class CiStreamingClient(object):
    def __init__(self, base_url, api):
        self.base_url = base_url
        self.api = api
        self.log = logging.getLogger('CiStreamingClient')
        self._streaming_client = None
        self._trading_client = None
        self._default_client = None

        # CityIndex market ID -> Lightstreamer table ID.
        self._market_table_map = {}
        # CityIndex market ID -> set([listening, funcs])
        self._market_func_map = {}
        # set([listening, funcs])
        self._account_margin_listeners = set()
        self._trade_margin_listeners = set()
        self._default_listeners = set()
        self._orders_listeners = set()
        self._quotes_listeners = set()

    def _make_client(self, adapter_set):
        """Create an LsClient instance connected to the given `adapter_set."""
        streamer = lightstreamer.LsClient(self.base_url, content_length=1<<20)
        streamer.create_session(self.api.username, adapter_set=adapter_set,
            password=self.api.session_id)
        return streamer

    def listen_default(self, func, operator_id):
        """Listen to the stream of default prices for the given operator ID.
        `func` receives a Price instance each time the price updates."""
        if not self._default_client:
            self._default_client = self._make_client(ADAPTER_SET_DEFAULT)

        self._default_table = self._default_client.table(
            data_adapter=ADAPTER_PRICES,
            item_ids='AC%s' % operator_id,
            mode=lightstreamer.MODE_MERGE,
            schema=' '.join(PRICE_FIELDS)
        )

    def _make_listen_op(self, ops, market_id, func):
        table_id = self._market_table_map.get(market_id)
        if table_id:
            self._market_func_map[market_id].add(func)
            return

        table_id = self._streaming_client.make_table()
        self._market_table_map[market_id] = table_id
        self._market_func_map[market_id] = set([func])
        ops.append(lightstreamer.make_add(table_id,
            data_adapter=ADAPTER_PRICES,
            id_='PRICE.%d' % market_id,
            mode=lightstreamer.MODE_MERGE,
            schema=' '.join(PRICE_FIELDS)
        ))

    def listen_prices(self, func, market_ids):
        """Listen for prices for the given list of market IDs."""
        if not self._streaming_client:
            self._streaming_client = self._make_client(ADAPTER_SET_STREAMING)

        ops = []
        for market_id in market_ids:
            self._make_listen_op(ops, market_id, func)
        if ops:
            self.log.debug('Sending %d table create messages.', len(ops))
            self._streaming_client.send_control(ops)

    def _make_unlisten_op(self, ops, market_id, func):
        table_id = self._market_table_map.get(market_id)
        if not table_id:
            self.log.warning('%r was not subscribed to market %d.',
                func, market_id)
            return

        funcs = self._market_func_map[market_id]
        try:
            funcs.remove(func)
        except KeyError:
            self.log.warning('%r was not subscribed to market %d.',
                func, market_id)
        if not funcs:
            ops.append(lightstreamer.make_delete(table_id))
            del self._market_func_map[market_id]
            del self._market_table_map[market_id]

    def unlisten_prices(self, func, market_ids):
        """Stop listening for prices for the given list of market IDs."""
        ops = []
        for market_id in market_ids:
            self._make_unlisten_op(ops, market_id, func)
        if ops:
            self.log.debug('Sending %d table delete messages.', len(ops))
            self._streaming_client.send_control(ops)



def api_test():
    if len(sys.argv) != 3:
        print 'Usage: ./cityindex.py USERNAME PASSWORD'
        raise SystemExit(1)

    username = sys.argv[1]
    password = sys.argv[2]

    client = CiApiClient(TEST_API_URL, username, password)
    client.login()

    '''
    from pprint import pprint
    chans = ['PRICE.99498', 'PRICE.99502']

    tags = client.tag_lookup()
    ids = {}

    for tag in tags:
        if not tag['Children']:
            ids[tag['MarketTagId']] = tag['Name']
        else:
            for ctag in tag['Children']:
                ids[ctag['MarketTagId']] = ctag['Name']

    streamer = lightstreamer.LsClient(TEST_STREAM_URL)
    listener = DefaultPricesListener(PRICE_FIELDS)

    table_id2 = streamer.make_table(listener)
    streamer.send_control([
        lightstreamer.make_add(table_id,
            data_adapter='PRICES',
            id_=' '.join(chans),
            mode=lightstreamer.MODE_MERGE,
            schema=' '.join(PRICE_FIELDS)),
    ])
    '''

    market_ids = [m['MarketId'] for m in client.list_cfd_markets()]

    streamer = CiStreamingClient(TEST_STREAM_URL, client)
    def on_connection_state(state):
        if state == lightstreamer.STATE_DISCONNECTED:
            reconnect()
    def on_update(table_id, item_id, row):
        self.log.debug('on_update(%r, %r) -> %r',
            table_id, item_id, price_factory(row))
    def on_update(table_id, item_id, row):
        print 'printo!', args
    streamer.listen_prices(printo, [154297, 99498, 99502] + market_ids)
    import signal
    while signal.pause():
        pass


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    api_test()
