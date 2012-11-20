#
# Copyright 2012, the py-cityindex authors
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import absolute_import

import logging
import threading

from cityindex import util
import lightstreamer


LIVE_STREAM_URL = 'https://push.cityindex.com/lightstreamer/'
TEST_STREAM_URL = 'https://pushpreprod.cityindextest9.co.uk/lightstreamer/'

# IFX Poland operator ID.
OPERATOR_IFX_POLAND = 2347


#
# Adapter sets.
#

# CLIENTACCOUNTMARGIN.ALL, TRADEMARGIN.All, ORDERS.ALL
AS_ACCOUNT = 'STREAMINGCLIENTACCOUNT'

# AC{operatorId}
AS_DEFAULT = 'CITYINDEXSTREAMINGDEFAULTPRICES'

# PRICE.{marketId}, NEWS.HEADLINES.{category}
AS_STREAMING = 'CITYINDEXSTREAMING'

# QUOTE.ALL
AS_TRADING = 'STREAMINGTRADINGACCOUNT'


def conv_bool(s):
    return s == 'true'


def conv_dt(s):
    return float(util.json_fixup('"%s"' % s))


# ClientAccountMarginDTO
MARGIN_FIELDS = (
    ('Cash', float),
    ('Margin', float),
    ('MarginIndicator', float),
    ('NetEquity', float),
    ('OpenTradeEquity', float),
    ('TradeableFunds', float),
    ('PendingFunds', float),
    ('TradingResource', float),
    ('TotalMarginRequirement', float),
    ('CurrencyId', int),
    ('CurrencyISO', unicode)
)


# PriceDTO
PRICE_FIELDS = (
    ('MarketId', int),
    ('Name', unicode),
    ('TickDate', conv_dt),
    ('Bid', float),
    ('Offer', float),
    ('Price', float),
    ('High', float),
    ('Low', float),
    ('Change', float),
    ('Direction', int),
    ('AuditId', unicode),
    ('StatusSummary', int)
)


# NewsDTO
NEWS_FIELDS = (
    ('StoryId', int),
    ('Headline', unicode),
    ('PublishDate', conv_dt)
)


# OrderDTO
ORDER_FIELDS = (
    ('OrderId', int),
    ('Name', unicode),
    ('MarketId', int),
    ('ClientAccountId', int),
    ('TradingAccountId', int),
    ('CurrencyId', int),
    ('CurrencyISO', unicode),
    ('Direction', int),
    ('AutoRollover', conv_bool),
    ('LastChangedTime', conv_dt),
    ('OpenPrice', float),
    ('OriginalLastChangedDateTime', conv_dt),
    ('OriginalQuantity', float),
    ('PositionMethodId', int),
    ('Quantity', float),
    ('Type', unicode),
    ('Status', unicode),
    ('ReasonId', int)
)


# QuoteDTO
QUOTE_FIELDS = (
    ('QuoteId', int),
    ('OrderId', int),
    ('MarketId', int),
    ('BidPrice', float),
    ('BidAdjust', float),
    ('OfferPrice', float),
    ('OfferAdjust', float),
    ('Quantity', float),
    ('CurrencyId', int),
    ('StatusId', int),
    ('TypeId', int),
    ('RequestDateTimeUTC', conv_dt),
    ('ApprovalDateTimeUTC', conv_dt),
    ('BreathTimeSecs', int),
    ('IsOversize', conv_bool),
    ('ReasonId', int),
    ('TradingAccountId', int)
)


# TradeMarginDTO
TRADE_MARGIN_FIELDS = (
    ('ClientAccountId', int),
    ('DirectionId', int),
    ('MarginRequirementConverted', float),
    ('MarginRequirementConvertedCurrencyId', int),
    ('MarginRequirementConvertedCurrencyISOCode', unicode),
    ('MarketId', int),
    ('MarketTypeId', int),
    ('Multiplier', float),
    ('OrderId', int),
    ('OTEConverted', float),
    ('OTEConvertedCurrencyId', int),
    ('OTEConvertedCurrencyISOCode', unicode),
    ('PriceCalculatedAt', float),
    ('PriceTakenAt', float),
    ('Quantity', float)
)


def make_row_factory(fields):
    """Return a function that, when passed a sequence of strings-or-None,
    passes each value through a converter function if it is non-None, and uses
    the result to form a dict.

    Example:
        >>> func = make_row_factory([
        ...     ('FieldA', float),
        ...     ('FieldB', unicode),
        ...     ('FieldC', int)
        ... ])
        >>> print func(['1234', 'test', None])
        {'FieldA': 1234.0, 'FieldB': u'test', 'FieldC': None}
    """
    def row_factory(row):
        return dict((key, conv(row[i]) if row[i] is not None else None)
                    for i, (key, conv) in enumerate(fields))
    return row_factory


class TableManager(object):
    """Manage a set of tables for a Lightstreamer client. Use table_factory()
    when the first caller requests a table, and later destroy it when the last
    caller unsubscribes."""
    def __init__(self, table_factory, ids_func=None):
        """Create an instance that uses `table_factory` to construct tables,
        and optionally using `ids_func` to map item_ids into a normal form."""
        self.table_factory = table_factory
        self.ids_func = ids_func or (lambda o: o)
        self.table_map = {}
        self.func_map = {}
        self._lock = threading.Lock()

    def _make_ids(self, ids):
        if not isinstance(ids, (list, tuple)):
            ids = [ids]
        return ' '.join(self.ids_func(id_) for id_ in ids)

    def listen(self, func, item_ids=None):
        """Subscribe `func` to updates for `item_ids``."""
        item_ids = self._make_ids(item_ids)
        with self._lock:
            if item_ids not in self.table_map:
                self.table_map[item_ids] = self.table_factory(item_ids)
                self.table_map[item_ids].on_update(
                    lambda item_id, row: self._on_update(item_ids, row))
            self.func_map.setdefault(item_ids, set()).add(func)

    def unlisten(self, func, item_ids=None):
        """Unsubscribe `func` from updates for `item_ids`, destroying the
        Lightstreamer subscription if it was the last interested function."""
        item_ids = self._make_ids(item_ids)
        with self._lock:
            if item_ids in self.table_map:
                self.func_map[item_ids].discard(func)
                self._maybe_delete(item_ids)

    def _maybe_delete(self, item_ids):
        if not self.func_map[item_ids]:
            table = self.table_map.pop(item_ids)
            table.delete()
            self.func_map.pop(item_ids)

    def _on_update(self, item_ids, row):
        """Invoked when any table has changed; forward the changed row to
        subscribed functions."""
        lightstreamer.dispatch(self.func_map[item_ids], row)
        with self._lock:
            self._maybe_delete(item_ids)


class CiStreamingClient(object):
    """CityIndex streaming data client.

    This wraps py-lightstreamer to provide a gently normalized view of
    CityIndex's streaming data sources. For each available source, a property
    exists that yields a TableManager instance. All that is required to
    subscribe to some data is to call listen() on this returned instance.

    Unless otherwise specified (for prices and news), call listen() with no
    key= parameter.

    Supported tables:
        account_margin:
            Client account margin updates.

        trade_margin:
            Per-trade margin updates.

        default:
            Default prices listener. The key= parameter to listen() selects the
            operator ID to subscribe to, for example
            cityindex.OPERATOR_IDX_POLAND.

        orders:
            Order status updates.

        prices:
            Market pricing updates. The key= parameter to listen() selects the
            MarketId to subscribe to, for example 99500 is UK 100. Refer to
            CiApiClient search methods to retrieve market IDs.

        quotes:
            Oversized order quotation updates.

        news:
            News headlines. The key= parameter to listen() selects the news
            category to subscribe to. Refer to the CIAPI documentation for a
            list.

    Example:
        api = CiClientApi('DM12345678', 'password')
        api.login()
        streamer = CiStreamingApi(api)

        def on_uk100_change(price):
            print 'UK 100:', price['Price']
        streamer.prices.listen(on_uk100_change, 99500)

        def on_news(headline):
            print 'UK NEWS:', news['Headline']
        streamer.news.listen(on_news, 'UK')

        # Get bored, so unsubscribe.
        streamer.news.unlisten(on_news, 'UK')

        def on_order_changed(order):
            print 'ORDER:', order
        streamer.orders.listen(on_order_changed)

        # Do useful work, decide to shutdown.
        streamer.stop()
    """
    def __init__(self, api, url=None, prod=True):
        """Create an instance using the API session from CiApiClient instance
        `api`."""
        self.api = api
        self.url = url or (LIVE_STREAM_URL if prod else TEST_STREAM_URL)
        self.log = logging.getLogger('CiStreamingClient')
        self._client_map = {}
        self._client_map_lock = threading.Lock()
        self._state_map = dict.fromkeys(
            (AS_ACCOUNT, AS_DEFAULT, AS_STREAMING, AS_TRADING),
            lightstreamer.STATE_DISCONNECTED)
        self._state_funcs = []
        self._stopped = False

    def stop(self, join=True):
        """Shut down the streaming client. If `join` is True, don't return
        until all connections are closed."""
        self._stopped = True
        for client in self._client_map.itervalues():
            client.destroy()
        if join:
            for client in self._client_map.itervalues():
                client.join()

    def on_state(self, func):
        """Subscribe `func` to be invoked as `func(dct)` every time a
        Lightstreamer client changes state, where dct is a dictionary whose
        keys are cityindex.streaming.AS_* constants and values are
        lightstreamer.STATE_* constants."""
        self._state_funcs.append(func)

    def last_price(self, market_id):
        ev = threading.Event()
        prices = []
        def listener(price):
            prices.append(price)
            ev.set()
        self.prices.listen(listener, market_id)
        ev.wait()
        self.prices.unlisten(listener, market_id)
        return prices[-1]

    def _create_session(self, client, adapter_set):
        client.create_session(self.api.username, adapter_set=adapter_set,
            password=self.api.session_id, keepalive_ms=1000)

    def _on_client_state(self, client, adapter_set, state):
        self._state_map[adapter_set] = state
        lightstreamer.dispatch(self._state_funcs, self._state_map)
        if state == lightstreamer.STATE_DISCONNECTED and not self._stopped:
            # TODO: sleep
            self.log('adapter %r lost connection; reconnecting', adapter_set)
            self._create_session(client, adapter_set)

    def _get_client(self, adapter_set):
        """Create an LsClient instance connected to the given `adapter_set."""
        with self._client_map_lock:
            client = self._client_map.get(adapter_set)
            if not client:
                client = lightstreamer.LsClient(self.url, content_length=1<<20)
                client.on_state(lambda state: \
                    self._on_client_state(client, adapter_set, state))
                self._create_session(client, adapter_set)
                self._client_map[adapter_set] = client
        return client

    def _make_table_factory(self, adapter_set, data_adapter, fields,
            snapshot=False):
        """Return a function that when passed an item_ids string, returns a
        lightstreamer.Table instance for `client` subscribed to those IDs from
        `data_adapter`, with a row factory coresponding to `fields`"""
        client = self._get_client(adapter_set)
        schema = ' '.join(p[0] for p in fields)
        row_factory = make_row_factory(fields)

        def table_factory(item_ids):
            return lightstreamer.Table(
                client,
                data_adapter=data_adapter,
                item_ids=item_ids,
                max_frequency='unfiltered',
                mode=lightstreamer.MODE_MERGE,
                schema=schema,
                snapshot=snapshot,
                row_factory=row_factory
            )
        return table_factory

    @util.cached_property
    def account_margin(self):
        """Listen to client account margin updates."""
        factory = self._make_table_factory(adapter_set=AS_ACCOUNT,
            data_adapter='CLIENTACCOUNTMARGIN', fields=MARGIN_FIELDS)
        return TableManager(factory, ids_func=lambda key: 'ALL')

    @util.cached_property
    def trade_margin(self):
        """Listen to per-trade margin updates."""
        factory = self._make_table_factory(adapter_set=AS_ACCOUNT,
            data_adapter='TRADEMARGIN', fields=TRADE_MARGIN_FIELDS)
        return TableManager(factory, ids_func=lambda key: 'ALL')

    @util.cached_property
    def default(self):
        """Listen to the stream of default prices for some operator ID."""
        factory = self._make_table_factory(adapter_set=AS_DEFAULT,
            data_adapter='PRICES', fields=PRICE_FIELDS, snapshot=True)
        return TableManager(factory, lambda operator_id: 'AC%d' % operator_id)

    @util.cached_property
    def orders(self):
        """Listen to order status updates."""
        factory = self._make_table_factory(adapter_set=AS_ACCOUNT,
            data_adapter='ORDERS', fields=ORDER_FIELDS)
        return TableManager(factory, ids_func=lambda key: 'ORDERS')

    @util.cached_property
    def prices(self):
        """Listen to prices for some market ID."""
        factory = self._make_table_factory(adapter_set=AS_STREAMING,
            data_adapter='PRICES', fields=PRICE_FIELDS, snapshot=True)
        return TableManager(factory, ids_func=lambda key: 'PRICE.%s' % key)

    @util.cached_property
    def quotes(self):
        """Listen to oversize order quote updates."""
        factory = self._make_table_factory(adapter_set=AS_TRADING,
            data_adapter='QUOTE', fields=QUOTE_FIELDS)
        return TableManager(factory, ids_func=lambda key: 'ALL')

    @util.cached_property
    def news(self):
        """Listen to news headlines for some category."""
        factory = self._make_table_factory(adapter_set=AS_STREAMING,
            data_adapter='NEWS', fields=NEWS_FIELDS)
        return TableManager(factory, ids_func=lambda key: 'HEADLINES.%s' % key)
