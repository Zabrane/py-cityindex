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


#
# Data adapters.
#

DA_CLIENT_MARGIN = 'CLIENTACCOUNTMARGIN'        # AS_ACCOUNT
DA_NEWS = 'NEWS'                                # AS_STREAMING
DA_ORDERS = 'ORDERS'                            # AS_ACCOUNT
DA_QUOTE = 'QUOTE'                              # AS_TRADING



def conv_bool(s):
    return s == 'true'


def conv_dt(s):
    from datetime import datetime
    return datetime.utcfromtimestamp(float(util.json_fixup('"%s"' % s)))


# ClientAccountMarginDTO
ACCOUNT_MARGIN_FIELD_TYPES = (
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
PRICE_FIELD_TYPES = (
    ('MarketId', int),
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
NEWS_FIELD_TYPES = (
    ('StoryId', int),
    ('Headline', unicode),
    ('PublishDate', conv_dt)
)


# OrderDTO
ORDERS_FIELD_TYPES = (
    ('OrderID', int),
    ('MarketID', int),
    ('ClientAccountID', int),
    ('TradingAccountID', int),
    ('CurrencyID', int),
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
QUOTE_FIELD_TYPES = (
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
TRADE_MARGIN_FIELD_TYPES = (
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


def schema_for_type(types):
    """Return the Lightstreamer table schema corresponding to a list of field
    types."""
    return ' '.join(p[0] for p in types)


def make_row_factory(field_types):
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
                    for i, (key, conv) in enumerate(field_types))
    return row_factory


class TableManager(object):
    """Manage a set of tables for a Lightstreamer client. Use table_factory()
    when the first caller requests a table, and later destroy it when the last
    caller unsubscribes."""
    def __init__(self, client):
        """Create an instance associated with the LsClient `client`."""
        self.client = client
        self.table_map = {}
        self.func_map = {}

    def key_func(self, key):
        """Map a key as necessary; default implementation simply returns the
        original key."""
        return key

    def listen(self, func, key=None):
        """Subscribe `func` to updates for `key``."""
        key = self.key_func(key)
        if key not in self.table_map:
            self.table_map[key] = self.table_factory(key)
            self.table_map[key].on_update(
                lambda item_id, row: self._on_update(key, row))
        self.func_map.setdefault(key, set()).add(func)

    def unlisten(self, func, key=None):
        """Unsubscribe `func` from updates for `key`, destroying the
        Lightstreamer subscription if it was the last interested function."""
        if key in self.table_map:
            self.func_map[key].discard(func)
            if not self.func_map[key]:
                table = self.table_map.pop(key)
                table.delete()

    def _on_update(self, key, row):
        """Invoked when any table has changed; forward the changed row to
        subscribed functions."""
        lightstreamer.dispatch(self.func_map[key], row)


class AccountMarginTableManager(TableManager):
    def key_func(self, key):
        """"""

    def table_factory(self, key):
        return lightstreamer.Table(self.client,
            data_adapter=DA_CLIENT_MARGIN,
            item_ids='CLIENTACCOUNTMARGIN.ALL',
            mode=lightstreamer.MODE_MERGE,
            schema=schema_for_type(ACCOUNT_MARGIN_FIELD_TYPES),
            row_factory=make_row_factory(ACCOUNT_MARGIN_FIELD_TYPES)
        )


class PriceTableManager(TableManager):
    def table_factory(self, market_id):
        return lightstreamer.Table(self.client,
            data_adapter='PRICES',
            item_ids='PRICE.%d' % market_id,
            mode=lightstreamer.MODE_MERGE,
            schema=schema_for_type(PRICE_FIELD_TYPES),
            row_factory=make_row_factory(PRICE_FIELD_TYPES)
        )


class DefaultTableManager(TableManager):
    def table_factory(self, operator_id):
        return lightstreamer.Table(self.client,
            data_adapter='PRICES',
            item_ids='AC%s' % operator_id,
            mode=lightstreamer.MODE_MERGE,
            schema=schema_for_type(PRICE_FIELD_TYPES),
            row_factory=make_row_factory(PRICE_FIELD_TYPES)
        )


class TradeMarginTableManager(TableManager):
    def key_func(self, key):
        """Map any provided key to None; there is only one channel.
        """

    def table_factory(self, key):
        return lightstreamer.Table(self.client,
            data_adapter='TRADEMARGIN',
            item_ids='ALL',
            mode=lightstreamer.MODE_MERGE,
            schema=schema_for_type(TRADE_MARGIN_FIELD_TYPES),
            row_factory=make_row_factory(TRADE_MARGIN_FIELD_TYPES)
        )


class OrderTableManager(TableManager):
    def key_func(self, key):
        """Many any provided key to None; there is only one channel."""

    def table_factory(self, key):
        return lightstreamer.Table(self.client,
            data_adapter=DA_ORDERS,
            item_ids='ORDERS',
            mode=lightstreamer.MODE_MERGE,
            schema=schema_for_type(ORDERS_FIELD_TYPES),
            row_factory=make_row_factory(ORDERS_FIELD_TYPES)
        )


class QuoteTableManager(TableManager):
    def key_func(self, key):
        """Map any provided key to None; there is only one channel.
        """

    def table_factory(self, key):
        return lightstreamer.Table(self.client,
            data_adapter='QUOTE',
            item_ids='ALL',
            mode=lightstreamer.MODE_MERGE,
            schema=schema_for_type(QUOTE_FIELD_TYPES),
            row_factory=make_row_factory(QUOTE_FIELD_TYPES)
        )


class CiStreamingClient(object):
    def __init__(self, api, url=None, prod=True):
        self.api = api
        self.url = url or (LIVE_STREAM_URL if prod else TEST_STREAM_URL)
        self.log = logging.getLogger('CiStreamingClient')
        self._client_map = {}
        self._stopped = False

    def _get_client(self, adapter_set):
        """Create an LsClient instance connected to the given `adapter_set."""
        client = self._client_map.get(adapter_set)
        if not client:
            client = lightstreamer.LsClient(self.url, content_length=1<<20)
            client.create_session(self.api.username, adapter_set=adapter_set,
                password=self.api.session_id)
            self._client_map[adapter_set] = client
        return client

    def stop(self, join=True):
        """Shut down the streaming client. If `join` is True, don't return
        until all connections are closed."""
        self._stopped = True
        for client in self._client_map.itervalues():
            client.destroy()
        if join:
            for client in self._client_map.itervalues():
                client.join()

    @util.cached_property
    def account_margin(self):
        return AccountMarginTableManager(self._get_client(AS_ACCOUNT))

    @util.cached_property
    def trade_margin(self):
        return TradeMarginTableManager(self._get_client(AS_ACCOUNT))

    @util.cached_property
    def default(self):
        """Listen to the stream of default prices for the given operator ID.
        `func` receives a Price instance each time the price updates."""
        return DefaultTableManager(self._get_client(AS_DEFAULT))

    @util.cached_property
    def orders(self):
        """Listen to order status for the active user account."""
        return OrderTableManager(self._get_client(AS_ACCOUNT))

    @util.cached_property
    def prices(self):
        """Listen to prices for the given list of market IDs."""
        return PriceTableManager(self._get_client(AS_STREAMING))

    @util.cached_property
    def quotes(self):
        """Listen to quote updates for the user's account."""
        return QuoteTableManager(self._get_client(AS_TRADING))
