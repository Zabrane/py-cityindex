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
OPERATOR_IFX_POLAND = 'AC2347'


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
DA_PRICES = 'PRICES'                            # AS_STREAMING, AS_DEFAULT
DA_QUOTE = 'QUOTE'                              # AS_TRADING
DA_TRADE_MARGIN = 'TRADEMARGIN'                 # AS_ACCOUNT



def conv_dt(s):
    from datetime import datetime
    return datetime.utcfromtimestamp(float(_json_fixup('"%s"' % s)))


#
# Lightstreamer topic names.
#

TOPIC_CLIENT_MARGIN = 'CLIENTACCOUNTMARGIN.ALL'


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



PRICE_FIELDS = tuple(p[0] for p in PRICE_FIELD_TYPES)
TRADE_MARGIN_FIELDS = tuple(p[0] for p in TRADE_MARGIN_FIELD_TYPES)
ACCOUNT_MARGIN_FIELDS = tuple(p[0] for p in ACCOUNT_MARGIN_FIELD_TYPES)



def make_row_factory(field_types):
    def row_factory(row):
        return dict((key, conv(row[i]))
                    for i, (key, conv) in enumerate(field_types))



class TableManager(object):
    def __init__(self, client):
        self.client = client
        self.table_map = {}
        self.func_set = set()

    def key_func(self, key):
        return key

    def listen(self, func, key=None):
        key = self.key_func(key)
        if key not in self.table_map:
            self.table_map[key] = self.table_factory(key)
        self.func_set.add(func)

    def unlisten(self, key, func):
        if key in self.table_map:
            self.func_set.discard(func)
            if not self.func_set:
                table = self.table_map.pop(key)
                table.delete()


class PriceTableManager(TableManager):
    def table_factory(self, market_id):
        return lightstreamer.Table(self.client,
            data_adapter=DA_PRICES,
            item_ids='PRICE.%d' % market_id,
            mode=lightstreamer.MODE_MERGE,
            schema=' '.join(PRICE_FIELDS),
            row_factory=make_row_factory(PRICE_FIELD_TYPES)
        )


class DefaultTableManager(TableManager):
    def table_factory(self, operator_id):
        return lightstreamer.Table(self.client,
            data_adapter=ADAPTER_PRICES,
            item_ids='AC%s' % operator_id,
            mode=lightstreamer.MODE_MERGE,
            schema=' '.join(PRICE_FIELDS)
        )


class TradeMarginTableManager(TableManager):
    def key_func(self, key):
        """Map any provided key to None; there is only one channel.
        """

    def table_factory(self, key):
        return lightstreamer.Table(self.client,
            data_adapter=AS_ACCOUNT,
            item_ids='TRADEMARGIN.ALL',
            mode=lightstreamer.MODE_MERGE,
            schema=' '.join(TRADE_MARGIN_FIELDS))


class QuoteTableManager(TableManager):
    def key_func(self, key):
        """Map any provided key to None; there is only one channel.
        """

    def table_factory(self, key):
        return lightstreamer.Table(self.client,
            data_adapter=DA_TRADING,
            item_ids='QUOTE.ALL',
            mode=lightstreamer.MODE_MERGE,
            schema=' '.join(QUOTE_FIELDS))


class CiStreamingClient(object):
    def __init__(self, base_url, api):
        self.base_url = base_url
        self.api = api
        self.log = logging.getLogger('CiStreamingClient')
        self._client_map = {}
        self._stopped = False

    def _get_client(self, adapter_set):
        """Create an LsClient instance connected to the given `adapter_set."""
        client = self._client_map.get(adapter_set)
        if not client:
            client = lightstreamer.LsClient(self.base_url, content_length=1<<20)
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
    def trade_margin(self):
        return TradeMarginTableManager(self._get_client('ZErp'))
        client = self._make

    @util.cached_property
    def default(self):
        """Listen to the stream of default prices for the given operator ID.
        `func` receives a Price instance each time the price updates."""
        return DefaultTableManager(self._get_client(AS_DEFAULT))

    @util.cached_property
    def prices(self):
        """Listen to prices for the given list of market IDs."""
        return PriceTableManager(self._get_client(AS_STREAMING))

    @util.cached_property
    def quotes(self):
        """Listen to quote updates for the user's account."""
        return QuoteTableManager(self._geT_client(AS_TRADING))
