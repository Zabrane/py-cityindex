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

import json
import logging
import socket
import urllib
import urllib2
import urlparse

from cityindex import util


# Production.
LIVE_API_URL = 'http://ciapi.cityindex.com/tradingapi/'
TEST_API_URL = 'http://ciapipreprod.cityindextest9.co.uk/tradingapi/'
REQS_PER_SEC = 10


#
# Lightstreamer field names.
#

def url_bool(b):
    return 'true' if b else 'false'


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


class CiApiClient:
    JSON_TYPE = 'application/json; charset=utf-8'

    def __init__(self, username, password, url=None, prod=True):
        self.username = username
        self.password = password
        self.url = url or (LIVE_API_URL if prod else TEST_API_URL)
        self.log = logging.getLogger('CiApiClient')
        self.session_id = None
        self._client_account_id = None
        # Docs say no more than 50reqs/5sec.
        self._bucket = util.LeakyBucket(REQS_PER_SEC, REQS_PER_SEC)
        self._resolve_host()

    def _resolve_host(self):
        parsed = list(urlparse.urlparse(self.url))
        self._original_host = parsed[1]
        parsed[1] = socket.gethostbyname(parsed[1])
        self.log.debug('Resolved %r to %r', self._original_host, parsed[1])
        self.url = urlparse.urlunparse(tuple(parsed))

    def _request(self, path):
        self._bucket.get()
        req = urllib2.Request(urlparse.urljoin(self.url, path), headers={
            'Host': self._original_host
        })
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
            return json.loads(util.json_fixup(raw))
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

    @util.cached_property
    def account_information(self):
        return self._get('useraccount/UserAccount/ClientAndTradingAccount')

    @property
    def client_account_id(self):
        return self.account_information['ClientAccountId']

    @property
    def trading_accounts(self):
        return self.account_information['TradingAccounts']

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
        tags = []
        for tag in self._get('market/taglookup')['Tags']:
            tags.append(tag)
            tag['ParentTagId'] = None
            tag.pop('Type')
            for child in tag.pop('Children'):
                child['ParentTagId'] = tag['MarketTagId']
                child.pop('Type')
                tags.append(child)
        return tags


    def search_with_tags(self, query='', tag_id=None, maxResults=1000,
            spread=True, cfd=True, binary=False):
        dct = {
            'maxResults': maxResults,
            'cfdProductType': url_bool(cfd),
            'binaryProductType': url_bool(binary),
            'spreadProductType': url_bool(spread)
        }
        if query:
            dct['query'] = query
        if tag_id:
            dct['tagId'] = tag_id
        return self._get('market/searchwithtags', dct)['Markets']

    def market_info(self, market_id):
        dct = self._get('market/%s/information' % market_id)
        return dct['MarketInformation']

    def market_search(self, query, by_code=False, by_name=False, spread=False,
            cfd=False, binary=False, options=False, max_results=200,
            mobile=False):
        dct = self._get('market/informationsearch', {
            'searchByMarketCode': url_bool(by_code),
            'searchByMarketName': url_bool(by_name),
            'spreadProductType': url_bool(spread),
            'cfdProductType': url_bool(cfd),
            'binaryProductType': url_bool(binary),
            'IncludeOptions': url_bool(options),
            'query': query,
            'maxResults': max_results,
            'useMobileShortName': url_bool(mobile)
        })
        return dct['MarketInformation']

    def market_bars(self, market_id, interval='MINUTE', span=1, bars=60):
        return self._get('market/%s/barhistory' % market_id, {
            'interval': interval,
            'span': span,
            'PriceBars': bars
        })

    def market_ticks(self, market_id, ticks=1000):
        return self._get('market/%s/tickhistory' % market_id, {
            'PriceTicks': ticks
        })

    def headlines(self, source=None, category=None, max_results=50,
            culture_id=None):
        return self._post('news/headlines', {
            'Source': source or '',
            'Category': category or 'uk',
            'MaxResults': max_results,
            'CultureId': culture_id
        })

    def news_detail(self, source, story_id):
        return self._get('news/%s/%s' % (source, story_id))

    def list_cfd_markets(self, name=None, code=None, max_results=200):
        return self._get('cfd/markets', {
            'MarketName': name or '',
            'MarketCode': code or '',
            'ClientAccountId': self.client_account_id,
            'MaxResults': max_results
        })['Markets']

    def list_spread_markets(self, name=None, code=None, max_results=200):
        return self._get('spread/markets', {
            'MarketName': name or '',
            'MarketCode': code or '',
            'ClientAccountId': self.client_account_id,
            'MaxResults': max_results
        })['Markets']

    def get_system_lookup(self, entity):
        return self._get('message/lookup', {
            'LookupEntityName': entity
        })

    def list_trade_history(self, account_id, max_results=200):
        return self._get('order/tradehistory', {
            'TradingAccountId': account_id,
            'maxResults': max_results
        })
