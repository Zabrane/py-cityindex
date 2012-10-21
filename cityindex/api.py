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
import re
import urllib
import urllib2
import urlparse

from cityindex import util


# Production.
LIVE_API_URL = 'https://ciapi.cityindex.com/tradingapi/'
TEST_API_URL = 'https://ciapipreprod.cityindextest9.co.uk/tradingapi/'


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
        # Docs say no more than 50reqs/5sec.
        self._bucket = util.LeakyBucket(10, 10)

    def _request(self, path):
        self._bucket.get()
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
