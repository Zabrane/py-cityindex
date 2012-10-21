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

import threading
import time


class LeakyBucket(object):
    """'Leaky bucket' rate limiting discipline."""
    def __init__(self, per_sec, capacity, initial=None):
        """Create an instance. Allow no more than `per_sec` tokens per second
        or `capacity` tokens in the bucket."""
        self._per_sec = per_sec
        self._capacity = capacity
        self._tokens = initial or capacity
        self._lock = threading.Lock()
        self._last_fill = time.time()

    def _fill(self):
        """Fill the bucket up to its capacity with as many tokens as were
        produced since the last fill."""
        now = time.time()
        tokens = self._tokens + ((now - self._last_fill) * self._per_sec)
        self._tokens = min(self._capacity or tokens, tokens)
        self._last_fill = now

    def get(self):
        """Block the calling thread until a token is available."""
        with self._lock:
            self._fill()
            if self._tokens < 1:
                time.sleep((1 - self._tokens) / float(self._per_sec))
                self._fill()
            self._tokens -= 1


class cached_property(object):
    """Method decorator that exposes a property that caches the functions
    return value on first call."""
    _default_value = object()

    def __init__(self, func, name=None, doc=None):
        self.__name__ = name or func.__name__
        self.__module__ = func.__module__
        self.__doc__ = doc or func.__doc__
        self.func = func

    def __get__(self, obj, type=None):
        if obj is None:
            return self

        value = obj.__dict__.get(self.__name__, self._default_value)
        if value is self._default_value:
            value = self.func(obj)
            obj.__dict__[self.__name__] = value

        return value
