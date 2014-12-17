# Copyright 2009-2013 Yelp and Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Protocols deserialize and serialize the input and output of tasks to raw
bytes for Hadoop to distribute to the next task or to write as output. For more
information, see :ref:`job-protocols` and :ref:`writing-protocols`.
"""
# don't add imports here that aren't part of the standard Python library,
# since MRJobs need to run in Amazon's generic EMR environment
from six.moves import cPickle

import six

from mrjob.util import safeeval
from mrjob.portability import to_bytes, to_text, is_bytes, is_text

try:
    import simplejson as json  # preferred because of C speedups
    json  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    import json  # built in to Python 2.6 and later


class _KeyCachingProtocol(object):
    """Protocol that caches the last decoded key.

    We're not currently exposing this class; inheriting from this class
    will result in almost as much code as simply writing your own read/write
    methods. You should probably cache keys, but in a way that makes sense for
    your use case.
    """
    _last_key_encoded = None
    _last_key_decoded = None

    def _loads(self, value):
        """Decode a single key/value, and return it."""
        raise NotImplementedError

    def _dumps(self, value):
        """Encode a single key/value, and return it."""
        raise NotImplementedError

    def read(self, line):
        """Decode a line of input.

        :type line: bytes
        :param line: A line of raw input to the job, without trailing newline.

        :return: A tuple of ``(key, value)``."""

        assert(is_bytes(line))
        raw_key, raw_value = line.split(b'\t', 1)

        if raw_key != self._last_key_encoded:
            self._last_key_encoded = raw_key
            self._last_key_decoded = self._loads(raw_key)
        return (self._last_key_decoded, self._loads(raw_value))

    def write(self, key, value):
        """Encode a key and value.

        :param key: A key (of any type) yielded by a mapper/reducer
        :param value: A value (of any type) yielded by a mapper/reducer

        :rtype:  bytes
        :return: A line, without trailing newline."""
        return b'\t'.join((self._dumps(key), self._dumps(value)))


class JSONProtocol(_KeyCachingProtocol):
    """Encode ``(key, value)`` as two JSONs separated by a tab.

    Note that JSON has some limitations; dictionary keys must be strings,
    and there's no distinction between lists and tuples.
    """

    def _loads(self, value):
        assert(is_bytes(value))
        return json.loads(to_text(value))

    def _dumps(self, value):
        # mimic behavior of json dumps in PY2 for compatibility,
        # forbidding any bytes from not being valid utf-8
        if is_bytes(value) and not is_text(value):
            value.decode('utf-8')  # no need to capture result
        return to_bytes(json.dumps(value))


class JSONValueProtocol(object):
    """Encode ``value`` as a JSON and discard ``key``
    (``key`` is read in as ``None``).
    """
    def read(self, line):
        assert(is_bytes(line))
        return (None, json.loads(to_text(line)))

    def write(self, key, value):
        return to_bytes(json.dumps(value))


class PickleProtocol(_KeyCachingProtocol):
    """Encode ``(key, value)`` as two string-escaped pickles separated
    by a tab. In python 3, this is just a passthrough.

    We string-escape the pickles to avoid having to deal with stray
    ``\\t`` and ``\\n`` characters, which would confuse Hadoop
    Streaming.

    Ugly, but should work for any type.
    """

    def _loads(self, value):
        assert(is_bytes(value))
        if six.PY2:
            value = value.decode('string_escape')
        return cPickle.loads(value)

    def _dumps(self, value):
        d = cPickle.dumps(value)
        if six.PY2:
            d = d.encode('string_escape')
        assert(is_bytes(d))
        return d


class PickleValueProtocol(object):
    """Encode ``value`` as a string-escaped pickle and discard ``key``
    (``key`` is read in as ``None``).
    """
    def read(self, line):
        if six.PY2:
            line = line.decode('string_escape')
        return (None, cPickle.loads(line))

    def write(self, key, value):
        d = cPickle.dumps(value)
        if six.PY2:
            d = d.encode('string_escape')
        return d


# This was added in 0.3, so no @classmethod for backwards compatibility
class RawProtocol(object):
    """Encode ``(key, value)`` as ``key`` and ``value`` separated by
    a tab (``key`` and ``value`` should be bytes).

    If ``key`` or ``value`` is ``None``, don't include a tab. When decoding a
    line with no tab in it, ``value`` will be ``None``.

    When reading from a line with multiple tabs, we break on the first one.

    Your key should probably not be ``None`` or have tab characters in it, but
    we don't check.
    """
    def read(self, line):
        assert(is_bytes(line))
        key_value = line.split(b'\t', 1)
        if len(key_value) == 1:
            key_value.append(None)

        return tuple(key_value)

    def write(self, key, value):
        assert(is_bytes(key) and is_bytes(value))
        return b'\t'.join(x for x in (key, value) if x is not None)


class RawValueProtocol(object):
    """Read in a line as ``(None, line)``. Write out ``(key, value)``
    as ``value``. ``value`` must be a ``bytes``.

    The default way for a job to read its initial input.
    """
    def read(self, line):
        assert(is_bytes(line))
        return (None, line)

    def write(self, key, value):
        assert(is_bytes(key) and is_bytes(value))
        return value


class ReprProtocol(_KeyCachingProtocol):
    """Encode ``(key, value)`` as two reprs separated by a tab.

    This only works for basic types (we use :py:func:`mrjob.util.safeeval`).
    """

    def _loads(self, value):
        assert(is_bytes(value))
        return safeeval(to_text(value))

    def _dumps(self, value):
        if six.PY3 and isinstance(value, bytes):
            # We either need to sacrifice compatibility or risk an exception
            # here. Since people dealing with non-utf-8 bytes are unlikely to
            # use the repr protocol, we go with the latter.
            value = value.decode('utf-8')
        return to_bytes(repr(value))


class ReprValueProtocol(object):
    """Encode ``value`` as a repr and discard ``key`` (``key`` is read
    in as None).

    This only works for basic types (we use :py:func:`mrjob.util.safeeval`).
    """
    def read(self, line):
        assert(is_bytes(line))
        return (None, safeeval(to_text(line)))

    def write(self, key, value):
        return to_bytes(repr(value))
