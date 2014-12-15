# Copyright 2009-2012 Yelp
# Copyright 2013 David Marin
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
"""Trivial multi-step job, useful for testing runners."""
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.portability import is_bytes, to_bytes, to_text

try:
    import simplejson as json  # preferred because of C speedups
    json  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    import json  # built in to Python 2.6 and later


class CustomRawValueProtocol(object):
    """Just like :py:class:`mrjob.protocol.RawValueProtocol`, but using
    instance methods instead of classmethods like we're supposed to
    """

    def read(self, line):
        assert(is_bytes(line))
        return (None, line)

    def write(self, key, value):
        return to_bytes(value)


class CustomJSONProtocol(object):
    """Just like :py:class:`mrjob.protocol.JSONProtocol`, but using
    instance methods instead of classmethods like we're supposed to
    """

    def read(self, line):
        assert(is_bytes(line))
        tline = line.decode('utf-8')
        k, v = tline.split('\t', 1)
        return (json.loads(to_text(k)), json.loads(to_text(v)))

    def write(self, key, value):
        return to_bytes('%s\t%s' % (json.dumps(key), json.dumps(value)))


class MRTwoStepJob(MRJob):

    INPUT_PROTOCOL = CustomRawValueProtocol
    INTERNAL_PROTOCOL = CustomJSONProtocol

    def mapper(self, key, value):
        yield key, value
        yield value, key

    def combiner(self, key, values):
        # just pass through and make note that this was run
        self.increment_counter('count', 'combiners', 1)
        for value in values:
            yield key, value

    def reducer(self, key, values):
        yield key, len(list(values))

    def mapper2(self, key, value):
        yield value, key

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer,
                       combiner=self.combiner),
                MRStep(mapper=self.mapper2)]


if __name__ == '__main__':
    MRTwoStepJob.run()
