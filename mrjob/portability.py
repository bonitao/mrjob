import six
import io

try:
    from cStringIO import StringIO
    StringIO  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    from six import StringIO
finally:
    if six.PY2:
        BytesIO = StringIO
    else:
        from six import BytesIO


_BYTES_IO_TYPE = type(BytesIO())
_STRING_IO_TYPE = type(StringIO())
_BINARY_TYPES = (six.binary_type, io.BufferedReader, io.BufferedWriter, _BYTES_IO_TYPE)
_TEXT_TYPES = (six.text_type, io.TextIOWrapper, _STRING_IO_TYPE)


# TODO: rename to to_binary and to_unicode
def to_bytes(stream):
    if stream is None:
        return stream
    if isinstance(stream, _BINARY_TYPES):
        return stream
    if isinstance(stream, six.text_type):
        return stream.encode('utf-8')
    if isinstance(stream, (six.binary_type)):
        return stream
    if isinstance(stream, io.TextIOWrapper):
        return stream.buffer
    if isinstance(stream, _STRING_IO_TYPE):
        return BytesIO(stream.getvalue().encode('utf-8'))
    if isinstance(stream, list):
        return [to_bytes(x) for x in stream]
    # Given a stream like object, it seems I can't really  say whether it will
    # return bytes or text. If we are in PY2 world, it does not make a
    # difference.
    if six.PY2:
        return stream
    # Since we can't say whether we are reading bytes or text, give up. We could
    # alternatively convert after reading, but that is too invasive to the
    # codebase and potentially performance affecting.
    raise IOError('Unable to convert stream of type %s into a binary stream.' %
                  type(stream))


def to_text(stream):
    if stream is None:
        return stream
    if isinstance(stream, _TEXT_TYPES):
        return stream
    if isinstance(stream, six.binary_type):
        return stream.decode('utf-8')
    if isinstance(stream, io.BufferedReader):
        return io.TextIOWrapper(stream)
    if isinstance(stream, io.BufferedWriter):
        return io.TextIOWrapper(stream)
    if isinstance(stream, _BYTES_IO_TYPE):
        return io.TextIOWrapper(stream)
    if isinstance(stream, list):
        return [to_text(x) for x in stream]
    # Given a stream like object, it seems I can't really  say whether it will
    # return bytes or text. If we are in PY2 world, it does not make a
    # difference.
    if six.PY2:
        return stream
    # Since we can't say whether we are reading bytes or text, give up. We could
    # alternatively convert after reading, but that is too invasive to the
    # codebase and potentially performance affecting.
    raise IOError('Unable to convert stream of type %s into a text stream.' %
                  type(stream))


def is_bytes(stream):
    return isinstance(stream, _BINARY_TYPES) or stream is None


def is_text(stream):
    return isinstance(stream, _TEXT_TYPES) or stream is None
