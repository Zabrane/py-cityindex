============
py-cityindex
============

This is a project to implement a robust set of client bindings for the
`City Index <http://www.cityindex.co.uk/>`_ HTTP API, including streaming data
and order submission.

At present, order submission is less developed than the streaming support,
which is to a point where it works relatively well. **Please get in touch
before using this code in production! It's full of known bugs!**

Streaming data works by way of
`py-lightstreamer <http://github.com/dw/py-lightstreamer/>`_, which itself is
only a recent project. In all, use this code at your peril, however for basic
experimentation it should be in reasonable shape.

Patches welcome!


Code Style
^^^^^^^^^^

**Why are timestamps floating point?**
    Given the available choices, a UNIX timestamp expressed as float seems the
    least ambiguous and most flexible representation of time, as no further
    effort is required to encode such a timestamp for transmission, apply
    arithmetic to it, identify its reference timezone, or mix it with integer
    seconds, whereas msec and higher precision ints require divison before
    performing most common operations.

    Use of ``datetime`` in a library quickly breaks down in the presence of
    timezone-aware programs, where mixing aware datetimes with naive datetimes
    gets ugly fast. UNIX timestamps are implicitly UTC, easily convert to
    datetime via ``datetime.fromtimestamp()`` (while the converse is not true),
    and require no further effort for sorting, or storing e.g. as JSON or in
    SQLite.

    Performance is another factor; manipulating large quantities of
    timezone-aware datetimes is horrifically slow, particularly with ``pytz``.
    The datetime API is well suited for calendaring or on-screen display tasks,
    but as an internal format where timezones are unimportant it kinda sucks.

**Why are prices floating point?**
    See previous answer. Python's ``decimal.Decimal`` is a nice ideal, but
    performance is horrendous, few know how to configure it correctly, it
    suffers the same serializability issues as ``datetime``, and is generally
    overkill for the task of representing prices. In particular it is
    noteworthy that ``decimal`` remains a fixed precision type.

    It is also commonplace for useful libraries to simply ignore ``decimal``
    altogether in preference of float: none of ``pandas``, ``TaLib`` or NumPy
    offer direct support. This means that consuming a time series in those
    libraries would require a conversion step.

    Using the ``PriceDecimalPlaces`` field of ``ApiMarketInformationDTO`` it
    should always be possible to recover a decimal price from its floating
    point representation.

**Dictionaries. Everywhere.**
    While it is possible (and possibly convenient at times) to introduce
    distinct types for API entities, it results in more code required to
    serialize, deserialize, convert, store, or otherwise mogrify data exchanged
    with the library, alongside a detrimental effect on modularity and
    discoverability as its types leak into consumer code.

    One line tasks like producing an audit log or emitting to a message bus
    would require serializers, while log replay would require corresponding
    deserializers (most probably with ironic names like ``order_to_dict()``).
    Since it is desirable to avoid writing such error-prone functions by hand,
    a framework for their automatic generation would thus be required. Now the
    task of referencing an integer in a dict has transmuted into a
    ``MapperValidationError`` exception with 30 frame stack trace encompassing
    an elaborately engineered hierarchy of nonsensically named classes.

    An additional concern is more philosophical in nature: types are useful for
    abstracting implementation and state, whereas data is inherently concrete
    and factual in nature. In some respects, wrapping data with types in a
    language like Python may be seen as far from a sensible approach.

    A final appeal of discrete types, validation, is more simply provided
    orthogonally, without coupling its implementation to the data under test.
