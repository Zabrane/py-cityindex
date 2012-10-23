============
py-cityindex
============

This is a project to implement a robust set of client bindings for the `City Index <http://www.cityindex.co.uk/>`_ HTTP API, including streaming data and order submission.

At present, order submission is less developed than the streaming support, which is to a point where it works relatively well. **Please get in touch before using this code in production! It's full of known bugs!**

Streaming data works by way of `py-lightstreamer <http://github.com/dw/py-lightstreamer/>`_, which itself is only a recent project. In all, use this code at your peril, however for basic experimentation it should be in reasonable shape.

Patches welcome!
