Introduction to restq 
*********************

What is restq:

* A priority queuing job checkout and completion service.
* Controlled and accessed through a bottle RESTful web application.

As a reference and guide to restq see: `restq documentation`_


For additional tips / tricks with this restq feel free to post a question at 
the github `restq/issues`_ page. 


Project hosting provided by `github.com`_.


[mjdorma+yara-ctypes@gmail.com] and [sptonkin@outlook.com]


Install and run
===============

Simply run the following::

    > python setup.py install
    > python setup.py test
    > python -m restq -h

or `PyPi`_:: 

    > pip install restq
    > python -m restq -h


Compatability
=============

*restq* is implemented to be compatible with Python 2.6+ and Python 3.x.
It has been tested against the following Python implementations:

Ubuntu 12.04:

 + CPython 2.7 (32bit, 64bit)
 + CPython 3.2 (32bit, 64bit)

Ubuntu 11.10 |build_status|:

 + CPython 2.6 (32bit)
 + CPython 2.7 (32bit)
 + CPython 3.2 (32bit)
 + PyPy 1.9.0 (32bit)

Windows 7:

 + CPython 2.6 (32bit, 64bit)
 + CPython 3.2 (32bit, 64bit)

OS X Mountain Lion

 + CPython 2.7 (64bit)


Continuous integration testing is provided by `Travis CI <http://travis-ci.org/>`_.


Issues
======

Source code for *yara-ctypes* is hosted on `GitHub <https://github.com/provoke-vagueness/restq>`_. 
Please file `bug reports <https://github.com/provoke-vagueness/restq/issues>`_
with GitHub's issues system.


Change log
==========

version 0.0.1 (10/04/2013)

* pre life


.. _github.com: https://github.com/provoke-vagueness/restq
.. _PyPi: http://pypi.python.org/pypi/yara
.. _yara-ctypes/issues: https://github.com/provoke-vagueness/restq/issues
.. _notes on building: http://packages.python.org/yara/howto/build.html
.. _yara-ctypes documentation: http://packages.python.org/yara/
.. |build_status| image:: https://secure.travis-ci.org/provoke-vagueness/restq.png?branch=master
   :target: http://travis-ci.org/#!/provoke-vagueness/restq
