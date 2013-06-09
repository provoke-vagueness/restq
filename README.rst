Introduction to restq 
*********************

Why restq?

We wanted to have a simple platform independent solution for managing the
coordination and distribution of batched execution across our analysis
platforms.  restq solved our wants to have a system that could:

 * segregate execution based on a category or type (realm),
 * manage priorities of job execution (ordered queues),
 * enqueue, check-out, and expiry time based (almost FIFO) dequeuing of jobs
   from a queue.
 * status of jobs remaining against arbitrary tag indices.
 * zero configuration for the concepts talked about above.  


What's in restq:

 * An implementation of the execution management system described above.  
 * A RESTful web API that exposes complete control over the execution
   management system.
 * A Python client that seamlessly interfaces the RESTful web API.



For additional tips / tricks with this restq feel free to post a question at 
the github `restq/issues`_ page. 


Project hosting provided by `github.com`_.


[mjdorma+restq@gmail.com] and [sptonkin@outlook.com]


Install and run
===============

Simply run the following::

    > python setup.py install

or `PyPi`_:: 

    > pip install restq


Getting started
===============

Coding with restq
-----------------

A simple example on how ::

 > restq web &
 > ipython

 In [1]: from restq import Realms

 In [2]: realms = Realms()

 In [3]: realms.test.
 realms.test.add
 realms.test.bulk_add
 realms.test.bulk_flush
 realms.test.get_job
 realms.test.get_tag_status
 realms.test.get_tagged_jobs
 realms.test.name
 realms.test.pull
 realms.test.remove_job
 realms.test.remove_tagged_jobs
 realms.test.request
 realms.test.requester
 realms.test.set_default_lease_time
 realms.test.set_queue_lease_time
 realms.test.status

 In [3]: realms.test.add('job 1', 0, 'do the dishes', tags=['house work'])

 In [4]: realms.test.add('job 2', 0, 'cut the grass', tags=['house work'])

 In [5]: realms.test.add('job 3', 1, 'fix bugs in restq', tags=['devel'])

 In [6]: realms.test.add('job 4', 3, 'document restq', tags=['devel'])

 In [7]: realms.test.add('job 5', 0, 'go for walk', tags=['sport'])

 In [8]: realms.test.status
 Out[8]: 
 {u'queues': {u'0': 4, u'1': 1, u'2': 1, u'3': 1},
  u'total_jobs': 7,
  u'total_tags': 3}

 In [9]: jobs = realms.test.pull(count=7)

 In [10]: jobs
 Out[10]: 
 {u'job 1': [0, u'do the dishes'],
  u'job 2': [0, u'cut the grass'],
  u'job 3': [1, u'fix bugs in restq'],
  u'job 4': [3, u'document restq'],
  u'job 5': [0, u'go for walk'],
  u'job 6': [0, u'go for walk with dog'],
  u'job 7': [2, u'go for bike ride']}

 In [11]: realms.test.get_tag_status('house work')
 Out[11]: {u'count': 2}

 In [12]: realms.test.get_tagged_jobs('devel')
 Out[12]: 
 {u'job 3': {u'data': u'fix bugs in restq',
   u'queues': [[1, 82.17003393173218]],
   u'tags': [u'devel']},
  u'job 4': {u'data': u'document restq',
   u'queues': [[3, 82.16989994049072]],
   u'tags': [u'devel']}}



Issues
======

Source code for *restq* is hosted on `GitHub <https://github.com/provoke-vagueness/restq>`_. 
Please file `bug reports <https://github.com/provoke-vagueness/restq/issues>`_
with GitHub's issues system.


Change log
==========

version 0.0.3 (06/06/2013)
 
 * bulk post & stable error handling

version 0.0.1 (10/04/2013)

 * pre life


.. _github.com: https://github.com/provoke-vagueness/restq
.. _PyPi: http://pypi.python.org/pypi/restq
.. _restq/issues: https://github.com/provoke-vagueness/restq/issues
.. |build_status| image:: https://secure.travis-ci.org/provoke-vagueness/restq.png?branch=master
   :target: http://travis-ci.org/#!/provoke-vagueness/restq
