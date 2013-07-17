Introduction to restq 
*********************

Why restq?

We wanted to have a simple platform independent solution for managing the
coordination and distribution of batched execution across our analysis
platforms.  restq solved our wants into a system that could:

 * segregate execution based on a category or type (realm),
 * manage priorities of job execution (ordered queues),
 * enqueue, check-out, and expiry time based (almost FIFO) dequeuing of jobs
   from a realm.
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


Using restq in the shell
------------------------

Lets start this example by adding some arguments into the default realm::

  > restq add "ls -lah"
  >
  > # Lets tag the argument "ls -lah" with a label of 'work'. 
  > restq add --tags=work "ls -lah"
  >
  > # Add another argument to the realm, but this time we'll tag it with work
  > # and fun. 
  > restq add --tags=work,fun  pwd
  >
  > # Checkout the status of the realm. 
  > restq status
  Status of realm default:
  Contains 2 tags with 2 jobs
  Defined queues: 0
  >
  > # time to add pwd to another queue
  > restq add --queue=1 pwd
  >
  > restq status
  Status of realm default:
  Contains 2 tags with 2 jobs
  Defined queues: 1, 0

Now lets see how we can pull (checkout) arguments and execute them::

  > # Pull and execute a maximum of two arguments from the default realm.  
  > # After the default time out, these arguments will be available for 
  > # checkout once again.
  > while read i; do eval "$i"; done < <(restq pull --count=2)
  drwxr-xr-x 9 mick mick 4.0K Jul 18 08:01 .
  drwxrwxr-x 9 mick mick 4.0K Jul 14 03:07 ..
  drwxrwxr-x 3 mick mick 4.0K Jul 12 00:04 docs
  -rw-rw-r-- 1 mick mick   72 Jul 12 00:04 MANIFEST.in
  -rw-rw-r-- 1 mick mick 3.7K Jul 12 00:04 README.rst
  drwxrwxr-x 2 mick mick 4.0K Jul 17 23:13 restq
  -rw-rw-r-- 1 mick mick 2.1K Jul 17 19:57 setup.py
  drwxrwxr-x 2 mick mick 4.0K Jul 12 00:04 tests
  -rw-rw-r-- 1 mick mick  321 Jul 12 00:04 .travis.yml
  /home/mick/work/restq
  >
  > # The argument pwd was placed into two queues.  Lets do another pull to 
  > # see it being dequeued from queue 1.
  > restq pull
  pwd
  >
  > # Lets check the status of the pwd argument since checkout. This shows what
  > # queues a specific argument is in, what tags it has, and how long it has
  > # been since it was checked out (pulled). 
  > restq status arg pwd
  Status of argument pwd:
  Tagged with: work
  queue id | (s) since dequeue
       1 | 35.22
       0 | 454.49
  >
  > # Time to remove pwd from our realm...  We're done with this argument and 
  > # we no longer require it for execution.  You will notice that the fun tag
  > # no longer exists in the realm as it was only attched to pwd. 
  > restq remove arg pwd
  >
  > # The default lease time for a dequeue of an arguement is 600s.  At this 
  > # point we should be able to pull our ls -lah argument from the realm
  > restq pull
  ls -lah


Issues
======

Source code for *restq* is hosted on `GitHub <https://github.com/provoke-vagueness/restq>`_. 
Please file `bug reports <https://github.com/provoke-vagueness/restq/issues>`_
with GitHub's issues system.


Change log
==========

version 0.1.0 (18/07/2013)

 * implemented cli controls. 
 * realms now using yaml -> breaks compatibility with previous version.

version 0.0.4 (09/06/2013)

 * config and cli shell implementation

version 0.0.3 (06/06/2013)
 
 * bulk post & stable error handling

version 0.0.1 (10/04/2013)

 * pre life


.. _github.com: https://github.com/provoke-vagueness/restq
.. _PyPi: http://pypi.python.org/pypi/restq
.. _restq/issues: https://github.com/provoke-vagueness/restq/issues
.. |build_status| image:: https://secure.travis-ci.org/provoke-vagueness/restq.png?branch=master
   :target: http://travis-ci.org/#!/provoke-vagueness/restq
