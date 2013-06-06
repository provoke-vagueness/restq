Introduction to restq 
*********************

What is restq:

* A priority queuing job checkout and completion service.
* Controlled and accessed through a bottle RESTful web application.


For additional tips / tricks with this restq feel free to post a question at 
the github `restq/issues`_ page. 


Project hosting provided by `github.com`_.


[mjdorma+restq@gmail.com] and [sptonkin@outlook.com]


Install and run
===============

Simply run the following::

    > python setup.py install
    > python setup.py test
    > python -m restq -h

or `PyPi`_:: 

    > pip install restq
    > python -m restq -h

Example::

    > nohup restq-webapp &
    > ipython
    # import the client 
    from restq import Realms
    # connect to our local restq-webapp
    realms = Realms()
    # add some jobs
    realms.test.add('job 1', 0, 'do the dishes', tags=['house work'])
    realms.test.add('job 2', 0, 'cut the grass', tags=['house work'])
    realms.test.add('job 3', 1, 'fix bugs in restq', tags=['devel'])
    realms.test.add('job 4', 3, 'document restq', tags=['devel'])
    realms.test.add('job 5', 0, 'go for walk', tags=['sport'])
    realms.test.add('job 6', 0, 'go for walk with dog', tags=['sport'])
    realms.test.add('job 7', 2, 'go for bike ride', tags=['sport'])
    jobs = realms.test.pull(count=7)
    print(jobs)


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
