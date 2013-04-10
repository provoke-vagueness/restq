import unittest
from pprint import pprint
import time
import os

from restq import realms 

#change the lease time to 1 second while we run tests
realms.DEFAULT_LEASE_TIME = 0.5


class TestJobs(unittest.TestCase):

    def setUp(self):
        try:
            os.remove(os.path.join(realms.CONFIG_ROOT, 'test.conf'))
        except OSError:
            pass

    def test_add(self):
        """add data"""
        work = realms.get('test')
        work.add(0, 0, 0, 'h')
        status = work.status
        self.assertEqual(len(status['queues']), 1)
        self.assertEqual(status['total_jobs'], 1)
        self.assertEqual(status['total_tasks'], 1)
    
        work.add(1, 0, 0, None)
        status = work.status
        self.assertEqual(len(status['queues']), 1)
        self.assertEqual(status['total_jobs'], 2)
        self.assertEqual(status['total_tasks'], 1)
 
        work.add(2, 2, 0, 443434)
        status = work.status
        self.assertEqual(len(status['queues']), 1)
        self.assertEqual(status['total_jobs'], 3)
        self.assertEqual(status['total_tasks'], 2)

        work.add(3, 2, 1, 3343.343434)
        status = work.status
        self.assertEqual(len(status['queues']), 2)
        self.assertEqual(status['total_jobs'], 4)
        self.assertEqual(status['total_tasks'], 2)

        pprint(work.tasks)
        pprint(work.jobs)
        pprint(work.queues)
 
    def test_add_diff_data(self):
        """add diff data errors"""
        work = realms.get('test')
        work.add(0, 0, 0, 'h')
        self.assertRaises(ValueError,
                work.add, 0, 0, 0, 'a')

    def test_pull(self):
        """test that we can pull work"""
        work = realms.get('test')
        work.add(0, 0, 0, 'h')
        work.add(1, 0, 0, None)
        work.add(2, 2, 0, 443434)
        work.add(3, 2, 1, 3343.343434)
        worker = work.pull(4)
        pprint(worker)
        self.assertEqual(len(worker), 4)
        self.assertEqual(worker[3][1], 3343.343434)

        #make sure there are no more work available because they should be
        # checked out with the previous pull request
        worker = work.pull(4)
        pprint(worker)
        self.assertFalse(worker)

        #now that the least time has expired, lets make sure we can check out 
        # the work once again
        time.sleep(1)        
        worker = work.pull(4)
        pprint(worker)
        self.assertEqual(len(worker), 4)
        self.assertEqual(worker[1][1], None)

        #again, make sure the work are all checked out
        worker = work.pull(4)
        pprint(worker)
        self.assertFalse(worker)

        #make sure we can checkout one job, wait until it will be 
        # checked back in, but when we checkout the next job, we should 
        # increment to the next job in the queue
        time.sleep(1)        
        worker = work.pull(1)
        pprint(worker)
        self.assertEqual(worker[0][1], 'h')
        time.sleep(1)



