import unittest
from pprint import pprint
import time

import restq
from restq import Jobs

#change the lease time to 1 second while we run tests
restq.JOB_LEASE_TIME = 0.5


class TestJobs(unittest.TestCase):
    def test_add(self):
        """add data"""
        jobs = Jobs()
        jobs.add(0, 0, 0, 'h')
        status = jobs.status
        self.assertEqual(len(status['queues']), 1)
        self.assertEqual(status['total_jobs'], 1)
        self.assertEqual(status['total_tasks'], 1)
    
        jobs.add(1, 0, 0, None)
        status = jobs.status
        self.assertEqual(len(status['queues']), 1)
        self.assertEqual(status['total_jobs'], 2)
        self.assertEqual(status['total_tasks'], 1)
 
        jobs.add(2, 2, 0, 443434)
        status = jobs.status
        self.assertEqual(len(status['queues']), 1)
        self.assertEqual(status['total_jobs'], 3)
        self.assertEqual(status['total_tasks'], 2)

        jobs.add(3, 2, 1, 3343.343434)
        status = jobs.status
        self.assertEqual(len(status['queues']), 2)
        self.assertEqual(status['total_jobs'], 4)
        self.assertEqual(status['total_tasks'], 2)

        pprint(jobs.tasks)
        pprint(jobs.jobs)
        pprint(jobs.queues)
 
    def test_add_diff_data(self):
        """add diff data errors"""
        jobs = Jobs()
        jobs.add(0, 0, 0, 'h')
        self.assertRaises(ValueError,
                jobs.add, 0, 0, 0, 'a')

    def test_pull(self):
        """test that we can pull jobs"""
        jobs = Jobs()
        jobs.add(0, 0, 0, 'h')
        jobs.add(1, 0, 0, None)
        jobs.add(2, 2, 0, 443434)
        jobs.add(3, 2, 0, 3343.343434)
        worker = jobs.pull(4)
        pprint(worker)
        self.assertEqual(len(worker), 4)
        self.assertEqual(worker[3][1], 3343.343434)

        #make sure there are no more jobs available because they should be
        # checked out with the previous pull request
        worker = jobs.pull(4)
        pprint(worker)
        self.assertFalse(worker)

        #now that the least time has expired, lets make sure we can check out 
        # the jobs once again
        time.sleep(1)        
        worker = jobs.pull(4)
        pprint(worker)
        self.assertEqual(len(worker), 4)
        #self.assertEqual(worker[1][1], 3343.343434)

        #again, make sure the jobs are all checked out
        worker = jobs.pull(4)
        pprint(worker)
        self.assertFalse(worker)

        #make sure we can checkout one job, wait until it will be 
        # checked back in, but when we checkout the next job, we should 
        # increment to the next job in the queue
        time.sleep(1)        
        worker = jobs.pull(1)
        print worker
        #self.assertEqual(worker[0], 'h')
        time.sleep(1)



