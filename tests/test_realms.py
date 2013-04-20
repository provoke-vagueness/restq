import unittest
from pprint import pprint
import time
import os

from restq import realms 


class TestJobsBase(unittest.TestCase):

    def setUp(self):
        try:
            os.remove(os.path.join(realms.CONFIG_ROOT, 'test.realm'))
            reload(realms)
            realms.DEFAULT_LEASE_TIME = 0.5
            self.realms = realms
        except OSError:
            pass


class TestJobs(TestJobsBase):

    def test_add(self):
        """add data"""
        realm = self.realms.get('test')
        realm.add(0, 'q0', 'h', tags=['project 1', 'task 1'])

        status = realm.status
        self.assertEqual(len(status['queues']), 1)
        self.assertEqual(status['total_jobs'], 1)
        self.assertEqual(status['total_tags'], 2)

        realm.add(1, 'q0', None, tags=['project 1', 'task 1'])
        status = realm.status
        self.assertEqual(len(status['queues']), 1)
        self.assertEqual(status['total_jobs'], 2)
        self.assertEqual(status['total_tags'], 2)
 
        realm.add(2, 'q0', 443434, tags=['project 1', 'task 2', 'odd job'])
        status = realm.status
        self.assertEqual(len(status['queues']), 1)
        self.assertEqual(status['total_jobs'], 3)
        self.assertEqual(status['total_tags'], 4)

        realm.add(2, 'q0', 443434, tags=['project 2', 'task 2'])
        status = realm.status
        self.assertEqual(len(status['queues']), 1)
        self.assertEqual(status['total_jobs'], 3)
        self.assertEqual(status['total_tags'], 5)

        realm.add(3, 'q1', 3343.343434, tags=['project 2', 'task 2'])
        status = realm.status
        self.assertEqual(len(status['queues']), 2)
        self.assertEqual(status['total_jobs'], 4)
        self.assertEqual(status['total_tags'], 5)


    def test_remove_job(self):
        """remove a job"""
        realm = self.realms.get('test')
        realm.add("job 1", 'q0', 'h', tags=['project 1', 'task 1'])
        realm.add("job 1", 'q0', 'h', tags=['project 1', 'task 1'])
        realm.add("job 1", 'q1', 'h', tags=['project 2', 'task 1'])
        realm.add("job 2", 'q0', 'h', tags=['project 1', 'task 2'])

        realm.remove_job("job 2")

        status = realm.status
        self.assertEqual(len(status['queues']), 2)
        self.assertEqual(status['total_jobs'], 1)
        self.assertEqual(status['total_tags'], 3)


    def test_remove_tagged_task(self):
        """remove a task"""
        realm = self.realms.get('test')
        realm.add("job1", 'q0', 'h', tags=['project 1', 'task 1'])
        realm.add("job1", 'q0', 'h', tags=['project 1', 'task 1'])
        realm.add("job1", 'q1', 'h', tags=['project 2', 'task 1'])
        realm.add("job2", 'q0', 'h', tags=['project 1', 'task 2'])

        realm.remove_tagged_jobs("task 1")

        status = realm.status
        self.assertEqual(len(status['queues']), 2)
        self.assertEqual(status['total_jobs'], 1)
        self.assertEqual(status['total_tags'], 2)

    def test_remove_tagged_project(self):
        """remove a project"""
        realm = self.realms.get('test')
        realm.add("job1", 'q0', 'h', tags=['project 1', 'task 1'])
        realm.add("job1", 'q0', 'h', tags=['project 1', 'task 1'])
        realm.add("job2", 'q1', 'h', tags=['project 2', 'task 1'])
        realm.add("job1", 'q0', 'h', tags=['project 1', 'task 2'])

        realm.remove_tagged_jobs("project 1")

        status = realm.status
        self.assertEqual(len(status['queues']), 2)
        self.assertEqual(status['total_jobs'], 1)
        self.assertEqual(status['total_tags'], 2)

    def test_get_jobs(self):
        """get the state of a job"""
        realm = self.realms.get('test')
        realm.add("job1", 'q0', 'h', tags=['project 1', 'task 1'])
        realm.add("job1", 'q0', 'h', tags=['project 1', 'task 1'])
        realm.add("job1", 'q1', 'h', tags=['project 2', 'task 1'])
        realm.add("job2", 'q0', 'h', tags=['project 1', 'task 2'])

        state = realm.get_job("job1")
        state = realm.get_tagged_jobs("task 1")
        state = realm.get_tagged_jobs("project 1")

    def test_pull(self):
        """pull data test"""
        realm = self.realms.get('test')
        realm.add("job0", "q0", 'h')
        realm.add("job1", "q0", None)
        realm.add("job2", "q0", 443434)
        realm.add("job3", "q1", 3343.343434)
        realmer = realm.pull(4)
        realmer = dict(realmer)
        self.assertEqual(len(realmer), 4)
        self.assertEqual(realmer["job3"][1], 3343.343434)

        #make sure there are no more realm available because they should be
        # checked out with the previous pull request
        realmer = realm.pull(4)
        self.assertFalse(realmer)

        #now that the least time has expired, lets make sure we can check out 
        # the realm once again
        time.sleep(1)        
        realmer = realm.pull(4)
        realmer = dict(realmer)
        self.assertEqual(len(realmer), 4)
        self.assertEqual(realmer["job1"][1], None)

        #again, make sure the realm are all checked out
        realmer = realm.pull(4)
        self.assertFalse(realmer)

        #make sure we can checkout one job, wait until it will be 
        # checked back in, but when we checkout the next job, we should 
        # increment to the next job in the queue
        time.sleep(1)        
        realmer = realm.pull(1)
        realmer = dict(realmer)
        self.assertEqual(realmer["job0"][1], 'h')


class TestJobsNonGeneric(TestJobsBase):

    def test_add_diff_data(self):
        """add diff data errors"""
        realm = self.realms.get('test')
        realm.add("job 1", 'q0', 'data one')
        self.assertRaises(ValueError,
                realm.add, "job 1", "q0", "data broke")

