import unittest
from pprint import pprint
import time
import os
import sys

if sys.version_info[0] >= 3:
    from imp import reload

from restq import realms


class TestRealmsBase(unittest.TestCase):

    def setUp(self):
        reload(realms)
        realms.delete("test")
        realms.DEFAULT_LEASE_TIME = 0.5
        self.realms = realms


class TestRealms(TestRealmsBase):
    """ These test cases are used in both restq.realm testing and the full
        webapp<->client<->realm test (see test_client)."""

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
        self.assertEqual(realm.get_tag_status('project 2')['count'], 1)
        self.assertRaises(KeyError, realm.get_tag_status, 'project 1')

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
        realm.set_default_lease_time(1)
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

        # now that the least time has expired, lets make sure we can check out
        # the realm once again
        time.sleep(1.5)
        realmer = realm.pull(4)
        realmer = dict(realmer)
        self.assertEqual(len(realmer), 4)
        self.assertEqual(realmer["job1"][1], None)

        #again, make sure the realm are all checked out
        realmer = realm.pull(4)
        self.assertFalse(realmer)

        # make sure we can checkout one job, wait until it will be
        # checked back in, but when we checkout the next job, we should
        # increment to the next job in the queue
        time.sleep(1.5)
        realmer = realm.pull(1)
        realmer = dict(realmer)
        self.assertEqual(realmer["job0"][1], 'h')

    def test_move_job(self):
        """move job test"""
        realm = self.realms.get('test')
        realm.set_default_lease_time(1)
        realm.add("job0", "q0", 'h')
        realm.add("job1", "q0", None)
        realm.add("job2", "q0", 443434)
        realm.add("job3", "q1", 3343.343434)
        realm.add("job1", "q1", None)

        # try to move a job that has not been checked out
        realm.move_job("job0", "q0", "q1")
        status = realm.status
        self.assertEqual(status['queues']['q0'], 2)
        self.assertEqual(status['queues']['q1'], 3)

        # try to move a job to a new queue
        realm.move_job("job0", "q1", "q6")
        status = realm.status
        self.assertEqual(status['queues']['q1'], 2)
        self.assertEqual(status['queues']['q6'], 1)

        # try to move a job into a queue it is already in
        realm.move_job("job1", "q1", "q0")
        status = realm.status
        self.assertEqual(status['queues']['q1'], 1)
        self.assertEqual(status['queues']['q0'], 2)

        # try to move a job from a non-existent queue
        self.assertRaises(ValueError, realm.move_job, "job0", "q9", "q0")

        # try to move a job that doesn't exist
        self.assertRaises(ValueError, realm.move_job, "job9", "q1", "q0")

        # try to move a job from the wrong queue
        self.assertRaises(ValueError, realm.move_job, "job3", "q0", "q2")

        # try to move a job that has been checked out already
        realm.pull(5)
        self.assertRaises(ValueError, realm.move_job, "job3", "q1", "q2")

    def test_clear_queue(self):
        """clear queue test"""
        realm = self.realms.get('test')
        realm.set_default_lease_time(1)
        realm.add("job0", "q0", 'h')
        realm.add("job1", "q0", None)
        realm.add("job2", "q0", 443434)
        realm.add("job3", "q1", 3343.343434)
        realm.add("job1", "q1", None)

        # make sure clear only clears the one queue
        realm.clear_queue("q0")
        status = realm.status
        self.assertEqual(status['queues']['q0'], 0)
        self.assertEqual(status['queues']['q1'], 2)
        # make sure jobs with no queues are removed
        self.assertRaises(KeyError, realm.get_job, 'job0')
        # make sure jobs with multiple queues are not removed
        self.assertTrue(realm.get_job('job1'),
                        'Job with multiple queues was removed')

        # try to clear a non existent queue
        self.assertRaises(ValueError, realm.clear_queue, "q55")

    def test_pull_priority(self):
        """
        Pull jobs up to a max queue priority.
        """
        realm = self.realms.get('test')
        realm.add("job0", "q0", 'h')
        realm.add("job1", "q0", None)
        realm.add("job2", "q0", 443434)
        realm.add("job3", "q1", 3343.343434)
        realmer = realm.pull(4, max_queue="q0")
        realmer = dict(realmer)
        self.assertEqual(len(realmer), 3)
        self.assertEqual(realmer["job2"][1], 443434)

    def test_global_pull(self):
        """
        Pull jobs, in queue order, across multiple realms.
        """
        realm = self.realms.get('test1')
        realm.add("job1.0", "q0", 'h')
        realm.add("job1.1", "q1", None)
        realm.add("job1.2", "q1", 443434)
        realm.add("job1.3", "q1", 3343.343434)

        realm = self.realms.get('test2')
        realm.add("job2.0", "q0", 'h')
        realm.add("job2.1", "q0", None)
        realm.add("job2.2", "q0", 443434)
        realm.add("job2.3", "q1", 3343.343434)

        realmer = self.realms.pull(3)
        realmer = dict(realmer)
        print(realmer)
        self.assertEqual(len(realmer), 3)
        self.assertTrue("job1.0" in realmer)
        self.assertTrue("job2.0" in realmer)
        self.assertTrue("job2.1" in realmer)

    def test_global_subset_pull(self):
        """
        Pull jobs, in queue order, across multiple realms.
        """
        realm1 = self.realms.get('test1')
        realm1.add("job1.0", "q0", 'h')
        realm1.add("job1.1", "q1", None)
        realm1.add("job1.2", "q2", 443434)
        realm1.add("job1.3", "q2", 3343.343434)

        realm2 = self.realms.get('test2')
        realm2.add("job2.0", "q0", 'h')
        realm2.add("job2.1", "q0", None)
        realm2.add("job2.2", "q1", 443434)
        realm2.add("job2.3", "q1", 3343.343434)

        realm3 = self.realms.get('test3')
        realm3.add("job3.0", "q0", 'h')
        realm3.add("job3.1", "q0", None)
        realm3.add("job3.2", "q1", 443434)
        realm3.add("job3.3", "q1", 3343.343434)

        realmer = self.realms.pull(5, realms=['test1', 'test3'])
        realmer = dict(realmer)
        self.assertEqual(len(realmer), 5)
        self.assertTrue("job1.0" in realmer)
        self.assertTrue("job3.0" in realmer)
        self.assertTrue("job3.1" in realmer)
        self.assertTrue("job1.1" in realmer)
        self.assertTrue("job3.2" in realmer)

    def test_unsafe_iter(self):
        """
        Attempt to trigger an iteration error when iterating
        a queue that is modified in a particular sequence.
        """
        realm = self.realms.get('test')
        realm.add(101, 1)
        realm.add(102, 1)
        realm.pull(1)
        realm.remove_job(101)
        realm.remove_job(102)
        realm.add(103, 1)
        jobs = realm.pull(1)


class TestRealmsNonGeneric(TestRealmsBase):
    """Test the stuff that applies just to realm and not the
    full webapp<->client<->realm interaction"""

    def test_add_diff_data(self):
        """add diff data errors"""
        realm = self.realms.get('test')
        realm.add("job 1", 'q0', 'data one')
        self.assertRaises(ValueError,
                realm.add, "job 1", "q0", "data broke")
