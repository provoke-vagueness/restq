import unittest
from pprint import pprint
import time
import os

from restq import realms 


class TestJobs(unittest.TestCase):

    def setUp(self):
        try:
            os.remove(os.path.join(realms.CONFIG_ROOT, 'test.realm'))
            reload(realms)
            realms.DEFAULT_LEASE_TIME = 0.5
        except OSError:
            pass

    def test_add(self):
        """add data"""
        realm = realms.get('test')
        realm.add(0, 'q0', 'h', project_id='project 1', task_id='task 1')

        status = realm.status
        self.assertEqual(len(status['queues']), 1)
        self.assertEqual(status['total_jobs'], 1)
        self.assertEqual(status['total_tasks'], 1)
        self.assertEqual(status['total_projects'], 1)

        realm.add(1, 'q0', None, project_id='project 1', task_id='task 1')
        status = realm.status
        self.assertEqual(len(status['queues']), 1)
        self.assertEqual(status['total_jobs'], 2)
        self.assertEqual(status['total_tasks'], 1)
        self.assertEqual(status['total_projects'], 1)
 
        realm.add(2, 'q0', 443434, project_id='project 1', task_id='task 2')
        status = realm.status
        self.assertEqual(len(status['queues']), 1)
        self.assertEqual(status['total_jobs'], 3)
        self.assertEqual(status['total_tasks'], 2)
        self.assertEqual(status['total_projects'], 1)

        realm.add(2, 'q0', 443434, project_id='project 2', task_id='task 2')
        status = realm.status
        self.assertEqual(len(status['queues']), 1)
        self.assertEqual(status['total_jobs'], 3)
        self.assertEqual(status['total_tasks'], 2)
        self.assertEqual(status['total_projects'], 2)

        realm.add(3, 'q1', 3343.343434, project_id='project 2', 
                        task_id='task 2')
        status = realm.status
        self.assertEqual(len(status['queues']), 2)
        self.assertEqual(status['total_jobs'], 4)
        self.assertEqual(status['total_tasks'], 2)
        self.assertEqual(status['total_projects'], 2)


    def test_remove_job(self):
        realm = realms.get('test')
        realm.add("job 1", 'q0', 'h', project_id='project 1', task_id='task 1')
        realm.add("job 1", 'q0', 'h', project_id='project 1', task_id='task 1')
        realm.add("job 1", 'q1', 'h', project_id='project 2', task_id='task 1')
        realm.add("job 2", 'q0', 'h', project_id='project 1', task_id='task 2')

#       print ("Before removing job 1:")
#       pprint(realm.tasks)
#       pprint(realm.projects)
#       pprint(realm.jobs)
#       pprint(realm.queues)

        realm.remove_job("job 2")

#       print ("After removing job 1:")
#       pprint(realm.tasks)
#       pprint(realm.projects)
#       pprint(realm.jobs)
#       pprint(realm.queues)

        status = realm.status
        self.assertEqual(len(status['queues']), 2)
        self.assertEqual(status['total_jobs'], 1)
        self.assertEqual(status['total_tasks'], 1)
        self.assertEqual(status['total_projects'], 2)


    def test_remove_task(self):
        realm = realms.get('test')
        realm.add("job 1", 'q0', 'h', project_id='project 1', task_id='task 1')
        realm.add("job 1", 'q0', 'h', project_id='project 1', task_id='task 1')
        realm.add("job 1", 'q1', 'h', project_id='project 2', task_id='task 1')
        realm.add("job 2", 'q0', 'h', project_id='project 1', task_id='task 2')

  #     print ("Before removing job 1:")
  #     pprint(realm.tasks)
  #     pprint(realm.projects)
  #     pprint(realm.jobs)
  #     pprint(realm.queues)

        realm.remove_task("task 1")

  #     print ("After removing task 1:")
  #     pprint(realm.tasks)
  #     pprint(realm.projects)
  #     pprint(realm.jobs)
  #     pprint(realm.queues)

        status = realm.status
        self.assertEqual(len(status['queues']), 2)
        self.assertEqual(status['total_jobs'], 1)
        self.assertEqual(status['total_tasks'], 1)
        self.assertEqual(status['total_projects'], 1)

    def test_remove_project(self):
        realm = realms.get('test')
        realm.add("job 1", 'q0', 'h', project_id='project 1', task_id='task 1')
        realm.add("job 1", 'q0', 'h', project_id='project 1', task_id='task 1')
        realm.add("job 2", 'q1', 'h', project_id='project 2', task_id='task 1')
        realm.add("job 1", 'q0', 'h', project_id='project 1', task_id='task 2')


        print ("Before removing project 1:")
        pprint(realm.tasks)
        pprint(realm.projects)
        pprint(realm.jobs)
        pprint(realm.queues)

        realm.remove_project("project 1")

        print ("After removing project 1:")
        pprint(realm.tasks)
        pprint(realm.projects)
        pprint(realm.jobs)
        pprint(realm.queues)

        status = realm.status
        self.assertEqual(len(status['queues']), 2)
        self.assertEqual(status['total_jobs'], 1)
        self.assertEqual(status['total_tasks'], 1)
        self.assertEqual(status['total_projects'], 1)

    def test_get_state(self):
        realm = realms.get('test')
        realm.add("job 1", 'q0', 'h', project_id='project 1', task_id='task 1')
        realm.add("job 1", 'q0', 'h', project_id='project 1', task_id='task 1')
        realm.add("job 1", 'q1', 'h', project_id='project 2', task_id='task 1')
        realm.add("job 2", 'q0', 'h', project_id='project 1', task_id='task 2')

        state = realm.get_job_state("job 1")
        pprint(state)
        state = realm.get_task_state("task 1")
        pprint(state)
        state = realm.get_project_state("project 1")
        pprint(state)

    def test_add_diff_data(self):
        """add diff data errors"""
        realm = realms.get('test')
        realm.add("job 1", 'q0', 'data one')
        self.assertRaises(ValueError,
                realm.add, "job 1", "q0", "data broke")

    def test_pull(self):
        realm = realms.get('test')
        realm.add("job 0", "q0", 'h')
        realm.add("job 1", "q0", None)
        realm.add("job 2", "q0", 443434)
        realm.add("job 3", "q1", 3343.343434)
        realmer = realm.pull(4)
        pprint(realmer)
        realmer = dict(realmer)
        self.assertEqual(len(realmer), 4)
        self.assertEqual(realmer["job 3"][1], 3343.343434)

        #make sure there are no more realm available because they should be
        # checked out with the previous pull request
        realmer = realm.pull(4)
        pprint(realmer)
        self.assertFalse(realmer)

        #now that the least time has expired, lets make sure we can check out 
        # the realm once again
        time.sleep(1)        
        realmer = realm.pull(4)
        pprint(realmer)
        realmer = dict(realmer)
        self.assertEqual(len(realmer), 4)
        self.assertEqual(realmer["job 1"][1], None)

        #again, make sure the realm are all checked out
        realmer = realm.pull(4)
        pprint(realmer)
        self.assertFalse(realmer)

        #make sure we can checkout one job, wait until it will be 
        # checked back in, but when we checkout the next job, we should 
        # increment to the next job in the queue
        time.sleep(1)        
        realmer = realm.pull(1)
        pprint(realmer)
        realmer = dict(realmer)
        self.assertEqual(realmer["job 0"][1], 'h')



