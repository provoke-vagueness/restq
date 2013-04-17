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
        projects = {'project 1':['task 1']}
        realm.add(projects, 0, 'q0', 'h')

        status = realm.status
        self.assertEqual(len(status['queues']), 1)
        self.assertEqual(status['total_jobs'], 1)
        self.assertEqual(status['total_tasks'], 1)
        self.assertEqual(status['total_projects'], 1)

        realm.add(projects, 1, 'q0', None)
        status = realm.status
        self.assertEqual(len(status['queues']), 1)
        self.assertEqual(status['total_jobs'], 2)
        self.assertEqual(status['total_tasks'], 1)
        self.assertEqual(status['total_projects'], 1)
 
        projects = {'project 1':['task 2',]}
        realm.add(projects, 2, 'q0', 443434)
        status = realm.status
        self.assertEqual(len(status['queues']), 1)
        self.assertEqual(status['total_jobs'], 3)
        self.assertEqual(status['total_tasks'], 2)
        self.assertEqual(status['total_projects'], 1)

        projects = {'project 2':['task 2',]}
        realm.add(projects, 2, 'q0', 443434)
        status = realm.status
        self.assertEqual(len(status['queues']), 1)
        self.assertEqual(status['total_jobs'], 3)
        self.assertEqual(status['total_tasks'], 2)
        self.assertEqual(status['total_projects'], 2)

        realm.add(projects, 3, 'q1', 3343.343434)
        status = realm.status
        self.assertEqual(len(status['queues']), 2)
        self.assertEqual(status['total_jobs'], 4)
        self.assertEqual(status['total_tasks'], 2)
        self.assertEqual(status['total_projects'], 2)

#       pprint(realm.tasks)
#       pprint(realm.projects)
#       pprint(realm.jobs)
#       pprint(realm.queues)


    def test_remove_job(self):
        realm = realms.get('test')
        projects = {'project 1':['task 1']}
        realm.add(projects, "job 1", 'q0', 'h')
        projects = {'project 1':['task 1']}
        realm.add(projects, "job 1", 'q0', 'h')
        
        projects = {'project 2':['task 1']}
        realm.add(projects,  "job 1", 'q1', 'h')

        projects = {'project 1':['task 2']}
        realm.add(projects,  "job 2", 'q0', 'h')

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
        projects = {'project 1':['task 1']}
        realm.add(projects, "job 1", 'q0', 'h')
        projects = {'project 1':['task 1']}
        realm.add(projects, "job 1", 'q0', 'h')
        
        projects = {'project 2':['task 1']}
        realm.add(projects,  "job 1", 'q1', 'h')

        projects = {'project 1':['task 2']}
        realm.add(projects,  "job 2", 'q0', 'h')

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
        projects = {'project 1':['task 1']}
        realm.add(projects, "job 1", 'q0', 'h')
        projects = {'project 1':['task 1']}
        realm.add(projects, "job 1", 'q0', 'h')
        
        projects = {'project 2':['task 1']}
        realm.add(projects,  "job 1", 'q1', 'h')
        realm.add(projects,  "job 3", 'q1', 'h')

        projects = {'project 1':['task 2']}
        realm.add(projects,  "job 2", 'q0', 'h')

        print ("Before removing job 1:")
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


    def test_add_diff_data(self):
        """add diff data errors"""
        realm = realms.get('test')
        projects = {'project 1':['task 1']}
        realm.add(projects, "job 1", 'q0', 'data one')
        self.assertRaises(ValueError,
                realm.add, projects, "job 1", "q0", "data broke")


    def test_pull(self):
        """test that we can pull realm"""
        realm = realms.get('test')
        projects = {'project 1':['task 1']}
        realm.add(projects, "job 0", 0, 'h')
        realm.add(projects, "job 1", 0, None)
        realm.add(projects, "job 2", 0, 443434)
        realm.add(projects, "job 3", 1, 3343.343434)
        realmer = realm.pull(4)
        pprint(realmer)
        self.assertEqual(len(realmer), 4)
        self.assertEqual(realmer["job 3"], 3343.343434)

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
        self.assertEqual(len(realmer), 4)
        self.assertEqual(realmer["job 1"], None)

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
        self.assertEqual(realmer["job 0"], 'h')
        time.sleep(1)



