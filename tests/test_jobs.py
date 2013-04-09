import unittest
from pprint import pprint

from restq import Jobs



class test_jobs(unittest.TestCase):
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




