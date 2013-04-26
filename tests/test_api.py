import unittest
from pprint import pprint
import time
import os
import json

import  webtest

from restq import webapp
from restq import realms

#change the lease time to 1 second while we run tests
realms.DEFAULT_LEASE_TIME = 0.5


class TestApi(unittest.TestCase):
    
    def setUp(self):
        self.app = webtest.TestApp(webapp.app)

    def test_add_and_pull(self):
        #add 100 jobs to the a test realm and then verify their data
        for i in xrange(0, 100):
            testdata = json.dumps(dict(task_id="testing", queue_id=1, data=i))
            resp = self.app.put("/realm/job/%d" % i, testdata)
            self.assertEquals(resp.status_int, 200)

        #on the first 50 jobs, test pulling jobs in order
        for i in xrange(0, 50):
            resp = self.app.get("/realm/job")
            self.assertEquals(resp.status_int, 200)
            body = json.loads(resp.body)
            for k,v in body.iteritems():
                self.assertEquals([1, int(k)], v)

            #remove the job
            resp = self.app.delete("/realm/job/%d" % i)
            self.assertEquals(resp.status_int, 200)
            
        #on the next 50 jobs, test pull in one hit
        resp = self.app.get("/realm/job?count=50")
        self.assertEquals(resp.status_int, 200)
        body = json.loads(resp.body)
        for k,v in body.iteritems():
            self.assertEquals([1,int(k)], v)

        #remove the remaining jobs
        for i in xrange(50, 100):
            resp = self.app.delete("/realm/job/%d" % i)
            self.assertEquals(resp.status_int, 200)
