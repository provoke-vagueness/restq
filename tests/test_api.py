import unittest
from pprint import pprint
import time
import os

import  webtest

from restq import webapp
from restq import realms

#change the lease time to 1 second while we run tests
realms.DEFAULT_LEASE_TIME = 0.5


class TestApi(unittest.TestCase):
    
    def setUp(self):
        self.app = webtest.TestApp(webapp.app)

    def test_doc(self):
        resp = self.app.get('/')
        self.assertEquals(resp.status.lower(), "200 ok")
        self.assertEquals(resp.status_int, 200)
        self.assertTrue("<!DOCTYPE html>" in resp)
        
