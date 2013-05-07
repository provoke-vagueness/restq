import unittest
from pprint import pprint
import time
import os

import webtest

import test_realms
from restq import webapp
from restq import client



def hooker(func):
    def decorate(*a, **k):
        class Res(object):
            def __init__(self, res):
                self._res = res

            def __getattribute__(self, k):
                res = object.__getattribute__(self, '_res')
                if k == 'json':
                    return lambda: res.json
                if k == 'ok':
                    return self.status_code == 200
                return getattr(res, k)
        data = k.pop('data', None)
        if data is not None:
            a = list(a)
            a.append(data)
        return Res(func(*a, **k))
    return decorate


class Requester():
    """Make sure the webtest.TestApp behaves like the "requests" library."""

    def __init__(self):
        self.app = webtest.TestApp(webapp.app)

    @hooker
    def put(self, *a, **k):
        return self.app.put(*a, **k)

    @hooker
    def delete(self, *a, **k):
        return self.app.delete(*a, **k)

    @hooker
    def post(self, *a, **k): 
        return self.app.post(*a, **k)

    @hooker
    def get(self, *a, **k): 
        return self.app.get(*a, **k)




class TestClient(test_realms.TestRealms):

    def setUp(self):
        test_realms.TestRealms.setUp(self)
        requester = Requester()
        self.realms = client.Realms(uri='', requester=requester)
    










