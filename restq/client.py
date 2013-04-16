import json
import requests


class Realm(object):
    def __init__(self, realm, url):
        self._realm = realm
        self._url = url

    def __str__(self):
        return str(self.status)

    def remove(self, job_id):
        url = "%s%s/job/%s" % (self._url, self._realm, job_id)
        requests.delete(url)

    def add(self, job_id, task_id, queue_id, data):
        url = "%s%s/job/%s" % (self._url, self._realm, job_id)
        body = json.dumps({'task_id':task_id,
                           'queue_id':queue_id,
                           'data':data})
        requests.put(url, data=body)

    def pull(self, count=5):
        url = "%s%s/job?count=%s" % (self._url, self._realm, count)
        return requests.get(url).json()
   
    @property
    def status(self):
        url = "%s%s/status" % (self._url, self._realm)
        return requests.get(url).json()


class Realms(object):
    def __init__(self, url='http://localhost:8080/'):
        if not url.endswith('/'):
            url += '/'
        self._url = url
        self._special = set(['realms', 'keys', 'items', 'values', 'get', 
                            'trait_names'])
        self.realms

    @property
    def realms(self):
        realms = requests.get(self._url).json()
        for k in realms:
            realm = self.__dict__.get(k, None)
            if realm is None:
                self.__dict__[k] = Realm(k, self._url)
            realms[k] = realm
        return realms

    def keys(self):
        return self.realms.keys()

    def items(self):
        return self.realms.items()

    def values(self):
        return self.realms.values()

    def __iter__(self):
        return self.realms.__iter__() 

    def __contains__(self, k):
        return k in sel.realms

    def get(self, k):
        return self.__getattribute__(k)

    def __str__(self):
        status = {}
        for name, realm in self.items():
            status[name] = realm.status
        return str(status)

    def __getattribute__(self, k):
        if k.startswith('_') or k in self._special:
            return object.__getattribute__(self, k)
        realm = self.__dict__.get(k, None)
        if realm is None:
            realm = Realm(k, self._url)
            self.__dict__[k] = realm
        return realm


