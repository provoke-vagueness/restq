import json
import requests


class Realm(object):
    def __init__(self, realm, url, requester=requests):
        self.requester = requester
        self._realm = realm
        self._url = url

    def __str__(self):
        return str(self.status)

    def remove_job(self, job_id):
        url = "%s%s/job/%s" % (self._url, self._realm, job_id)
        self.requester.delete(url)

    def remove_tagged_jobs(self, tag_id):
        url = "%s%s/tag/%s" % (self._url, self._realm, tag_id)
        self.requester.delete(url)

    def get_job(self, job_id):
        url = "%s%s/job/%s" % (self._url, self._realm, job_id)
        return self.requester.get(url)

    def get_tagged_jobs(self, tag_id):
        url = "%s%s/tag/%s" % (self._url, self._realm, tag_id)
        return self.requester.get(url)

    def set_default_lease_time(self, lease_time):
        url = "%s%s/config" % (self._url, self._realm)
        data = {'default_lease_time':lease_time}
        self.requester.post(url, data=json.dumps(data))

    def set_queue_lease_time(self, queue_id, lease_time):
        url = "%s%s/config" % (self._url, self._realm)
        data = {'queue_lease_time':[queue_id, lease_time]}
        self.requester.post(url, data=json.dumps(data))

    def add(self, job_id, queue_id, data, tags=None):
        """

        """
        url = "%s%s/job/%s" % (self._url, self._realm, job_id)
        data = {'queue_id':queue_id,
                'data':data}
        if tags is not None:
            data['tags'] = tags
        data = json.dumps(data)
        self.requester.put(url, data=data)

    def pull(self, count=5):
        url = "%s%s/job?count=%s" % (self._url, self._realm, count)
        return self.requester.get(url).json()
   
    @property
    def status(self):
        url = "%s%s/status" % (self._url, self._realm)
        return self.requester.get(url).json()


class Realms(object):
    def __init__(self, url='http://localhost:8080/',
                       requester=requests):
        if not url.endswith('/'):
            url += '/'
        self.requester = requester
        self._url = url
        self._special = set(['realms', 'keys', 'items', 'values', 'get', 
                            'trait_names'])
        self.realms

    @property
    def realms(self):
        realms = self.requester.get(self._url).json()
        for k in realms:
            realm = self.__dict__.get(k, None)
            if realm is None:
                self.__dict__[k] = Realm(k, self._url, 
                                         requester=self.requester)
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

    def __contains__(self, name):
        return name in sel.realms

    def get(self, name):
        return self.__getattribute__(name)

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
            realm = Realm(k, self._url, requester=self.requester)
            self.__dict__[k] = realm
        return realm

