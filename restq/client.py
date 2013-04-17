import json
import requests


class Realm(object):
    def __init__(self, realm, url):
        self._realm = realm
        self._url = url

    def __str__(self):
        return str(self.status)

    def remove_job(self, job_id):
        url = "%s%s/job/%s" % (self._url, self._realm, job_id)
        requests.delete(url)

    def remove_task(self, task_id):
        url = "%s%s/task/%s" % (self._url, self._realm, task_id)
        requests.delete(url)

    def remove_project(self, project_id):
        url = "%s%s/project/%s" % (self._url, self._realm, project_id)
        requests.delete(url)

    def get_job_state(self, job_id):
        url = "%s%s/job/%s" % (self._url, self._realm, job_id)
        requests.get(url)

    def get_task_state(self, task_id):
        url = "%s%s/task/%s" % (self._url, self._realm, task_id)
        requests.get(url)

    def get_project_state(self, project_id):
        url = "%s%s/project/%s" % (self._url, self._realm, project_id)
        requests.get(url)

    def set_default_lease_time(self, lease_time):
        pass #TODO

    def set_queue_lease_time(self, queue_id, lease_time):
        pass #TODO

    def add(self, job_id, queue_id, data, project_id=None, task_id=None):
        """

        """
        url = "%s%s/job/%s" % (self._url, self._realm, job_id)
        data = {'task_id':task_id,
                'queue_id':queue_id,
                'data':data}
        if project_id is not None:
            data['project_id'] = project_id
        if task_id is not None:
            data['task_id'] = task_id
        data = json.dumps(data)
        requests.put(url, data=data)

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


