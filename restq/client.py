import json
import requests
import sys

if sys.version_info[0] < 3:
    builtins = __builtins__
else:
    import builtins 

from restq import realms
from restq import config


class Realm(object):
    def __init__(self, name, uri, requester=requests):
        self.requester = requester
        self._name = name
        self._uri = uri
        self._bulk_jobs = []

    def __str__(self):
        return str(self.status)

    def request(self, rtype, *args, **kwargs):
        func = getattr(self.requester, rtype)
        r = func(*args, **kwargs)
        content_type = r.headers.get('content-type', None)
        if content_type != 'application/json':
            raise Exception(
                    "content-type!=application/json got %s(%s) %s\n'%s'" %\
                        (content_type, r.status_code, r.url, r.text))
        if not r.ok:
            try:
                out = r.json()
            except Exception:
                out = {}
            etype = out.get('exception', 'Exception')
            if isinstance(builtins, dict):
                eclass = builtins.get(etype, 'Exception')
            else:
                eclass = getattr(builtins, etype, 'Exception')
            raise eclass(out.get('message', 'status: %s' % r.status_code))
        try:
            out = r.json()
        except Exception:
            raise Exception("Failed to decode response after a 200 response")
        return out

    def remove_job(self, job_id):
        uri = "%s%s/job/%s" % (self._uri, self._name, job_id)
        self.request('delete', uri)
    remove_job.__doc__ = realms.Realm.remove_job.__doc__

    def remove_tagged_jobs(self, tag_id):
        uri = "%s%s/tag/%s" % (self._uri, self._name, tag_id)
        self.request('delete', uri)
    remove_tagged_jobs.__doc__ = realms.Realm.remove_tagged_jobs.__doc__

    def __getitem__(self, job_id):
        return self.get_job(job_id)

    def get_job(self, job_id):
        uri = "%s%s/job/%s" % (self._uri, self._name, job_id)
        return self.request('get', uri)
    get_job.__doc__ = realms.Realm.get_job.__doc__

    def get_tagged_jobs(self, tag_id):
        uri = "%s%s/tag/%s" % (self._uri, self._name, tag_id)
        return self.request('get', uri)
    get_tagged_jobs.__doc__ = realms.Realm.get_tagged_jobs.__doc__

    def set_default_lease_time(self, lease_time):
        uri = "%s%s/config" % (self._uri, self._name)
        data = {'default_lease_time':lease_time}
        self.request('post', uri, data=json.dumps(data))
    set_default_lease_time.__doc__ = realms.Realm.set_default_lease_time.__doc__

    def set_queue_lease_time(self, queue_id, lease_time):
        uri = "%s%s/config" % (self._uri, self._name)
        data = {'queue_lease_time':[queue_id, lease_time]}
        self.request('post', uri, data=json.dumps(data))
    set_queue_lease_time.__doc__ = realms.Realm.set_queue_lease_time.__doc__

    def add(self, job_id, queue_id, data=None, tags=None):
        uri = "%s%s/job/%s" % (self._uri, self._name, job_id)
        body = {'queue_id':queue_id}
        if data is not None:
            body['data'] = data
        if tags is not None:
            body['tags'] = tags
        body = json.dumps(body)
        self.request('put', uri, data=body)
    add.__doc__ = realms.Realm.add.__doc__ 
    
    def bulk_add(self, job_id, queue_id, data=None, tags=None):
        """add jobs in bulk.
        
        Note: once you have added jobs through this interface, you are
            required to call bulk_flush to transmit the jobs in bulk to the
            web service.
        """
        job = {'job_id':job_id, 'queue_id': queue_id}
        if data is not None:
            job['data'] = data
        if tags is not None:
            job['tags'] = tags
        self._bulk_jobs.append(job)

    def bulk_flush(self):
        """flush jobs through to the web service that have been queued up using
        the bulk_add function"""
        if not self._bulk_jobs:
            return
        uri= "%s%s/job" % (self._uri, self._name)
        body = {'jobs': self._bulk_jobs}
        body = json.dumps(body)
        self.request('post', uri, data=body)
        self._bulk_jobs = []

    def pull(self, count=None):
        if count is None:
            count = config.client['count']
        uri = "%s%s/job?count=%s" % (self._uri, self._name, count)
        return self.request('get', uri)
    pull.__doc__ = realms.Realm.pull.__doc__

    def get_tag_status(self, tag_id):
        uri = "%s%s/tag/%s/status" % (self._uri, self._name, tag_id)
        return self.request('get', uri)
    get_tag_status.__doc__ = realms.Realm.get_tag_status.__doc__

    @property
    def status(self):
        uri = "%s%s/status" % (self._uri, self._name)
        return self.request('get', uri)

    @property
    def name(self):
        return self._name


class Realms(object):
    def __init__(self, uri=None,
                       requester=requests):
        if uri is None:
            uri = config.client['uri']
        if not uri.endswith('/'):
            uri += '/'
        self.requester = requester
        self._uri = uri
        self._special = set(['realms', 'keys', 'items', 'values', 'get', 
                            'trait_names'])
        self.realms

    @property
    def realms(self):
        realms = self.requester.get(self._uri).json()
        for k in realms:
            realm = self.__dict__.get(k, None)
            if realm is None:
                self.__dict__[k] = Realm(k, self._uri, 
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

    def __getitem__(self, key):
        return getattr(self, key)

    def __contains__(self, name):
        return name in self.realms

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
            realm = Realm(k, self._uri, requester=self.requester)
            self.__dict__[k] = realm
        return realm

