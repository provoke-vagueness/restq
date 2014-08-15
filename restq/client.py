import json
import requests
import sys
from collections import MutableMapping
if sys.version_info[0] < 3:
    builtins = __builtins__
else:
    import builtins

from restq import realms
from restq import config


class BaseClient(object):
    __slots__ = ('requester',)

    def __init__(self, requester=requests):
        self.requester = requester

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

    def bulk_remove(self, jobs):
        """bulk remove

        jobs = [(realm_id, job_id), ...]

        """
        uri = "%s/jobs" % (self._uri)
        body = {'jobs': jobs}
        body = json.dumps(body)
        self.request('delete', uri, data=body)

    def bulk_add(self, jobs):
        """add jobs in bulk.

        jobs = [job, ...]

        Where job is a dictionary of key:values
            {realm_id:
             job_id:
             queue_id:
             # Optional tag values
             data:
             tags:}
        """
        uri = "%s/jobs" % (self._uri)
        body = {'jobs': jobs}
        body = json.dumps(body)
        self.request('post', uri, data=body)


class Realm(BaseClient):
    def __init__(self, name, uri, requester=requests):
        BaseClient.__init__(self, requester)
        self._name = name
        self._uri = "%s/%s" % (uri, name)

    def __str__(self):
        return str(self.status)

    def remove_job(self, job_id):
        uri = "%s/job/%s" % (self._uri, job_id)
        self.request('delete', uri)
    remove_job.__doc__ = realms.Realm.remove_job.__doc__

    def remove_tagged_jobs(self, tag_id):
        uri = "%s/tag/%s" % (self._uri, tag_id)
        self.request('delete', uri)
    remove_tagged_jobs.__doc__ = realms.Realm.remove_tagged_jobs.__doc__

    def __getitem__(self, job_id):
        return self.get_job(job_id)

    def get_job(self, job_id):
        uri = "%s/job/%s" % (self._uri, job_id)
        return self.request('get', uri)
    get_job.__doc__ = realms.Realm.get_job.__doc__

    def get_tagged_jobs(self, tag_id):
        uri = "%s/tag/%s" % (self._uri, tag_id)
        return self.request('get', uri)
    get_tagged_jobs.__doc__ = realms.Realm.get_tagged_jobs.__doc__

    def move_job(self, job_id, from_q, to_q):
        uri = "%s/job/%s/from_q/%s/to_q/%s" % (self._uri, job_id, from_q, to_q)
        return self.request('get', uri)
    move_job.__doc__ = realms.Realm.move_job.__doc__

    def set_default_lease_time(self, lease_time):
        uri = "%s/config" % (self._uri)
        data = {'default_lease_time': lease_time}
        self.request('post', uri, data=json.dumps(data))
    set_default_lease_time.__doc__ = realms.Realm.set_default_lease_time.__doc__

    def set_queue_lease_time(self, queue_id, lease_time):
        uri = "%s/config" % (self._uri)
        data = {'queue_lease_time': [queue_id, lease_time]}
        self.request('post', uri, data=json.dumps(data))
    set_queue_lease_time.__doc__ = realms.Realm.set_queue_lease_time.__doc__

    def clear_queue(self, queue_id):
        uri = "%s/queues/%s/clear" % (self._uri, queue_id)
        self.request('get', uri)
    clear_queue.__doc__ = realms.Realm.clear_queue.__doc__

    def add(self, job_id, queue_id, data=None, tags=None):
        uri = "%s/job/%s" % (self._uri, job_id)
        body = {'queue_id': queue_id}
        if data is not None:
            body['data'] = data
        if tags is not None:
            body['tags'] = tags
        body = json.dumps(body)
        self.request('put', uri, data=body)
    add.__doc__ = realms.Realm.add.__doc__

    def bulk_add(self, jobs):
        for job in jobs.values():
            job['realm_id'] = self._name
        super(Realm, self).bulk_add(jobs)

    def bulk_remove(self, jobs):
        jobs = [(self._name, job) for job in jobs]
        super(Realm, self).bulk_remove(jobs)

    def pull(self, count=None):
        if count is None:
            count = config.client['count']
        uri = "%s/job?count=%s" % (self._uri, count)
        return self.request('get', uri)
    pull.__doc__ = realms.Realm.pull.__doc__

    def get_tag_status(self, tag_id):
        uri = "%s/tag/%s/status" % (self._uri, tag_id)
        return self.request('get', uri)
    get_tag_status.__doc__ = realms.Realm.get_tag_status.__doc__

    @property
    def status(self):
        uri = "%s/status" % (self._uri)
        print(uri)
        return self.request('get', uri)

    @property
    def name(self):
        return self._name


class Realms(MutableMapping, BaseClient):
    __slots__ = ('_reserved',
                 '_uri',
                 '_realms',
                 '_requester')

    def __init__(self, uri=None, requester=requests):
        BaseClient.__init__(self, requester)
        if uri is None:
            uri = config.client['uri']
        if uri.endswith('/'):
            uri = uri[:-1]
        self._realms = None
        self._requester = requester
        self._uri = uri

    @property
    def realms(self):
        if self._realms is None:
            self._realms = {}
            realms = self.request('get', self._uri)
            for k in realms:
                self._realms[k] = Realm(k, self._uri, requester=self._requester)
        return self._realms

    def __dir__(self):
        return list(Realms._reserved) + list(self.realms)

    def __delitem__(self, a):
        self.realms.__delitem__(a)

    def __getitem__(self, a):
        return getattr(self, a)

    def __len__(self):
        return len(self.realms)

    def __iter__(self):
        return self.realms.__iter__()

    def __setitem__(self, a, b):
        raise ValueError("Can not set items against this object")

    def __str__(self):
        status = {}
        for name, realm in self.items():
            status[name] = realm.status
        return str(status)

    def __getattribute__(self, k):
        if k in Realms._reserved:
            return super(Realms, self).__getattribute__(k)
        realm = self.realms.get(k, None)
        if realm is None:
            realm = Realm(k, self._uri, requester=self._requester)
            self.realms[k] = realm
        return realm

Realms._reserved = set(dir(Realms))
