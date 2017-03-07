import os
import pprint
import sys
import time
import yaml

from collections import OrderedDict
from functools import wraps
from itertools import takewhile
from threading import Lock

from restq import config


if sys.version_info[0] < 3:
    dictiter = lambda d: d.iteritems()
else:
    dictiter = lambda d: iter(d.items())
    iternext = lambda i: i.__next__()


# Serialise access to functions of Jobs
def serialise(func):
    @wraps(func)
    def with_serialisation(self, *a, **k):
        with self.lock:
            return func(self, *a, **k)
    return with_serialisation


JOB_DATA = 0
JOB_TAGS = 1
JOB_QUEUES = 2


class Realm:
    def __init__(self, realm_id):
        self.realm_id = realm_id
        self.queues = {}
        self.queue_lease_time = {}
        self.default_lease_time = config.realms['default_lease_time']
        self.tags = {}
        self.jobs = {}
        self.lock = Lock()
        p = os.path.join(config.realms['realms_config_root'],
                            realm_id + ".realm")
        self.realm_config_path = p
        self._load_config()

    def _remove_from_tags(self, job_id):
        """Remove jobs from tags"""
        job = self.jobs.get(job_id, None)
        if job is not None:
            for tag_id in job[JOB_TAGS]:
                tag = self.tags[tag_id]
                tag.remove(job_id)
                if not tag:
                    self.tags.pop(tag_id)

    def _remove_from_queues(self, job_id):
        """Remove job from queues"""
        job = self.jobs.get(job_id, None)
        if job is not None:
            for queue_id in job[JOB_QUEUES]:
                queue = self.queues[queue_id]
                queue.pop(job_id)
            # recreate dict to release mem from large allocations
            if not len(queue):
                self.queues[queue_id] = OrderedDict()

    @serialise
    def remove_job(self, job_id):
        """remove job_id from the system"""
        self._remove_job(job_id)
    def _remove_job(self, job_id):
        # remove from queues
        self._remove_from_queues(job_id)
        # remove from tags
        self._remove_from_tags(job_id)
        # remove the job
        self.jobs.pop(job_id)

    @serialise
    def remove_tagged_jobs(self, tag_id):
        """remove all jobs related to this tag_id"""
        tag = self.tags[tag_id]
        for job_id in [i for i in tag]:
            self._remove_job(job_id)

    @serialise
    def move_job(self, job_id, from_q, to_q):
        """move the job from a queue to another queue"""
        job = self.jobs.get(job_id, None)
        if job is None:
            raise ValueError("Job '%s' does not exist" % job_id)

        if from_q not in self.queues:
            # trying to move from a queue that doesn't exist is bad
            raise ValueError("from_q '%s' doesn't exist" % from_q)

        if job_id not in self.queues[from_q]:
            # job not in from_q, can't move it
            raise ValueError("Job '%s' is not in queue '%s'" % \
                             (job_id, from_q))

        # check if job is checked out
        now = time.time()
        checkout_time = self.queues[from_q][job_id]
        if checkout_time != 0 and \
            now - checkout_time < self.queue_lease_time[from_q]:
            # already checked out, can't do anything
            raise ValueError("Job '%s' queue '%s' is already checked out" % \
                             (job_id, from_q))

        # OK, we can remove from the old queue now
        self.queues[from_q].pop(job_id)
        job[JOB_QUEUES].discard(from_q)

        if to_q in job[JOB_QUEUES]:
            # job is already in to_q, nothing more to do
            return

        # do we need to create the new queue?
        queue = self.queues.get(to_q, None)
        if queue is None:
            # create a new queue
            queue = self._create_queue(to_q, self.default_lease_time)
            self._save_config

        # Now we can add to the new queue
        job[JOB_QUEUES].add(to_q)
        if job_id not in queue:
            queue[job_id] = 0

    @serialise
    def get_job(self, job_id):
        """return the status of a job"""
        return self._get_job(job_id)
    def _get_job(self, job_id):
        job = self.jobs[job_id]
        status = {'tags': list(job[JOB_TAGS]),
                  'data': job[JOB_DATA],
                  'queues': []}
        now = time.time()
        for queue_id in job[JOB_QUEUES]:
            checkout_time = self.queues[queue_id][job_id]
            if checkout_time != 0:
                checkout_time = now - checkout_time
            status['queues'].append((queue_id, checkout_time))
        return status

    @serialise
    def get_tagged_jobs(self, tag_id):
        """return a dict of all jobs tagged by tag_id"""
        jobs = {}
        tag = self.tags[tag_id]
        for job_id in tag:
            jobs[job_id] = self._get_job(job_id)
        return jobs

    @serialise
    def add(self, job_id, queue_id, data=None, tags=[]):
        """store a job into a queue"""
        # store our job
        job = self.jobs.get(job_id, None)
        if job is None:
            job = (data, set(), set())
            self.jobs[job_id] = job
        else:
            if data != job[JOB_DATA]:
                msg = "add of existing job '%s' with data != old data" % \
                        (job_id)
                raise ValueError(msg)

        # update the job's queue record
        job[JOB_QUEUES].add(queue_id)

        # add this job to the queue
        queue = self.queues.get(queue_id, None)
        if queue is None:
            #create a new queue
            queue = self._create_queue(queue_id, self.default_lease_time)
            self._save_config()

        # if the job is not in the queue, add it and init the checkout time
        if job_id not in queue:
            queue[job_id] = 0

        # add tags to jobs and job to tags
        for tag_id in tags:
            job[JOB_TAGS].add(tag_id)
            tag = self.tags.get(tag_id, None)
            if tag is None:
                tag = set()
                self.tags[tag_id] = tag
            tag.add(job_id)

    @serialise
    def pull(self, count, max_queue=None):
        """pull out a max of count jobs"""
        queues_ids = [k for k in self.queues]
        queues_ids.sort()
        jobs = {}
        ctime = time.time()
        pred = lambda x: max_queue is None or x <= max_queue
        for queue_id in takewhile(pred, queues_ids):
            # add this job to the jobs result dict and update its lease time
            for job_id, dequeue_time in dictiter(self.queues[queue_id]):

                # skip any non expired leases
                if ctime - dequeue_time > self.queue_lease_time[queue_id]:
                    self.queues[queue_id][job_id] = ctime
                    jobs[job_id] = (queue_id, self.jobs[job_id][JOB_DATA])
                    if len(jobs) >= count:
                        return jobs
        return jobs

    @serialise
    def clear_queue(self, queue_id):
        """remove all jobs from the given queue"""
        queue = self.queues.get(queue_id, None)
        if queue is None:
            raise ValueError("Queue '%s' does not exist" % queue_id)

        # get a list of all jobs in this queue so we can remove the queue
        # references from them afterwards. We do this first so we can clear the
        # queue in one go and prevent dequeueing during iteration
        job_ids = [job_id for job_id in queue]

        # clear the queue, then remove the queue references from all jobs
        queue.clear()
        for job_id in job_ids:
            job = self.jobs.get(job_id, None)
            if job is None:
                # Job doesn't exist any more, skip
                continue
            queues_job_in = job[JOB_QUEUES]
            queues_job_in.discard(queue_id)
            if len(queues_job_in) == 0:
                # job no longer in any queues, lets remove it completely
                self._remove_from_tags(job_id)
                self.jobs.pop(job_id)

    def queue_names(self):
        """list of current queue names"""
        return self.queues.keys()

    @property
    def status(self):
        """return the status of the indexes"""
        queue_status = {}
        for key in self.queues:
            queue_status[key] = len(self.queues[key])
        return dict(total_jobs=len(self.jobs),
                    total_tags=len(self.tags),
                    queues=queue_status)

    def get_tag_status(self, tag_id):
        """return the count of jobs tagged by tag_id"""
        tag = self.tags[tag_id]
        return {'count': len(tag)}

    @serialise
    def set_queue_lease_time(self, queue_id, lease_time):
        """set the lease time for the given queue_id"""
        self._set_queue_lease_time(queue_id, lease_time)

    @serialise
    def set_default_lease_time(self, lease_time):
        """The number of seconds a job can be leased for before a job can be
        handed out to a new requester"""
        self.default_lease_time = lease_time
        self._save_config()

    def _set_queue_lease_time(self, queue_id, lease_time):
        queue = self.queues.get(queue_id, None)
        if queue is None:
            self._create_queue(queue_id, lease_time)
        else:
            self.queue_lease_time[queue_id] = lease_time
        self._save_config()

    def _load_config(self):
        if not os.path.exists(self.realm_config_path):
            self._save_config()
            return
        with open(self.realm_config_path, 'r') as f:
            realm_config = yaml.load(f)
        self.default_lease_time = realm_config.get('default_lease_time',
                config.realms['default_lease_time'])
        for queue_id, lease_time in realm_config['queues']:
            self._create_queue(queue_id, lease_time)

    def _save_config(self):
        realm_config = dict(queues=[],
                      default_lease_time=self.default_lease_time)
        for queue_id in self.queues:
            realm_config['queues'].append(
                    (queue_id, self.queue_lease_time[queue_id]))
        with open(self.realm_config_path, 'w') as f:
            yaml.dump(realm_config, f, default_flow_style=False)

    def _create_queue(self, queue_id, lease_time):
        queue = OrderedDict()
        self.queues[queue_id] = queue
        self.queue_lease_time[queue_id] = lease_time
        return queue


_realms = dict()
def get(realm_id):
    """return a realm for the given realm_id"""
    realm = _realms.get(realm_id, None)
    if realm is None:
        realm = Realm(realm_id)
        _realms[realm_id] = realm
    return realm

def current():
    return _realms.values()

def delete(realm_id):
    """delete the realm at realm_id and remove the associated config file"""
    realm = _realms.pop(realm_id, None)
    if realm is not None:
        try:
            os.remove(realm.realm_config_path)
        except OSError:
            pass


def set_realms_config_root(config_root):
    global _realms
    _realms = {}

    config.realms['realms_config_root'] = config_root
    if not os.path.exists(config_root):
        os.makedirs(config_root)

    for filename in os.listdir(config_root):
        realm_id, ext = os.path.splitext(filename)
        if ext == '.realm':
            get(realm_id)
set_realms_config_root(config.realms['realms_config_root'])


def get_status():
    """pull back the status for each realm

        returns {'realm_id':realm.status}
    """
    status = {}
    for realm_id, realm in dictiter(_realms):
        status[realm_id] = realm.status
    return status

def pull(count, realms=None):
    """pull next priority jobs across multiple realms"""
    if realms is None:
        realms = current()
    else:
        realms = [get(r) for r in realms]
    queues = list(set(sum([r.queue_names() for r in realms], [])))
    queues.sort()
    jobs = {}
    for q in queues:
        for r in realms:
            for job_id, val in \
                r.pull(max_queue=q, count=count-len(jobs)).iteritems():
                # assumes job_id will be globally unique
                # job_id -> realm, priority, data
                jobs[job_id] = r.realm_id, val[0], val[1]
            if len(jobs) >= count:
                return jobs
    return jobs
