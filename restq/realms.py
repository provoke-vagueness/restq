import time
import json
from collections import OrderedDict
from threading import Lock
from functools import wraps
import os
import pprint


# The number of seconds a job can be leased for before it can be handed out to
# a new requester
DEFAULT_LEASE_TIME = 60 * 10 


#setup the config path
CONFIG_ROOT = os.path.join(os.path.expanduser("~"), ".restq")
if not os.path.exists(CONFIG_ROOT):
    os.makedirs(CONFIG_ROOT)


# Need a circular (ring) iterator to walk the ordered dict and we also need to 
# be able to revert a yielded object in some circumstances...  This facility
# makes it possible to keep our queue order and fairness
class QueueIterator:
    def __init__(self, queue):
        self._queue = queue
        self._count = 0
        self._iter = self._queue.iteritems()

    def next(self):
        assert self._queue
        while True:
            try:
                obj = self._iter.next()
                self._count += 1
                break
            except StopIteration:
                self._iter = self._queue.iteritems()
        return obj

    def revert(self):
        self._count -= 1
        self._iter = self._queue.iteritems()
        index = len(self._queue) % self._count
        while index > 0:
            self._iter.next()
            index -= 1


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
        self.queue_iter = {}
        self.queue_lease_time = {}
        self.default_lease_time = DEFAULT_LEASE_TIME
        self.tags = {}
        self.jobs = {}
        self.lock = Lock()
        self.config_path = os.path.join(CONFIG_ROOT, realm_id + ".realm")
        self._load_config()
   
    @serialise
    def remove_job(self, job_id):
        """remove job_id from the system"""
        self._remove_job(job_id)

    def _remove_job(self, job_id):
        # remove the job
        job = self.jobs.pop(job_id)
        
        # remove from queues
        for queue_id in job[JOB_QUEUES]:
            queue = self.queues[queue_id]
            queue.pop(job_id)

        # remove from tags
        for tag_id in job[JOB_TAGS]:
            tag = self.tags[tag_id]
            tag.remove(job_id)
            if not tag:
                self.tags.pop(tag_id)

    @serialise
    def remove_tagged_jobs(self, tag_id):
        """remove all jobs related to this tag_id"""
        tag = self.tags[tag_id]
        for job_id in [i for i in tag]:
            self._remove_job(job_id)

    @serialise 
    def get_job(self, job_id):
        """return the status of a job"""
        return self._get_job(job_id)
    def _get_job(self, job_id):
        job = self.jobs[job_id]
        status = {'tags':list(job[JOB_TAGS]), 
                  'data':job[JOB_DATA],
                  'queues':[]}
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
        """store a job into a queue
        
        kwargs:
           job_id 
           queue_id
        optional :
           data
           project_id
           task_id
        """
        #store our job 
        job = self.jobs.get(job_id, None)
        if job is None:
            job = (data, set(), set())
            self.jobs[job_id] = job
        else:
            if data != job[JOB_DATA]:
                msg = "old job entry and new data != old data (%s)" % (job_id)
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
    def pull(self, count):
        """pull out a max of count jobs"""
        queues_ids = self.queues.keys()
        queues_ids.sort()
        jobs = {} 
        for queue_id in queues_ids:
            #skip queues that have no jobs
            if not self.queues[queue_id]:
                continue 
            
            #get our queue iterator
            iterator = self.queue_iter[queue_id]
            
            #pull a max of count jobs from the queue
            while True:
                job_id, dequeue_time = iterator.next()
                
                #add this job to the jobs result dict and update its lease time 
                ctime = time.time()
                if ctime - dequeue_time > self.queue_lease_time[queue_id]:
                    self.queues[queue_id][job_id] = ctime
                    jobs[job_id] = (queue_id, self.jobs[job_id][JOB_DATA])
                    if len(jobs) >= count:
                        return jobs
                else:
                    #iff the queues maintain insert order and the iteration
                    #sequence walks the queue on that insert order we can 
                    #rely on the lease time being consistent such that all jobs
                    #following this point will also met this same condition
                    # -> go to next queue
                    iterator.revert()
                    break
        return jobs 

    @property
    def status(self):
        """return the status of the indexes"""
        queue_status = {}
        for key in self.queues:
            queue_status[key] = len(self.queues[key])
        return dict(
             total_jobs = len(self.jobs),
             total_tags = len(self.tags),
             queues = queue_status)

    def get_tag_status(self, tag_id):
        """ return the count of jobs tagged by tag_id """
        tag = self.tags.get(tag_id, None)
        if tag is None:
            return {'count':None}
        else:
            return {'count':len(tag)}

    @serialise
    def set_queue_lease_time(self, queue_id, lease_time):
        """set the lease time for the given queue_id"""
        self._set_queue_lease_time(queue_id, lease_time)

    @serialise 
    def set_default_lease_time(self, lease_time):
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
        if not os.path.exists(self.config_path):
            self._save_config()
            return
        with open(self.config_path, 'rb') as f:
            config = json.loads(f.read())
        self.default_lease_time = config.get('default_lease_time',
                DEFAULT_LEASE_TIME)
        for queue_id, lease_time in config['queues']:
            self._create_queue(queue_id, lease_time)

    def _save_config(self):
        config = dict(queues=[],
                      default_lease_time=self.default_lease_time)
        for queue_id in self.queues:
            config['queues'].append((queue_id, self.queue_lease_time[queue_id]))
        with open(self.config_path, 'wb') as f:
            f.write(json.dumps(config))

    def _create_queue(self, queue_id, lease_time):
        queue = OrderedDict()
        self.queues[queue_id] = queue
        self.queue_iter[queue_id] = QueueIterator(queue)
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


def delete(realm_id):
    """delete the realm at realm_id and remove the associated config file"""
    realm = _realms.pop(realm_id, None)
    if realm is not None:
        try:
            os.remove(realm.config_path)
        except OSError:
            pass


for filename in os.listdir(CONFIG_ROOT):  
    realm_id, ext = os.path.splitext(filename)
    if ext == '.realm':
        get(realm_id)


def get_status():
    """pull back the status for each realm
    
        returns {'realm_id':realm.status}
    """
    status = {}
    for realm_id, realm in _realms.iteritems():
        status[realm_id] = realm.status
    return status


