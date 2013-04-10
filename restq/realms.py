import time
import json
from collections import OrderedDict
from threading import Lock
from functools import wraps
import os


# The number of seconds a job can be leased for before it can be handed out to
# a new requester
DEFAULT_LEASE_TIME = 60 * 10 

#setup the config path
CONFIG_ROOT = os.path.join(os.path.expanduser("~"), ".restq")
if not os.path.exists(CONFIG_ROOT):
    os.makedirs(CONFIG_ROOT)


# Need to iterator to circle (ring) around the ordered dict but enable a revert
# of the last yielded object #to ensure our queue does not loose its order/time
# consistency
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
JOB_TASKS = 1
JOB_QUEUES = 2

class Work:
    def __init__(self, realm):
        self.realm = realm 
        self.queues = {}
        self.queue_iter = {}
        self.queue_lease_time = {}
        self.tasks = {}
        self.jobs = {}
        self.lock = Lock()
        self.config_path = os.path.join(CONFIG_ROOT, realm)
        self._load_config()

    @serialise
    def remove(self, job_id):
        """remove job_id from the system"""
        #remove the job
        job = self.jobs.pop(job_id)
        
        #clean out the queues
        for queue_id in job[JOB_QUEUES]:
            queue = self.queues[queue_id]
            queue.pop(job_id)

        #clean out the tasks 
        for task_id in job[JOB_TASKS]:
            task = self.tasks[task_id]
            task.remove(job_id)
            if not task:
                self.tasks.pop(task_id)

    @serialise
    def add(self, job_id, task_id, queue_id, data):
        """store a job into a queue"""
        #store our job 
        job = self.jobs.get(job_id, None)
        if job is None:
            job = (data, set(), set())
            self.jobs[job_id] = job
        else:
            if data != job[JOB_DATA]:
                msg = "old job entry and new data != old data (%s)" % (job_id)
                raise ValueError(msg)

        #update the job with a record of it existing in this task and queue
        job[JOB_TASKS].add(task_id)
        job[JOB_QUEUES].add(queue_id)

        #add this job to the queue
        queue = self.queues.get(queue_id, None)
        if queue is None:
            #create a new queue
            queue = self._create_queue(queue_id, DEFAULT_LEASE_TIME)
            self._save_config()
        
        #if the job is not in the queue, add it and initialise the checkout time
        if job_id not in queue:
            queue[job_id] = 0
       
        #add this job to the specified task
        task = self.tasks.get(task_id, None)
        if task is None:
            task = set()
            self.tasks[task_id] = task
        task.add(job_id)
        
    @serialise
    def pull(self, count):
        """pull out a max of count jobs"""
        queues_ids = self.queues.keys()
        queues_ids.sort()
        jobs = [] 
        for queue_id in self.queues:
            #skip queues that have no jobs
            if not self.queues[queue_id]:
                continue 
            
            #get our queue iterator
            iterator = self.queue_iter[queue_id]
            
            #pull a max of count jobs from the queue
            while True:
                job_id, dequeue_time = iterator.next()
                print job_id
                
                #add this job to the jobs result dict and update its lease time 
                ctime = time.time()
                if ctime - dequeue_time > self.queue_lease_time[queue_id]:
                    self.queues[queue_id][job_id] = ctime
                    jobs.append((job_id, self.jobs[job_id][JOB_DATA]))
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
        return dict(total_jobs = len(self.jobs),
             total_tasks = len(self.tasks),
             queues = queue_status)

    @serialise
    def set_lease_time(self, queue_id, lease_time):
        """set the lease time for the given queue_id"""
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
        for queue_id, lease_time in config['queues']:
            self._create_queue(queue_id, lease_time)

    def _save_config(self):
        config = dict(queues=[])
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

def get(realm):
    work = _realms.get(realm, None)
    if work is None:
        work = Work(realm)
        _realms[realm] = work
    return work

def get_status():
    status = {}
    for realm, work in _realms.iteritems():
        status[realm] = work.status
    return status


