import time
from collections import OrderedDict
from threading import Lock
from functools import wraps
import traceback
import json
import httplib

from bottle import route, run, abort


def serialise(func):
    @wraps(func)
    def with_serialisation(self, *a, **k):
        with self.lock:
            return func(self, *a, **k)
        return with_serialisation

class Jobs:
    def __init__(self):
        self.queues = {}
        self.queue_iter = {}
        self.tasks = {}
        self.jobs = {}
        self.lock = Lock()

    def _get_queue(self, queue_id):
        return queue

    def _get_task(self, task_id):
        return task
    
    @serialise
    def remove(self, job_id):
        #remove the job
        job = self.jobs.pop(job_id)
        
        #clean out the queues
        for queue_id in job['queues']:
            queue = self.queues[queue_id]
            queue.pop(job_id)

        #clean out the tasks 
        for task_id in job['tasks']:
            task = self.tasks[task_id]
            task.remove(job_id)
            if not task:
                self.tasks.pop(task_id)

    @serialise
    def add(self, job_id, task_id, queue_id, params):
        """store a job into a queue"""
        #store our job 
        job = self.jobs.get(job_id, {})
        if not job:
            if params != job['params']:
                msg = "old job entry and new params != old params" % (job_id)
                raise Exception(msg)
        job['tasks'].add(task_id)
        job['queues'].add(queue_id)
        job['params'] = params 

        #add this job to the queue
        queue = self.queues.get(queue_id, None)
        if queue is None:
            queue = OrderedDict()
            self.jobs[queue_id] = queue
        if job_id not in queue:
            queue[job_id] = 0
       
        #add this job to the specified task
        task = self.tasks.get(task_id, None)
        if task is None:
            task = set()
            self.tasks[task_id] = task
        task.add(job_id)
        
    @serialise
    def pull(self, count=1):
        """pull out a max of count jobs"""
        queues_ids = self.queues.keys()
        queues_ids.sort()
        jobs = {}
        for queue_id in queues:
            #skip queues that have no jobs
            if not self.queues[queue_id]:
                continue 
            
            #recover our previous iterator
            iteritems = self.queue_inter.get(queue_id, None)
            if iteritems is None:
                iteritems = self.queues[queue_id].iteritems()
                self.queue_iter[queue_id] = iteritems
            
            #pull a max of count jobs from the queue
            while True:
                try:
                    job_id, deuque_time = iteritems.next()
                except StopIteration:
                    iteritems = self.queues[queue_id].iteritems()
                    self.queue_iter[queue_id] = iteritems
                    continue 
                
                #add this job to the jobs result dict and update its lease time 
                ctime = time.time()
                if ctime - dequeue_time > DEQUEUE_LEASE_TIME:
                    self.queues[queue_id][job_id] = ctime
                    jobs[job_id] = self.jobs[job_id]['params']
                    if len(jobs) >= count:
                        return jobs
                else:
                    #iff the queues are maintain insert order and the iteration
                    #sequence walks the queue on the insert order we can safely
                    #rely on the lease times also being consistent such that 
                    #all jobs past this point will also met this same condition
                    # -> go to next queue
                    break
        return jobs 

    def get_status(self):
        queue_status = {}
        for key in self.queues:
            queue_status[key] = len(self.queues[key])
        return dict(total_jobs = len(self.jobs),
             total_tasks = len(self.tasks),
             queues = queue_status)

jobs = Jobs()

# Remove a job from a queue 
@route('/job/<job_id>', method='DELETE')
def job_delete(job_id):
    try:
        jobs.remove(job_id)
    except:
        abort(httplib.INTERNAL_SERVER_ERROR, traceback.format_exc())

# Put a job into a queue
@route('/job/<job_id>', method='PUT')
def job_put(job_id):
    """
    Job fields 
       + task_id - defines which task a job belongs to
       + queue_id - defines which queue_id this job belongs to 
       + params - params are returned when this job is returned from a GET
    """
    params = request.body.readline()
    if not params:
        abort(httplib.BAD_REQUEST, 'No params received')

    job = json.loads(params)   
    try:
        task_id = job['task_id']
        queue_id = job['queue_id']
        params = job['params']
        jobs.add(job_id, task_id, queue_id, params)
    except:
        abort(httplib.INTERNAL_SERVER_ERROR, traceback.format_exc())

# Get the next job
@route('/job/', method='GET')
def job_get():
    try:
        job = jobs.get_next()
    except:
        abort(httplib.INTERNAL_SERVER_ERROR, traceback.format_exc())
    return job

# Get the status 
@route('/status', method='GET')
def status():
    try:
        status = jobs.get_status()
    except:
        abort(httplib.INTERNAL_SERVER_ERROR, traceback.format_exc())
    return status


run(host='localhost', port=8080, debug=True)

