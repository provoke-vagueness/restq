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
JOB_PROJECTS = 1
JOB_TASKS = 2
JOB_QUEUES = 3
TASK_JOBS = 0
TASK_PROJECTS = 1
PROJECT_JOBS = 0
PROJECT_TASKS = 1


class Realm:
    def __init__(self, realm):
        self.realm = realm 
        self.queues = {}
        self.queue_iter = {}
        self.queue_lease_time = {}
        self.projects = {}
        self.tasks = {}
        self.jobs = {}
        self.lock = Lock()
        self.config_path = os.path.join(CONFIG_ROOT, realm + ".realm")
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

        # remove from tasks
        empty_tasks = []
        for task_id in job[JOB_TASKS]:
            task = self.tasks[task_id]
            task[TASK_JOBS].remove(job_id)
            # if there are no more jobs in task, remove it
            if not task[TASK_JOBS]:
                self.tasks.pop(task_id)
                for project_id in task[TASK_PROJECTS]:
                    self.projects[project_id][PROJECT_TASKS].remove(task_id)

        # remove from projects 
        empty_projects = []
        for project_id in job[JOB_PROJECTS]:
            project = self.projects[project_id]
            project[PROJECT_JOBS].remove(job_id)
            # if there are no more jobs in project, remove it
            if not project[TASK_JOBS]:
                self.projects.pop(project_id)
                for task_id in project[PROJECT_TASKS]:
                    self.tasks[task_id][PROJECT_TASKS].remove(project_id)

    @serialise
    def remove_task(self, task_id):
        """remove a task and all of its jobs from the system"""
        task = self.tasks[task_id]
        for job_id in [i for i in task[TASK_JOBS]]:
            self._remove_job(job_id)

    @serialise 
    def remove_project(self, project_id):
        """remove a task and all of its jobs from the system"""
        project = self.projects[project_id]
        for job_id in [i for i in project[PROJECT_JOBS]]:
            self._remove_job(job_id)

    @serialise 
    def get_job_state(self, job_id):
        """return the status of a job"""
        self._get_job_state(job_id)
    def _get_job_state(self, job_id):
        status = {'tasks':job[JOB_TASKS], 
                  'projects':job[JOB_PROJECTS],
                  'queues':[]}
        now = time.time()
        for queue_id in job[JOB_QUEUES]:
            checkout_time = self.queues[queue_id][job_id]
            if checkout_time != 0:
                checkout_time = now - checkout_time
            status['queues'].append((queue_id, checkout_time))
        return status

    @serialise
    def get_task_state(self, task_id):
        """return the status of a task"""
        status = {}
        task = self.tasks[task_id]
        for job_id in task[TASK_JOBS]:
            status[job_id] = self._get_job_state(job_id)
        return status

    @serialise
    def get_project_state(self, project_id):
        """return the status of a project"""
        status = {}
        project = self.project[project_id]
        for job_id in project[PROJECT_JOBS]:
            status[job_id] = self._get_job_state(job_id)
        return status

    @serialise
    def add(self, projects, job_id, queue_id, data):
        """store a job into a queue
        
        kwargs:
           projects - {project_id:[task_id,...],...}
           job_id 
           queue_id
           data         
        """
        #store our job 
        job = self.jobs.get(job_id, None)
        if job is None:
            job = (data, set(), set(), set())
            self.jobs[job_id] = job
        else:
            if data != job[JOB_DATA]:
                msg = "old job entry and new data != old data (%s)" % (job_id)
                raise ValueError(msg)
        
        # update the job's queue record 
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
 
        # update project and task records 
        for project_id, task_ids in projects.items():

            # add project to job
            job[JOB_PROJECTS].add(project_id)
            
            # add job to this project
            project = self.projects.get(project_id, None)
            if project is None:
                project = (set(), set())
                self.projects[project_id] = project
            project[PROJECT_JOBS].add(job_id)

            for task_id in task_ids:
                # add task to project and job
                project[PROJECT_TASKS].add(task_id)
                job[JOB_TASKS].add(task_id)
                
                # add job and project to this task 
                task = self.tasks.get(task_id, None)
                if task is None:
                    task = (set(), set())
                    self.tasks[task_id] = task
                task[TASK_JOBS].add(job_id)
                task[TASK_PROJECTS].add(project_id)


    @serialise
    def pull(self, count):
        """pull out a max of count jobs"""
        queues_ids = self.queues.keys()
        queues_ids.sort()
        jobs = {}
        for queue_id in self.queues:
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
                    jobs[job_id] = self.jobs[job_id][JOB_DATA]
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
             total_tasks = len(self.tasks),
             total_projects = len(self.projects),
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
def get(realm_id):
    realm = _realms.get(realm_id, None)
    if realm is None:
        realm = Realm(realm_id)
        _realms[realm_id] = realm
    return realm


for filename in os.listdir(CONFIG_ROOT):  
    realm_id, ext = os.path.splitext(filename)
    if ext == '.realm':
        get(realm_id)


def get_status():
    status = {}
    for realm_id, realm in _realms.iteritems():
        status[realm_id] = realm.status
    return status


