from __future__ import print_function
import sys
import traceback
import json
import inspect
import functools
if sys.version_info[0] < 3:
    import httplib as client
else:
    from http import client
import time
import sys
from getopt import getopt

import bottle
from bottle import request, response
bottle.BaseRequest.MEMFILE_MAX = 1600000000

import prometheus_client
from prometheus_client import Gauge, Summary

from restq import realms 
from restq import config

# prometheus metrics state
request_summary = Summary(
    'restq_api_request_duration_seconds',
    'Time spent processing api request',
    ['resource', 'method']
)
request_timer = lambda *x: request_summary.labels(*x).time()

job_gauge = Gauge(
    'restq_queued_jobs',
    'Number of jobs in restq realms/queues',
    ['realm', 'queue']
)
job_gauge._samples = lambda: _get_job_stats()

tag_gauge = Gauge(
    'restq_queued_tags',
    'Number of tags in restq realms',
    ['realm']
)
tag_gauge._samples = lambda: _get_tag_stats()

def _get_job_stats():
    d = realms.get_status()
    return  [('', {'realm': name, 'queue': str(q)}, v) \
        for name, detail in d.items() for q, v in detail['queues'].items()]

def _get_tag_stats():
    d = realms.get_status()
    return  [('', {'realm': name}, detail['total_tags']) \
        for name, detail in d.items()]


class JSONError(bottle.HTTPResponse):
    def __init__(self, status, message='', exception='Exception'):
        if inspect.isclass(exception) and issubclass(exception, Exception):
            exception = exception.__name__
        elif isinstance(exception, Exception):
            exception = exception.__class__.__name__
        elif not type(exception) in [str, unicode]:
            raise Exception("unknown exception type %s" % type(exception))
        body = json.dumps({'error': status,
                            'exception': exception,
                            'message': message})
        bottle.HTTPResponse.__init__(\
                                self,
                                status=status,
                                headers={'Content-Type': 'application/json'},
                                body=body)


def wrap_json_error(f):
    @functools.wraps(f)
    def wrapper(*a, **k):
        try:
            return f(*a, **k)
        except JSONError:
            raise
        except Exception as exc:
            raise JSONError(client.INTERNAL_SERVER_ERROR,
                    exception=exc,
                    message=traceback.format_exc())
    return wrapper


def profile_function(profile_dict):
    def decorator(f):
        p = dict(call_count=0, max_time=-1, min_time=9999, total_time=0)
        profile_dict[f.func_name] = p
        @functools.wraps(f)
        def wrapper(*a, **k):
            try:
                s = time.time()
                return f(*a, **k)
            finally:
                t = time.time() - s
                p['call_count'] += 1
                p['max_time'] = max(p['max_time'], t)
                p['min_time'] = max(p['min_time'], t)
                p['total_time'] += t
        return wrapper
    return decorator


profile = dict()


def _del_job(realm_id, job_id):
    realm = realms.get(realm_id)
    try:
        realm.remove_job(job_id)
    except KeyError:
        pass


@bottle.delete('/<realm_id>/job/<job_id>')
@wrap_json_error
@request_timer('/realm/job', 'delete')
@profile_function(profile)
def delete_job(realm_id, job_id):
    """Remove a job from a realm"""
    _del_job(realm_id, job_id)
    return {}


@bottle.delete('/<realm_id>/tag/<tag_id>')
@wrap_json_error
@request_timer('/realm/tag', 'delete')
@profile_function(profile)
def delete_tagged_jobs(realm_id, tag_id):
    """Remove a tag and all of its jobs from a realm"""
    realm = realms.get(realm_id)
    realm.remove_tagged_jobs(tag_id)
    return {}


def _add_job(realm_id, job_id, queue_id, job):
    data = job.get('data', None)
    tags = job.get('tags', [])
    realm = realms.get(realm_id)
    realm.add(job_id, queue_id, data, tags=tags)


@bottle.put('/<realm_id>/job/<job_id>')
@wrap_json_error
@request_timer('/realm/job', 'put')
@profile_function(profile)
def add_job(realm_id, job_id):
    """Put a job into a queue
    JSON requires:
        queue_id   -
    Optional fields:
        data - input type='file' - data returned on GET job request
             - Max size data is JOB_DATA_MAX_SIZE
    """
    #validate input
    try:
        body = json.loads(request.body.read())
    except ValueError:
        raise JSONError(client.BAD_REQUEST,
                        exception='ValueError',
                        message='Require json object in request body')
    try:
        _add_job(realm_id, job_id, body['queue_id'], body)
    except KeyError:
        raise JSONError(client.BAD_REQUEST,
                        exception='KeyError',
                        message='Require queue_id & data')
    return {}


@bottle.post('/<realm_id>/jobs')
@wrap_json_error
@request_timer('/realm/jobs', 'post')
@profile_function(profile)
def realm_bulk_add_jobs(realm_id):
    """Multiple job post

    body contains jobs=[job, job, job, ...]
            where job={job_id, queue_id, data=None, tags=[]}

    """
    try:
        body = json.loads(request.body.read())
        try:
            jobs = body['jobs']
            for job in jobs:
                _add_job(realm_id, job['job_id'], job['queue_id'], job)
        except KeyError:
            raise JSONError(client.BAD_REQUEST,
                            exception='KeyError',
                            message='Require queue_id & data')
    except ValueError:
        raise JSONError(client.BAD_REQUEST,
                        exception='ValueError',
                        message='Require json object in request body')
    return {}


@bottle.post('/jobs')
@wrap_json_error
@request_timer('/jobs', 'post')
@profile_function(profile)
def realms_bulk_add_jobs():
    """Multiple job post across multiple realms

    body contains jobs=[{realm_id, job_id, queue_id, data, tags)}, ...]
    """
    try:
        body = json.loads(request.body.read())
        try:
            jobs = body['jobs']
            for job in jobs:
                _add_job(job['realm_id'], job['job_id'], job['queue_id'], job)
        except KeyError:
            raise JSONError(client.BAD_REQUEST,
                            exception='KeyError',
                            message='Require queue_id & data')
    except ValueError:
        raise JSONError(client.BAD_REQUEST,
                        exception='ValueError',
                        message='Require json object in request body')
    return {}

@bottle.delete('/<realm_id>/jobs')
@wrap_json_error
@request_timer('/realm/jobs', 'delete')
@profile_function(profile)
def realm_bulk_del_jobs(realm_id):
    """Multiple job post

    body contains jobs=[job, job, job, ...]
            where job={job_id, queue_id, data=None, tags=[]}
    """
    try:
        body = json.loads(request.body.read())
        jobs = body['jobs']
        for job_id in jobs:
            _del_job(realm_id, job_id)
    except ValueError:
        raise JSONError(client.BAD_REQUEST,
                        exception='ValueError',
                        message='Require json object in request body')
    return {}


@bottle.delete('/jobs')
@wrap_json_error
@request_timer('/jobs', 'delete')
@profile_function(profile)
def realms_bulk_del_jobs():
    """Multiple job delete across multiple realms

    body contains jobs=[(realm_id, job_id), ...]
    """
    try:
        body = json.loads(request.body.read())
        jobs = body['jobs']
        for realm_id, job_id in jobs:
            _del_job(realm_id, job_id)
    except ValueError:
        raise JSONError(client.BAD_REQUEST,
                        exception='ValueError',
                        message='Require json object in request body')
    return {}


@bottle.get('/<realm_id>/job/<job_id>/from_q/<from_q>/to_q/<to_q>')
@wrap_json_error
@profile_function(profile)
def move_job(realm_id, job_id, from_q, to_q):
    """Move the job from one queue to another"""
    realm = realms.get(realm_id)
    realm.move_job(job_id, from_q, to_q)
    return {}


@bottle.get('/<realm_id>/job/<job_id>')
@wrap_json_error
@request_timer('/realm/job', 'get')
@profile_function(profile)
def get_job(realm_id, job_id):
    """Get the status of a job"""
    realm = realms.get(realm_id)
    job = realm.get_job(job_id)
    return job


@bottle.get('/<realm_id>/tag/<tag_id>')
@wrap_json_error
@request_timer('/realm/tag', 'get')
@profile_function(profile)
def get_tagged_jobs(realm_id, tag_id):
    """return a dict of all jobs tagged by tag_id"""
    realm = realms.get(realm_id)
    jobs = realm.get_tagged_jobs(tag_id)
    return jobs


@bottle.get('/<realm_id>/tag/<tag_id>/status')
@wrap_json_error
@request_timer('/realm/tag/status', 'get')
@profile_function(profile)
def get_tag_status(realm_id, tag_id):
    """return an int of the number of jobs related to tag_id"""
    realm = realms.get(realm_id)
    status = realm.get_tag_status(tag_id)
    return status


@bottle.get('/<realm_id>/job')
@wrap_json_error
@profile_function(profile)
def pull_jobs(realm_id):
    """pull the next set of jobs from the realm"""
    realm = realms.get(realm_id)
    count = request.GET.get('count', default=1, type=int)
    job = realm.pull(count=count)
    return job


@bottle.get('/<realm_id>/queues/<queue_id>/clear')
@wrap_json_error
@profile_function(profile)
def clear_queue(realm_id, queue_id):
    """remove all jobs from the given queue"""
    realm = realms.get(realm_id)
    realm.clear_queue(queue_id)
    return {}


# Get the status of the realm
@bottle.get('/<realm_id>/status')
@wrap_json_error
@profile_function(profile)
def get_realm_status(realm_id):
    """return the status of a realm"""
    realm = realms.get(realm_id)
    status = realm.status
    return status


@bottle.post('/<realm_id>/config')
@wrap_json_error
@request_timer('/realm/config', 'post')
@profile_function(profile)
def update_realm_config(realm_id):
    """update the configuration of a realm"""
    realm = realms.get(realm_id)
    try:
        body = json.loads(request.body.read(4096))
    except Exception as exc:
        raise JSONError(client.BAD_REQUEST,
                        exception=exc,
                        message='Require JSON in request body')

    lease_time = body.get('default_lease_time', None)
    if lease_time is not None:
        if type(lease_time) not in (long, int):
            raise JSONError(client.BAD_REQUEST,
                    exception='TypeError',
                    message="default_lease_time not int")
        realm.set_default_lease_time(lease_time)

    queue_lease_time = queue_lease_time = body.get('queue_lease_time', None)
    if queue_lease_time is not None:
        try:
            queue_id, lease_time = queue_lease_time
        except (ValueError, TypeError) as err:
            raise JSONError(client.BAD_REQUEST,
                    exception='ValueError',
                    message='queue_lease_time err - %s' % err)
        if type(lease_time) not in (long, int):
            raise JSONError(client.BAD_REQUEST,
                    exception='TypeError',
                    message="default_lease_time not int")
        realm.set_queue_lease_time(queue_id, lease_time)
    return {}


@bottle.delete('/<realm_id>/')
@wrap_json_error
@request_timer('/realm', 'delete')
@profile_function(profile)
def delete_realm(realm_id):
    realms.delete(realm_id)
    return {}


@bottle.get('/performance')
@wrap_json_error
@request_timer('/performance', 'get')
@profile_function(profile)
def webapp_performance():
    """return the performance of the webapp"""
    return profile


@bottle.get('/')
@wrap_json_error
@request_timer('/', 'get')
@profile_function(profile)
def realms_status():
    """return all of the realms and their statuses"""
    return realms.get_status()

@bottle.get('/metrics')
@request_timer('/metrics', 'get')
def prometheus_metrics():
    """Prometheus exporter api"""
    response.content_type = prometheus_client.CONTENT_TYPE_LATEST
    return prometheus_client.generate_latest()


app = bottle.default_app()
def run():
    global proxy_requests
    bottle_kwargs = dict(debug=config.webapp['debug'],
                         quiet=config.webapp['quiet'],
                         host=config.webapp['host'],
                         port=config.webapp['port'],
                         server=config.webapp['server'])
    bottle.run(app=app, **bottle_kwargs)
