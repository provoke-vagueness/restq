from __future__ import print_function
import traceback
import json
import httplib
import sys
from getopt import getopt

import bottle
from bottle import request
import realms 


@bottle.delete('/<realm_id>/job/<job_id>')
def remove_job(realm_id, job_id):
    """Remove a job from a realm"""
    realm = realms.get(realm_id)
    try:
        realm.remove_job(job_id)
    except:
        bottle.abort(httplib.INTERNAL_SERVER_ERROR, traceback.format_exc())

@bottle.delete('/<realm_id>/task/<task_id>')
def remove_task(realm_id, task_id):
    """Remove a task from a realm"""
    realm = realms.get(realm_id)
    try:
        realm.remove_task(task_id)
    except:
        bottle.abort(httplib.INTERNAL_SERVER_ERROR, traceback.format_exc())

@bottle.delete('/<realm_id>/project/<project_id>')
def remove_project(realm_id, project_id):
    """Remove a project from a realm"""
    realm = realms.get(realm_id)
    try:
        realm.remove_project(project_id)
    except:
        bottle.abort(httplib.INTERNAL_SERVER_ERROR, traceback.format_exc())

@bottle.put('/<realm_id>/job/<job_id>')
def add(realm_id, job_id):
    """Put a job into a queue
    JSON requires:  
        projects   - {project_id:[task_id,...],...}
        project    - (project_id, task_id)
        queue_id   -  
    Optional fields:
        data - input type='file' - data returned on GET job request
             - Max size data is JOB_DATA_MAX_SIZE
    """
    #validate client input
    try:
        body = json.loads(request.body.read(4096))
        try:
            project_id = body.get('project_id', None)
            task_id = body.get('task_id', None)
            queue_id = body['queue_id']
            data = body['data']
            realm = realms.get(realm_id)
            try:
                realm.add(job_id, queue_id, data, project_id=project_id,
                                    task_id=task_id)
            except:
                bottle.abort(\
                    httplib.INTERNAL_SERVER_ERROR, traceback.format_exc())
        except KeyError:
            bottle.abort(httplib.BAD_REQUEST, 'Require task_id queue_id & data')
    except ValueError:
        bottle.abort(httplib.BAD_REQUEST, 'Require json object in request body')

@bottle.get('/<realm_id>/job/<job_id>')
def get_job_state(realm_id, job_id):
    """Get the status of a job"""
    realm = realms.get(realm_id)
    try:
        status = realm.get_job_state(job_id)
    except:
        bottle.abort(httplib.INTERNAL_SERVER_ERROR, traceback.format_exc())
    return status

@bottle.get('/<realm_id>/task/<task_id>')
def get_task_state(realm_id, task_id):
    """Get the status of a task"""
    realm = realms.get(realm_id)
    try:
        status = realm.get_task_state(task_id)
    except:
        bottle.abort(httplib.INTERNAL_SERVER_ERROR, traceback.format_exc())
    return status

@bottle.get('/<realm_id>/project/<project_id>')
def get_project_state(realm_id, project_id):
    """Get the status of a project"""
    realm = realms.get(realm_id)
    try:
        status = realm.get_project_state(project_id)
    except:
        bottle.abort(httplib.INTERNAL_SERVER_ERROR, traceback.format_exc())
    return status

@bottle.get('/<realm_id>/job')
def pull(realm_id):
    """pull the next set of jobs from the realm"""
    realm = realms.get(realm_id)
    try:
        count = request.GET.get('count', default=1, type=int)
        job = realm.pull(count=count)
    except:
        bottle.abort(httplib.INTERNAL_SERVER_ERROR, traceback.format_exc())
    return job

# Get the status of the realm
@bottle.get('/<realm_id>/status')
def get_realm_status(realm_id):
    """return the status of a realm"""
    realm = realms.get(realm_id)
    try:
        status = realm.status
    except:
        bottle.abort(httplib.INTERNAL_SERVER_ERROR, traceback.format_exc())
    return status

@bottle.post('/<realm_id>/config')
def update_realm_config(realm_id):
    """update the configuration of a realm"""
    realm = realms.get(realm_id)
    try:
        body = json.loads(request.body.read(4096))
        lease_time = body.get('lease_time', None)
        if lease_time is not None:
            realm.set_lease_time(lease_time)
    except ValueError:
        bottle.abort(httplib.BAD_REQUEST, 'Require JSON in request body')

# Get the status from all of the realms
@bottle.get('/')
def realms_status():
    """return all of the realms and their statuses""" 
    return realms.get_status()


app = bottle.default_app()


__help__ = """
NAME restq-webapp - Start the restq webapp server

SYNOPSIS
    restq-webapp [OPTIONS]... [HOST:PORT]

DESCRIPTION

arguments 
    HOST:PORT default '127.0.0.1:8080'
        specify the ip and port to bind this server too

options 
    --server=
        choose the server adapter to use.

    --debug
        run in debug mode

    --quiet 
        run in quite mode
"""

def main(args):
    try:
        opts, args = getopt(args, 'h',['help',
            'server=', 'debug', 'quiet',
            ])
    except Exception as exc:
        print("Getopt error: %s" % (exc), file=sys.stderr)
        return -1

    bottle_run_kwargs = dict(app=app, debug=False)
    for opt, arg in opts:
        if opt in ['-h', '--help']:
            print(__help__)
            return 0
        elif opt in ['--server']:
            bottle_run_kwargs['server'] = arg
        elif opt in ['--quiet']:
            bottle_run_kwargs['quite'] = True
        elif opt in ['--debug']:
            bottle_run_kwargs['debug'] = True

    if args:
        try:
            host, port = args[0].split(':')
        except Exception as exc:
            print("failed to parse IP:PORT (%s)" % args[0])
            return -1
        try:
            port = int(port)
        except ValueError:
            print("failed to convert port to int (%s)" % port)
            return -1
        bottle_run_kwargs['host'] = host
        bottle_run_kwargs['port'] = port

    bottle.run(**bottle_run_kwargs)


entry = lambda :main(sys.argv[1:])
if __name__ == "__main__":
    sys.exit(entry())


