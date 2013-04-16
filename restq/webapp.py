from __future__ import print_function
import traceback
import json
import httplib
import sys
from getopt import getopt

import bottle
from bottle import request
import realms 


# Remove a job from a queue 
@bottle.delete('/<realm>/job/<job_id>')
def job_delete(realm, job_id):
    work = realms.get(realm)
    try:
        work.remove_job(job_id)
    except:
        bottle.abort(httplib.INTERNAL_SERVER_ERROR, traceback.format_exc())


# Put a job into a queue
@bottle.put('/<realm>/job/<job_id>')
def job_add(realm, job_id):
    """
    Required fields:
        task_id - input type='text' - defines which task the job belongs to
        queue_id - input type='int' - defines which queue_id the job belongs 
    Optional fields:
        data - input type='file' - data returned on GET job request
             - Max size data is JOB_DATA_MAX_SIZE
    """
    #validate client input
    try:
        body = json.loads(request.body.read(4096))
        try:
            task_id = body['task_id']
            queue_id = body['queue_id']
            data = body['data']

            work = realms.get(realm)

            try:
                work.add(job_id, task_id, queue_id, data)
            except:
                bottle.abort(\
                    httplib.INTERNAL_SERVER_ERROR, traceback.format_exc())
        except KeyError:
            bottle.abort(httplib.BAD_REQUEST, 'Require task_id queue_id & data')
    except ValueError:
        bottle.abort(httplib.BAD_REQUEST, 'Require json object in request body')


# Put a job into a queue
@bottle.get('/<realm>/job/<job_id>')
def job_status(realm, job_id):
    work = realms.get(realm)
    try:
        status = work.status_job(job_id)
    except:
        bottle.abort(httplib.INTERNAL_SERVER_ERROR, traceback.format_exc())
    return status


# Get the next job
@bottle.get('/<realm>/job')
def job_pull(realm):
    work = realms.get(realm)
    try:
        count = request.GET.get('count', default=1, type=int)
        job = work.pull(count=count)
    except:
        bottle.abort(httplib.INTERNAL_SERVER_ERROR, traceback.format_exc())
    return job


# Delete the task
@bottle.delete('/<realm>/task/<task_id>')
def task_delete(realm, task_id):
    work = realms.get(realm)
    try:
        work.remove_task(task_id)
    except:
        bottle.abort(httplib.INTERNAL_SERVER_ERROR, traceback.format_exc())


# Get the status of the task 
@bottle.get('/<realm>/task/<task_id>')
def task_status(realm, task_id):
    work = realms.get(realm)
    try:
        status = work.status_task(task_id)
    except:
        bottle.abort(httplib.INTERNAL_SERVER_ERROR, traceback.format_exc())
    return status 


# Get the status of the realm
@bottle.get('/<realm>/status')
def realm_status(realm):
    work = realms.get(realm)
    try:
        status = work.status
    except:
        bottle.abort(httplib.INTERNAL_SERVER_ERROR, traceback.format_exc())
    return status


# Get the status from all of the realms
@bottle.get('/')
def realms_status():
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

"""

def main(args):
    try:
        opts, args = getopt(args, 'h',['help',
            'server=',
            ])
    except Exception as exc:
        print("Getopt error: %s" % (exc), file=sys.stderr)
        return -1

    bottle_run_kwargs = dict(app=app)
    for opt, arg in opts:
        if opt in ['-h', '--help']:
            print(__help__)
            return 0
        if opt in ['--server']:
            bottle_run_kwargs['server'] = arg
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


