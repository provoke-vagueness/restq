from __future__ import print_function
import sys
import traceback
import json
if sys.version_info[0] < 3:
    import httplib as client
else: 
    from http import client

import sys
from getopt import getopt

import bottle
from bottle import request

from restq import realms 


@bottle.delete('/<realm_id>/job/<job_id>')
def remove_job(realm_id, job_id):
    """Remove a job from a realm"""
    realm = realms.get(realm_id)
    try:
        realm.remove_job(job_id)
    except:
        bottle.abort(client.INTERNAL_SERVER_ERROR, traceback.format_exc())

@bottle.delete('/<realm_id>/tag/<tag_id>')
def remove_tagged_jobs(realm_id, tag_id):
    """Remove a tag and all of its jobs from a realm"""
    realm = realms.get(realm_id)
    try:
        realm.remove_tagged_jobs(tag_id)
    except:
        bottle.abort(client.INTERNAL_SERVER_ERROR, traceback.format_exc())

@bottle.put('/<realm_id>/job/<job_id>')
def add(realm_id, job_id):
    """Put a job into a queue
    JSON requires:  
        queue_id   -  
    Optional fields:
        data - input type='file' - data returned on GET job request
             - Max size data is JOB_DATA_MAX_SIZE
    """
    #validate input
    try:
        body = json.loads(request.body.read(4096))
        try:
            tags = body.get('tags', [])
            queue_id = body['queue_id']
            data = body.get('data', None)
            realm = realms.get(realm_id)
            try:
                realm.add(job_id, queue_id, data, tags=tags)
            except:
                bottle.abort(\
                    client.INTERNAL_SERVER_ERROR, traceback.format_exc())
        except KeyError:
            bottle.abort(client.BAD_REQUEST, 'Require queue_id & data')
    except ValueError:
        bottle.abort(client.BAD_REQUEST, 'Require json object in request body')

@bottle.get('/<realm_id>/job/<job_id>')
def get_job(realm_id, job_id):
    """Get the status of a job"""
    realm = realms.get(realm_id)
    try:
        job = realm.get_job(job_id)
    except:
        bottle.abort(client.INTERNAL_SERVER_ERROR, traceback.format_exc())
    return job

@bottle.get('/<realm_id>/tag/<tag_id>')
def get_tagged_jobs(realm_id, tag_id):
    """return a dict of all jobs tagged by tag_id"""
    realm = realms.get(realm_id)
    try:
        jobs = realm.get_tagged_jobs(tag_id)
    except:
        bottle.abort(client.INTERNAL_SERVER_ERROR, traceback.format_exc())
    return jobs

@bottle.get('/<realm_id>/tag/<tag_id>/status')
def get_tag_status(realm_id, tag_id):
    """return an int of the number of jobs related to tag_id"""
    realm = realms.get(realm_id)
    try:
        status = realm.get_tag_status(tag_id)
    except:
        bottle.abort(client.INTERNAL_SERVER_ERROR, traceback.format_exc())
    return status

@bottle.get('/<realm_id>/job')
def pull(realm_id):
    """pull the next set of jobs from the realm"""
    realm = realms.get(realm_id)
    try:
        count = request.GET.get('count', default=1, type=int)
        job = realm.pull(count=count)
    except:
        bottle.abort(client.INTERNAL_SERVER_ERROR, traceback.format_exc())
    return job

# Get the status of the realm
@bottle.get('/<realm_id>/status')
def get_realm_status(realm_id):
    """return the status of a realm"""
    realm = realms.get(realm_id)
    try:
        status = realm.status
    except:
        bottle.abort(client.INTERNAL_SERVER_ERROR, traceback.format_exc())
    return status

@bottle.post('/<realm_id>/config')
def update_realm_config(realm_id):
    """update the configuration of a realm"""
    realm = realms.get(realm_id)
    try:
        body = json.loads(request.body.read(4096))

        lease_time = body.get('default_lease_time', None)
        if lease_time is not None:
            if type(lease_time) not in (long, int):
                bottle.abbort(client.BAD_REQUEST, "default_lease_time not int")
            realm.set_default_lease_time(lease_time)
        
        queue_lease_time = queue_lease_time = body.get('queue_lease_time', None)
        if queue_lease_time is not None:
            try:
                queue_id, lease_time = queue_lease_time 
            except (ValueError, TypeError) as err:
                bottle.abort(client.BAD_REQUEST, 
                                'queue_lease_time err - %s' % err)
            if type(lease_time) not in (long, int):
                bottle.abbort(client.BAD_REQUEST, "default_lease_time not int")
            realm.set_queue_lease_time(queue_id, lease_time)

    except ValueError:
        bottle.abort(client.BAD_REQUEST, 'Require JSON in request body')

@bottle.delete('/<realm_id>/')
def delete_realm(realm_id):
    realms.delete(realm_id)

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
    HOST:PORT default '127.0.0.1:8585'
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

    bottle_run_kwargs = dict(app=app, port=8585, debug=False)
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


