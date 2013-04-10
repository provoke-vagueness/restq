import traceback
import json
import httplib

import bottle
from bottle import request
import realms 


# Remove a job from a queue 
@bottle.delete('/<realm>/job/<job_id>')
def job_delete(realm, job_id):
    work = realms.get(realm)
    try:
        work.remove(job_id)
    except:
        bottle.abort(httplib.INTERNAL_SERVER_ERROR, traceback.format_exc())


# Put a job into a queue
@bottle.put('/<realm>/job/<job_id>')
def job_put(realm, job_id):
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


# Get the next job
@bottle.get('/<realm>/job')
def job_get(realm):
    work = realms.get(realm)
    try:
        count = request.GET.get('count', default=1, type=int)
        job = work.pull(count=count)
    except:
        bottle.abort(httplib.INTERNAL_SERVER_ERROR, traceback.format_exc())
    print job
    return job

# Get the status 
@bottle.get('/status')
def status(realm):
    try:
        status = realms.get_status()
    except:
        bottle.abort(httplib.INTERNAL_SERVER_ERROR, traceback.format_exc())
    return status


@bottle.get('/<realm>/task/<task_id>')
def task_status(realm, task_id):
    work = realms.get(realm)
    try:
        jobs = work.tasks[task_id]
    except:
        bottle.abort(httplib.INTERNAL_SERVER_ERROR, traceback.format_exc())
    return jobs


@bottle.get('/')
def doc():
    return """<!DOCTYPE html>
<html>
    <head>
        <title>restq: a Python-based queue with a RESTful web interface</title>
    </head>
    <body>
        <h1>restq</h1>
        <p>... placeholder for usage information...</p>
    </body>
</html>"""


if __name__ == "__main__":
    bottle.run(host='localhost', port=8080, debug=True)

app = bottle.default_app()





