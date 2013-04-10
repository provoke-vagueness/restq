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
    work = realms.get(realm)
    task_id = request.forms.get('task_id', type=str)
    queue_id = request.forms.get('queue_id', type=int)
    if task_id is None or queue_id is None:
        bottle.abort(httplib.BAD_REQUEST, 'Require task_id and queue_id')
    
    data = request.files.get('data')
    if data is not None:
        data = data.file.read(10000)

    try:
        work.add(job_id, task_id, queue_id, data)
    except:
        bottle.abort(httplib.INTERNAL_SERVER_ERROR, traceback.format_exc())

# Get the next job
@bottle.get('/<realm>/job/')
def job_get(realm):
    work = realms.get(realm)
    try:
        count = request.GET.get('count', default=1, type=int)
        job = work.pull(count=count)
    except:
        bottle.abort(httplib.INTERNAL_SERVER_ERROR, traceback.format_exc())
    return job

# Get the status 
@bottle.get('/status')
def status(realm):
    try:
        status = realms.get_status()
    except:
        bottle.abort(httplib.INTERNAL_SERVER_ERROR, traceback.format_exc())
    return status


@bottle.get('/<realm>/task/<task_id>/')
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





