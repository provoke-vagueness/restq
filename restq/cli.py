from __future__ import print_function
import sys
import os
from getopt import getopt

import restq
from restq import config


def command_web():
    from restq import webapp
    webapp.run()
    return 0


def command_add(arg):
    realm = restq.Realms()[config.cli['realm']]
    tags = config.cli['tags']
    if not tags:
        tags = None
    realm.add(arg, config.cli['queue_id'], tags=tags)
    return 0


def command_pull():
    realm = restq.Realms()[config.cli['realm']]
    jobs = realm.pull()
    for job in jobs:
        print(job)
    return 0


def command_remove():
    return 0


def command_list():
    print("Realms:\n  + %s" % "\n  + ".join(restq.Realms()))
    return 0


def command_status(arg=None, tag=None):
    realm = restq.Realms()[config.cli['realm']]
    if arg is not None:
        job = realm.get_job(arg)
        print("Status of argument %s:" % arg)
        print("Tagged with: " + ", ".join(job['tags']))
        queues = ["%8s | %0.2f" % (a, b) for a, b in job['queues']]
        print("queue id | (s) since dequeue\n%s" % "\n  + ".join(queues))
    elif tag is not None:
        status = realm.get_tag_status(tag)
        print("%s jobs tagged with %s" % (status['count'], tag))
    else:
        print("Status of realm %s:" % realm.name)
        status = realm.status
        print("Contains %(total_tags)s tags with %(total_jobs)s jobs" % \
                (status))
        print("Defined queues: " + ", ".join(status['queues']))
    return 0


def command_get(tag):
    realm = restq.Realms()[config.cli['realm']]
    try:
        jobs = realm.get_tagged_jobs(tag)
    except KeyError:
        print("No jobs found for tag '%s'" % tag)
    else:
        for arg in jobs:
            print(arg)
    return 0


defaults = {}
for interface, kwargs in config.values.items():
    c = {"%s_%s" % (interface, key) : value for key, value in kwargs.items()}
    defaults.update(c)

__help__ = """\
NAME restq - control over the restq 

SYNOPSIS
    restq COMMAND

COMMAND options:

    web [OPTIONS] [[HOST:][PORT]] 
        Run the RESTful web app.
        
        arguments 
            HOST:PORT defaults to %(webapp_host)s:%(webapp_port)s

        options
            --server=%(webapp_server)s
                Choose the server adapter to use.
            --debug=%(webapp_debug)s 
                Run in debug mode.
            --quiet=%(webapp_quiet)s
                Run in quite mode.

    add [OPTIONS] [ARG,...] 
        Add arguments into a REALM.

        options
            -r or --realm=%(cli_realm)s
                Specify which realm to operate in.
            --uri=%(client_uri)s
                Define the connection uri.
            --queue=%(cli_queue_id)s
                Put arguments into a specific priority queue id.
            --tags=[TAG,...]
                Tag the arguments.

    pull [OPTIONS]
        Pull arguments from REALM.  This will 'checkout' the arguments
        for the default timeout period.

        options
            -r or --realm=%(cli_realm)s
                Specify which realm to operate in.
            --count=%(client_count)s
                Number of jobs to pull from the queue
              --uri=%(client_uri)s
                Define the connection uri.
   
    remove arg|tag [OPTIONS] ARG|TAG
        Remove an arg or a set of arguments from a realm.

        options
            -r or --realm=%(cli_realm)s
                Specify which realm to operate in.
             --uri=%(client_uri)s
                Define the connection uri.
 
    list [OPTIONS] 
        List the available realms.

        options
            --uri=%(client_uri)s
                Define the connection uri.
 
    status [OPTIONS] arg|tag [ARG|TAG]
        Print the status of a REALM, or an argument or a tag in a realm.

        options
            -r or --realm=%(cli_realm)s
                Define which realm to read the status from.
            --uri=%(client_uri)s
                Define the connection uri.
 
    get [OPTIONS] TAG
        Get all arguments tagged in a realm with TAG. 

        options
            -r or --realm=%(cli_realm)s
                Specify which realm to operate in.
            --uri=%(client_uri)s
                Define the connection uri.
 
Example:
    restq add --tags=work "ls -lah"
    restq add --tags=work,fun pwd
    restq get work 
    restq status
    restq pull
    restq status arg pwd
    restq status arg "ls -lah"
    restq status tag work

""" % defaults


def main(args):
    if not args:
        print("No arguments provided", file=sys.stderr)
        return -1
    if '-h' in args or '--help' in args or "help" in args:
        print(__help__)
        return 0

    command = args.pop(0)

    try:
        opts, args = getopt(args, '-r', [
            'server=', 'debug=', 'quiet=',
            'realm=', 'uri=',
            'count=',
            'tags=', 'queue='
        ])
    except Exception as exc:
        print("Getopt error: %s" % (exc), file=sys.stderr)
        return -1

    for opt, arg in opts:
        if opt in ['--server']:
            config.webapp['server'] = arg
        elif opt in ['--quiet']:
            config.webapp['quite'] = arg.lower() != 'false'
        elif opt in ['--debug']:
            config.webapp['debug'] = arg.lower() != 'false'
        elif opt in ['--realm']:
            config.cli['realm'] = arg
        elif opt in ['--uri']:
            config.client['uri'] = arg
        elif opt in ['--count']:
            try:
                config.client['count'] = int(arg)
            except ValueError:
                print("failed to convert count to int (%s)" % arg)
                return -1
        elif opt in ['--queue']:
            config.cli['queue_id'] = arg
        elif opt in ['--tags']:
            config.cli['tags'] = arg.split(',')
        
    if command == 'web':
        if args:
            hostport = args[0]
            host = config.webapp['host']
            port = config.webapp['port']
            if ':' in hostport:
                host, p = hostport.split(':')
                # may not have a port value
                if p:
                    port = p
            else:
                port = hostport
            try:
                port = int(port)
            except ValueError:
                print("failed to convert port to int (%s)" % port)
                return -1
            config.webapp['host'] = host
            config.webapp['port'] = port
        return command_web()

    elif command == 'add':
        if not args:
            print("No arguments provided for addition into a realm.")
            return -1
        return command_add(" ".join(args))

    elif command == 'pull':
        return command_pull()

    elif command == 'remove':
        return command_remove()

    elif command == 'list':
        return command_list()

    elif command == 'status':
        tag = None
        arg = None
        if args:
            if len(args) < 2:
                print("require at least one more argument for this status")
                return -1
            subcmd = args.pop(0)
            if subcmd == 'arg':
                arg = args.pop(0)
            elif subcmd == 'tag':
                tag = args.pop(0)
            else:
                print("status args can only be arg or tag, got '%s'" % subcmd)
                return -1
        return command_status(arg=arg, tag=tag)

    elif command == 'get':
        if not args:
            print("No TAG argument provided.")
            return -1
        return command_get(args.pop(0))

    else:
        print("%s is not a valid command " % command, file=sys.stderr)
        return -1




entry = lambda: main(sys.argv[1:])
if __name__ == '__main__':
    sys.exit(entry())

