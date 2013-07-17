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


def command_put():
    return 0


def command_pull():
    return 0


def command_remove():
    return 0


def command_list():
    print("Realms:\n%s" % "\n  + ".join(restq.Realms()))
    return 0


def command_status():
    realm = restq.Realms()[config.cli['realm']]
    print("Realm: %s" % realm.name)
    status = realm.status
    print("Contains %(total_tags)s tags with %(total_jobs)s jobs" % (status))
    queues = ["%s(%s)" % (a, b) for a, b in status['queues']]
    print("Queues: " + ", ".join(queues))
    return 0


def command_get():
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

    put [OPTIONS] [ARG,...] 
        Put arguments into a REALM.

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
    
    remove arg|tag [OPTIONS] ARG|TAG
        Remove an arg or a set of arguments from a realm.

        options
            -r or --realm=%(cli_realm)s
                Specify which realm to operate in.
 
    list 
        List the available realms.

    status REALM
        Print the status of a REALM.

    get TAG
        Get all arguments tagged in a realm with TAG. 

        options
            -r or --realm=%(cli_realm)s
                Specify which realm to operate in.
 
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

    elif command == 'put':
        return command_put()

    elif command == 'pull':
        return command_pull()

    elif command == 'remove':
        return command_remove()

    elif command == 'list':
        return command_list()

    elif command == 'status':
        return command_status()

    elif command == 'get':
        return command_get()

    else:
        print("%s is not a valid command " % command, file=sys.stderr)
        return -1




entry = lambda: main(sys.argv[1:])
if __name__ == '__main__':
    sys.exit(entry())

