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




defaults = {}
for interface, kwargs in config.values.items():
    c = {"%s_%s" % (interface, key) : value for key, value in kwargs.items()}
    defaults.update(c)

__help__ = """\
NAME restq - control over the restq 

SYNOPSIS
    restq [COMMAND]

Commands:

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


""" % defaults


def main(args):
    if not args:
        print("No arguments provided", file=sys.stderr)
        return -1
    if '-h' in args or '--help' in args:
        print(__help__)
        return 0

    command = args.pop(0)

    try:
        opts, args = getopt(args, '', [
            'server=', 'debug=', 'quiet=',
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

    else:
        print("%s is not a valid command " % command, file=sys.stderr)
        return -1




entry = lambda: main(sys.argv[1:])
if __name__ == '__main__':
    sys.exit(entry())

