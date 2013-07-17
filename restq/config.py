import os
import yaml
import tempfile
import hashlib


# Define our defaults.
project = 'restq'

_default_realm_config_root = os.path.join(os.path.expanduser("~"), ".restq")
values = dict(
            webapp=dict(
                debug=False,
                quiet=False,
                host='127.0.0.1',
                port=8586,
                server='wsgiref',
                ),
            realms=dict(
                default_lease_time=60*10,
                realms_config_root=_default_realm_config_root,
            ),
            cli=dict(
                realm='default',
                queue_id='0',
                tags=[],
            ),
            client=dict(
                uri='http://localhost:8586/',
                count=5,
            ),
        )


def _update_values(new):
    for interface, kwargs in new.items():
        values[interface].update(kwargs)


# Load the system configuration file 
if os.path.exists('/etc/%s.yaml' % project):
    with open('/etc/%s.yaml' % project, 'r') as f:
        _update_values(yaml.load(f))


# Load the user configuration file, update config with its values or initialise 
# a new configuration file if it didn't exist. 
_config_file_path = os.path.join(os.path.expanduser('~'), '.%s.yaml' % project)
if os.path.exists(_config_file_path):
    with open(_config_file_path, 'r') as f:
        _update_values(yaml.load(f))
else:
    with open(_config_file_path, 'w') as f:
        yaml.dump(values, f, default_flow_style=False)


# Update config with the values found in our current env
for interface, kwargs in values.items():
    for key, value in kwargs.items():
        environ_key = ('%s_%s_%s' % (project, interface, key)).upper()
        value_type = type(value)
        kwargs[key] = value_type(os.environ.get(environ_key, value))


for key, value in values.iteritems():
    globals()[key] = value
