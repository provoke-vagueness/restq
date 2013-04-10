import bottle

import webapp
import realms 

try:
    # bjoern requires libev-dev to be installed for it to build
    # pip install bjoern 
    import bjoern
    server="bjoern"
except ImportError:
    server=None

if __name__ == "__main__":
    bottle.run(app=webapp.app, 
                server=server,
                host='localhost', 
                port=8080, 
                debug=True)


