import bottle

import webapp
import realms 


if __name__ == "__main__":
    bottle.run(app=webapp.app, 
                host='localhost', 
                port=8080, 
                debug=True)


