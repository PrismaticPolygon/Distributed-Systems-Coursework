import time
from http.server import BaseHTTPRequestHandler, HTTPServer

HOST_NAME = 'localhost'
PORT_NUMBER = 9000


class MyHandler(BaseHTTPRequestHandler):

    def do_HEAD(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):

        paths = {
            '/foo': {'status': 200},
            '/bar': {'status': 302},
            '/baz': {'status': 404},
            '/qux': {'status': 500}
        }

        if self.path in paths:
            self.respond(paths[self.path])
        else:
            self.respond({'status': 500})

    def handle_http(self, status_code, path):
        self.send_response(status_code)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        content = '''
        <html><head><title>Title goes here.</title></head>
        <body><p>This is a test.</p>
        <p>You accessed path: {}</p>
        </body></html>
        '''.format(path)
        return bytes(content, 'UTF-8')

    def respond(self, opts):
        response = self.handle_http(opts['status'], self.path)
        self.wfile.write(response)


if __name__ == '__main__':

    server_class = HTTPServer
    httpd = server_class((HOST_NAME, PORT_NUMBER), MyHandler)
    print(time.asctime(), 'Server Starts - %s:%s' % (HOST_NAME, PORT_NUMBER))
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    print(time.asctime(), 'Server Stops - %s:%s' % (HOST_NAME, PORT_NUMBER))

# Based on https://gist.github.com/Integralist/ce5ebb37390ab0ae56c9e6e80128fdc2

# import Pyro4
#
# uri = input("What is the Pyro uri of the greeting object? ").strip()
# name = input("What is your name? ").strip()
#
# greeting_maker = Pyro4.Proxy(uri)
# print(greeting_maker.get_fortunate(name))

## Front-end maintains a central list of all replication servers and decides accordingly.
# This means that each server will need a status.

# THis is the middleware. It handles remote calls, object invocation, and messages.
# From a user perspective, it hides the implementation details and distributed nature.
# Uses RPC: executes a remote function without the programmer coding the network communication.
# I shouldn't have to worry about timings. A client-side stub marshalls args, generates ID, starts timer
# then sends a message.

# the server-side stub unmarshalls, records ID, calls the remote function, marshalls, sets timer,
# and returns to the client.

# Location transparency cannot be facilitated.
# Let's start with the easy bits: the functions to retrieve, submit, and update movie ratings.

# Ask whether an RM is available. If it is send it off.

# The replica managers sends new postings to other RMs
# Causal update ordering, forced ordering, and immediate.

# Immediate-ordered updates are applied in a consistent order relative to any other update at all replica managers.
# Updates purely modify the state.

# Front-end normally sends requests to a single RM at a time. However, it will communicate with another if the
# one is normally uses in fails or becomes unreachable

# Isn't the daemon request loop() somewhat blocking?
# So: three servers and a front-end server! let's get that hosted first... hmm?