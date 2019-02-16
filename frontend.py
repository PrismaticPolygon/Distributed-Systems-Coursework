import time
from http.server import BaseHTTPRequestHandler, HTTPServer
import Pyro4
import queue

HOST_NAME = 'localhost'
PORT_NUMBER = 9000

# It blocks, though.
# Needs to be in a separate... thing.
# A replicat
#
# daemon = Pyro4.Daemon()
# uri = daemon.register(Server())
#
# print("Ready. Object uri = ", uri)
# daemon.requestLoop()

# Server creates one or more instances and registers them with the Pyro Name Server
# Queries the name server for the location of those objects. Gets a URI for the,
# Perfect, found an example.
# Ah. I've got it the wrong way around, it seems.
# So it's not the RCs being called by the front-end, it's the front-end being called by the RCs.
# Oh.. wait. So long as they're registered with the name server, I don't have to do jack!
# This sounds like my solution.

# FE receives request.
# FE forwards it to R(s)
# R(s) accept request and decide ordering relative to other requests
# R(s) process request
# R(s) reach consensus
# R(s) reply to FE; FE processes response, then returns it to client

# FE attaches unique id and uses TOTALLY ORDERED RELIABLE MULTICAST to send requests. It does not issue requests in parallel
# Multi-cast delivers requests to all the Rs in the same (total) order

# But it's neither of these. It's gossip architecture.


@Pyro4.expose
@Pyro4.behavior(instance_mode="single")
class DispatcherQueue(object):

    servers = []

    # So now the dispatcher knows that it has at least one worker. And so: it can question them.
    # So now it gets complicated, and I have to put in some fancy logic. For now: can I start

    def add_server(self, uri):

        self.servers.append(uri)
        print("URI added:", self.servers)

    def __init__(self):
        self.workqueue = queue.Queue()
        self.resultqueue = queue.Queue()

    def putWork(self, item):

        # So when we're given work... Put it in a queue?
        # Normally sends requests to a single RM at a time. If the one it normally uses is unreachable..

        self.workqueue.put(item)

    def getWork(self, timeout=5):
        try:
            return self.workqueue.get(block=True, timeout=timeout)
        except queue.Empty:
            raise ValueError("no items in queue")

    def putResult(self, item):
        self.resultqueue.put(item)

    def getResult(self, timeout=5):
        try:
            return self.resultqueue.get(block=True, timeout=timeout)
        except queue.Empty:
            raise ValueError("no result available")

    def workQueueSize(self):
        return self.workqueue.qsize()

    def resultQueueSize(self):
        return self.resultqueue.qsize()


Pyro4.Daemon.serveSimple({
    DispatcherQueue: "example.distributed.dispatcher",
})

# Running on the same port! Interesting. And, of course, we can just extend that to three.

# Right. This blocks!
# But we can set up multiple processes here, and set their names. That way, the dispatcher can know what the other
# ones will be called,

print("Serving simple")

# The key insight is that the front-end is also a Pyro object.








# This connects with the proxies.
# Could use sockets or events instead of a main request loop.

# Ah, Pyro does it for me!

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


# if __name__ == '__main__':
#
#     server_class = HTTPServer
#     httpd = server_class((HOST_NAME, PORT_NUMBER), MyHandler)
#     print(time.asctime(), 'Server Starts - %s:%s' % (HOST_NAME, PORT_NUMBER))
#     try:
#         httpd.serve_forever()
#     except KeyboardInterrupt:
#         pass
#     httpd.server_close()
#     print(time.asctime(), 'Server Stops - %s:%s' % (HOST_NAME, PORT_NUMBER))

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