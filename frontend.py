import time
from http.server import BaseHTTPRequestHandler, HTTPServer
import Pyro4
import queue
import uuid
from enums import ReplicaStatus
from enum import Enum

# And when I import it runs! That's dumb.
# Solution is probably a static.

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
# So there is some st


@Pyro4.expose
@Pyro4.behavior(instance_mode="single")
class Frontend(object):

    servers = []

    prev = {}
    # Reflects the version of the latest data values accessed by the FE. Contains an entry for every RM. Sent by the FE
    # in every request message to an RM, together with a description of the query or update operation itself. When a RM
    # returns a value as the result of a query operation, it supplies a new vector timestamp, since the RMs may be been
    # updated since the last operation. Each update operation returns a vector timestamp that is unique to the update;
    # each returned timestamp is merged with the FE's previous timestamp to record the version of the replicated data
    # observed by the client.

    def get_replica(self):

        print("Getting replica...")

        ns = Pyro4.locateNS()

        replicas = ns.list(metadata_all={"resource:replica"})

        print("Found replicas", replicas, end="\n\n")

        overloaded = None

        for (name, uri) in replicas.items():

            replica: 'Replica' = Pyro4.Proxy(uri)
            status: ReplicaStatus = replica.get_status()

            print(name + " reporting status:", status)

            # That's weird...

            if status is ReplicaStatus.ACTIVE:

                print("\nReturning % (%)", name, status)

                return replica

            elif status is ReplicaStatus.OVERLOADED:

                print("Saving as overloaded")

                overloaded = (name, uri, status, replica)

        if overloaded is not None:

            print("\nReturning % (%)", overloaded[0], overloaded[2])

            return overloaded[3]

        raise ConnectionRefusedError("All replicas offline")

    def request(self, request):

        operation = {
            "id": uuid.uuid4(),
            "prev": self.prev,
            "op": {
                "request": request
            }
        }

        # Should contain type and parameters
        # Likely redundant

        if request.method is Method.READ:

            operation["op"]["type"] = Operation.QUERY

        else:

            operation["op"]["type"] = Operation.UPDATE

        # Send operation to an available server an available server

        response = {
            "value": "",
            "label": {}
        }

        if operation["type"] is Operation.QUERY:

            self.prev = response["label"]

        else:

            return response["value"]




    def putResult(self, item):

        # Item contains a result and a set of operations
        # Label identifies the updates whose execution must proceed the execution of o.
        labels = item.label
        value = item.value

        # Each label is a vector timestamp. So why is it a set of them?


        self.resultqueue.put(item.result)


print("Creating frontend...")

# That's very strange...

daemon = Pyro4.Daemon()
ns = Pyro4.locateNS()
frontend = Frontend()

uri = daemon.register(frontend)

print("Frontend created at", uri, end="\n\n")

ns.register("frontend", uri, metadata={"resource:frontend"})


frontend.get_replica()

# daemon.requestLoop()


class Method(Enum):
    CREATE = 0
    READ = 1
    UPDATE = 2
    DELETE = 3


class Operation(Enum):
    QUERY = 0
    UPDATE = 1


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