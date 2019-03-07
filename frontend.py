import uuid
from typing import Any, List

import Pyro4
from Pyro4.errors import CommunicationError

from enums import Status, Operation
from requests import ClientRequest, FrontendRequest, ReplicaResponse
from timestamp import Timestamp

FAULT_TOLERANCE = 2


class Frontend(object):
    def __init__(self):

        self.id = "frontend-" + str(uuid.uuid4())  # The ID of this FE

        # Reflects the version of the replicated data accessed by the FE; contains an entry for every RM. The FE sends
        # this with every query or update operation. When a RM returns a value as the result of a query operation, it
        # supplies a new vector timestamp, since the RMs may have been updated since the last operation. Each returned
        # timestamp is merged with the FE's previous timestamp to record the version the data observed by the client.
        self.prev = Timestamp()

        self.ns = Pyro4.locateNS()  # The Pyro Name Server

    def get_replica_uri(self) -> List[Pyro4.URI]:
        """
        Gets the Pyro URI of a suitable RM to send a request too. Replicas with an ACTIVE status take priority, followed
        by OVERLOADED. An error is thrown if all replicas are OFFLINE.
        :return: The Pyro URI of an RM
        """

        replicas = self.ns.list(metadata_all={"resource:replica"})  # Get all registered replicas

        uris = []

        for (name, uri) in replicas.items():

            try:

                with Pyro4.Proxy(uri) as replica:

                    status: Status = Status(replica.get_status())

                    print("{} reporting {}".format(name, status))

                    if status is not Status.OFFLINE:
                        uris.append((name, uri))  # Add the replica to those we'll use

                    if len(uris) >= FAULT_TOLERANCE:
                        return uris  # We've found enough replicas: return those we already have

            except CommunicationError:

                print("{} reporting Status.OFFLINE".format(name))  # The replica is offline!

        if len(uris) > 0:
            return uris  # We've found some!

        raise ConnectionRefusedError("All replicas reported Status.OFFLINE")  # All replicas are offline: raise an error

    @Pyro4.expose
    def request(self, request: ClientRequest) -> Any:
        """
        Sends a client request to f RMs, and returns the most up-to-date response to the client
        :param request: A ClientRequest sent by a client
        :return: None if the request is an update, and a value if it is a read.
        """

        print("\nReceived request from client {0}\n".format(request))

        frontend_request: FrontendRequest = FrontendRequest(self.prev, request)  # Build a frontend request
        uris = self.get_replica_uri()  # Get the URIs of available replicas
        responses: List[ReplicaResponse] = []

        for (name, uri) in uris:  # Iterate through those URIs

            print("\nUsing {}\n".format(name))

            with Pyro4.Proxy(uri) as replica:

                print("Sent timestamp {0}".format(self.prev))

                if request.method in [Operation.READ, Operation.AVERAGE, Operation.ALL]:

                    response: ReplicaResponse = replica.query(frontend_request)  # Query the replica

                else:

                    response: ReplicaResponse = replica.update(frontend_request)  # Update the replica

                responses.append(response)  # Add the response to those received

        value = None

        for response in responses:

            self.prev.merge(response.label)  # Merge this FE's timestamp with the timestamp received

            if response.value is not None:

                value = response.value   # Return the response of the first RM to execute the request

        print("\nNew timestamp  {0}".format(self.prev))
        print("Returning '{0}'".format(value))

        return value


if __name__ == '__main__':
    print("Creating frontend...")

    daemon = Pyro4.Daemon()
    frontend = Frontend()

    uri = daemon.register(frontend)

    print("{0} running".format(frontend.id), end="\n\n")

    frontend.ns.register(frontend.id, uri, metadata={"resource:frontend"})

    daemon.requestLoop()
