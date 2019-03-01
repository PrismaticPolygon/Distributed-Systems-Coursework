import uuid
from typing import Dict, Any

import Pyro4

from enums import Status, Operation
from requests import ClientRequest, FrontendRequest, ReplicaResponse
from timestamp import Timestamp


@Pyro4.expose
class Frontend(object):

    @Pyro4.expose
    @property
    def id(self):
        return self._id

    def __init__(self):

        self._id = "frontend-" + str(uuid.uuid4())

        # A dictionary mapping RM IDs to their Pyro URIs. Places the overhead of a Pyro NS search on RM creation,
        # rather than every operation an FE performs.
        self.replicas: Dict[str, Pyro4.URI] = dict()

        # Reflects the version of the latest data values accessed by the FE. Contains an entry for every RM. Sent by the
        # FE in every request message to an RM, together with a description of the query or update operation itself.
        # When a RM returns a value as the result of a query operation, it supplies a new vector timestamp, since the
        # RMs may be been updated since the last operation. Each update operation returns a vector timestamp that is
        # unique to the update; each returned timestamp is merged with the FE's previous timestamp to record the version
        # of the replicated data observed by the client.
        self.prev = Timestamp()

    def register_replica(self, id, uri) -> None:
        """
        Registers a RM with this FE.
        :param id: The ID of the RM to be registered
        :param uri: The Pyro URI of the RM to be registered
        :return: None
        """

        print("{0} registered".format(id))

        self.replicas[id] = uri
        self.prev.replicas[id] = 0

    def get_replica_uri(self) -> Pyro4.URI:
        """
        Gets the Pyro URI of a suitable RM to send a request too. Replicas with an ACTIVE status take priority, followed
        by OVERLOADED. An error is thrown if all replicas are OFFLINE.
        :return: The Pyro URI of an RM
        """

        overloaded = None

        for (name, uri) in self.replicas.items():

            with Pyro4.Proxy(uri) as replica:

                status: Status = Status(replica.get_status())

                if status is Status.ACTIVE:

                    print("Using {0} ({1})".format(name, status), end="\n\n")

                    return uri

                elif status is Status.OVERLOADED:

                    overloaded = (name, uri, status)

        if overloaded is not None:

            print("Using {0} ({1})".format(overloaded[0], overloaded[2]), end="\n\n")

            return overloaded[1]

        raise ConnectionRefusedError("All replicas offline")

    @Pyro4.expose
    def request(self, request: ClientRequest) -> Any:
        """
        Sends a client request to an RM, and returns the response to the client
        :param request: A ClientRequest sent by a client
        :return: None if the request is an update, and a value if it is a read.
        """

        print("\nReceived request from client {0}".format(request))

        uri = self.get_replica_uri()

        with Pyro4.Proxy(uri) as replica:

            frontend_request: FrontendRequest = FrontendRequest(self.prev, request)

            print("Sent timestamp     {0}".format(self.prev))

            if request.method is Operation.READ:

                response: ReplicaResponse = replica.query(frontend_request)

            else:

                response: ReplicaResponse = replica.update(frontend_request)

            print("Received timestamp {0}".format(response.label))

            self.prev.merge(response.label)

            return response.value


if __name__ == '__main__':

    print("Creating frontend...")

    daemon = Pyro4.Daemon()
    ns = Pyro4.locateNS()
    frontend = Frontend()

    uri = daemon.register(frontend)

    print("{0} running".format(frontend.id), end="\n\n")

    ns.register(frontend.id, uri, metadata={"resource:frontend"})

    daemon.requestLoop()
