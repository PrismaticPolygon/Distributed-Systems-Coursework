import uuid
from typing import Any, List

import Pyro4
from Pyro4.errors import CommunicationError

from enums import Status, Operation
from requests import ClientRequest, FrontendRequest, ReplicaResponse
from timestamp import Timestamp

FAULT_TOLERANCE = 2

@Pyro4.expose
class Frontend(object):

    def __init__(self):

        self.id = "frontend-" + str(uuid.uuid4())

        # Reflects the version of the latest data values accessed by the FE. Contains an entry for every RM. Sent by the
        # FE in every request message to an RM, together with a description of the query or update operation itself.
        # When a RM returns a value as the result of a query operation, it supplies a new vector timestamp, since the
        # RMs may be been updated since the last operation. Each update operation returns a vector timestamp that is
        # unique to the update; each returned timestamp is merged with the FE's previous timestamp to record the version
        # of the replicated data observed by the client.
        self.prev = Timestamp()

        self.ns = Pyro4.locateNS()

    def get_replica_uri(self) -> List[Pyro4.URI]:
        """
        Gets the Pyro URI of a suitable RM to send a request too. Replicas with an ACTIVE status take priority, followed
        by OVERLOADED. An error is thrown if all replicas are OFFLINE.
        :return: The Pyro URI of an RM
        """

        replicas = self.ns.list(metadata_all={"resource:replica"})

        uris = []

        for (name, uri) in replicas.items():

            try:

                with Pyro4.Proxy(uri) as replica:

                    status: Status = Status(replica.get_status())

                    print("{} reporting {}".format(name, status))

                    if status is not Status.OFFLINE:

                        uris.append((name, uri))

                    if len(uris) >= FAULT_TOLERANCE:

                        return uris

            except CommunicationError as e:

                print("{} reporting Status.OFFLINE".format(name))

        if len(uris) > 0:

            return uris

        raise ConnectionRefusedError("All replicas reported Status.OFFLINE")

    @Pyro4.expose
    def request(self, request: ClientRequest) -> Any:
        """
        Sends a client request to f RMs, and returns the most up-to-date response to the client
        :param request: A ClientRequest sent by a client
        :return: None if the request is an update, and a value if it is a read.
        """

        print("\nReceived request from client {0}\n".format(request))

        frontend_request: FrontendRequest = FrontendRequest(self.prev, request)
        uris = self.get_replica_uri()
        responses: List[ReplicaResponse] = []

        for (name, uri) in uris:

            print("\nUsing {}\n".format(name))

            with Pyro4.Proxy(uri) as replica:

                print("Sent timestamp {0}".format(self.prev))

                if request.method in [Operation.READ, Operation.AVERAGE, Operation.ALL]:

                    response: ReplicaResponse = replica.query(frontend_request)

                else:

                    response: ReplicaResponse = replica.update(frontend_request)

                responses.append(response)

                self.prev.merge(response.label)

                print("New timestamp  {0}".format(self.prev))

        if len(responses) > 0:

            return responses[0].value

        else:

            return None


if __name__ == '__main__':

    print("Creating frontend...")

    daemon = Pyro4.Daemon()
    frontend = Frontend()

    uri = daemon.register(frontend)

    print("{0} running".format(frontend.id), end="\n\n")

    frontend.ns.register(frontend.id, uri, metadata={"resource:frontend"})

    daemon.requestLoop()
