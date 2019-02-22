import uuid

import Pyro4

from enums import Status, Operation, Method
from replica import Replica
from requests import ClientRequest, FrontendRequest, ReplicaResponse, Timestamp


@Pyro4.behavior(instance_mode="single")
class Frontend(object):

    def __init__(self):

        self.id = uuid.uuid4()

        # A dictionary mapping replica names to their Pyro URIs. Places the overhead of a Pyro NS search on RM creation,
        # rather than every operation an FE performs.
        self.replicas = dict()

        # Reflects the version of the latest data values accessed by the FE. Contains an entry for every RM. Sent by the
        # FE in every request message to an RM, together with a description of the query or update operation itself.
        # When a RM returns a value as the result of a query operation, it supplies a new vector timestamp, since the
        # RMs may be been updated since the last operation. Each update operation returns a vector timestamp that is
        # unique to the update; each returned timestamp is merged with the FE's previous timestamp to record the version
        # of the replicated data observed by the client.
        self.prev = Timestamp()

    @Pyro4.expose
    def add_replica(self, name, uri) -> None:

        print("Replica added", (name, uri))

        self.replicas[name] = uri
        self.prev.add()

    def get_replica(self) -> Replica:

        print("Getting replica... ")

        overloaded = None

        for (name, uri) in self.replicas.items():

            replica: Replica = Pyro4.Proxy(uri)
            status: Status = Status(replica.get_status())

            print(name + " reporting status:", status, end="...\n")

            if status is Status.ACTIVE:

                print("Using {0} ({1})".format(name, status), end="\n\n")

                return replica

            elif status is Status.OVERLOADED:

                overloaded = (name, uri, status, replica)

        if overloaded is not None:

            print("Using {0} ({1})".format(overloaded[0], overloaded[2]), end="\n\n")

            return overloaded[3]

        raise ConnectionRefusedError("All replicas offline")

    @Pyro4.expose
    def request(self, request: ClientRequest):

        print("\nRequest received:", request, type(request))

        # It's being received as a dict here. But why?

        # Has no "thing" method. Hmm...
        # Right? This is a dict.

        replica: Replica = self.get_replica()

        # It's going wrong HERE1

        # This is wrong

        if request.method is Method.READ:

            query: FrontendRequest = FrontendRequest(self.prev, request, Operation.QUERY)

            response: ReplicaResponse = replica.query(query)

        else:

            update: FrontendRequest = FrontendRequest(self.prev, request, Operation.QUERY)

            response: ReplicaResponse = replica.update(update)

        print(response)

        self.prev = response.label

        return response.value


if __name__ == '__main__':

    print("Creating frontend...")

    daemon = Pyro4.Daemon()
    ns = Pyro4.locateNS()
    frontend = Frontend()

    uri = daemon.register(frontend)

    print("Frontend created at", uri, end="\n\n")

    ns.register("frontend-" + str(frontend.id), uri, metadata={"resource:frontend"})

    daemon.requestLoop()
