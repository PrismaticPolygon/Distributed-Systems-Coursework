import uuid

import Pyro4

from enums import Status, Operation, Method
from replica import Replica
from requests import ClientRequest, FrontendRequest, ReplicaResponse
from timestamp import Timestamp


@Pyro4.expose
@Pyro4.behavior(instance_mode="single")
class Frontend(object):

    @Pyro4.expose
    @property
    def attr(self):
        return self.id

    def __init__(self):

        self.id = "frontend-" + str(uuid.uuid4())

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

    def add_replica(self, name, uri) -> None:

        print("{0} added".format(name))

        # Do I actually need my replicas? Not if I have my prev list. Wait, I do need the URIs.

        self.replicas[name] = uri
        self.prev.add(name)

    def get_replica(self) -> Replica:

        print("Getting replica... ")

        # Ah, replicas aren't adding themselves..

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

    # They're not the same size because we don't KNOW how big it ought to be
    # That implies fore-knowledge of the number of replicas.
    # For now I can fix the method, but we'll keep it in mind...

    @Pyro4.expose
    def request(self, request: ClientRequest):

        print("\n{0} requested...".format(request))

        try:

            replica: Replica = self.get_replica()

            if request.method is Method.READ:

                print("Sending query...")

                query: FrontendRequest = FrontendRequest(self.prev, request, Operation.QUERY)

                response: ReplicaResponse = replica.query(query)

            else:

                print("Sending update...")

                update: FrontendRequest = FrontendRequest(self.prev, request, Operation.UPDATE)

                response: ReplicaResponse = replica.update(update)

            print(response)

            self.prev = response.label

            return response.value

        except ConnectionRefusedError:

            return "All replicas offline"

    def handle_replica_response(self, response: ReplicaResponse):

        print("Handling replica response...")


if __name__ == '__main__':

    print("Creating frontend...")

    daemon = Pyro4.Daemon()
    ns = Pyro4.locateNS()
    frontend = Frontend()

    uri = daemon.register(frontend)

    print("{0} running".format(frontend.id), end="\n\n")

    ns.register(frontend.id, uri, metadata={"resource:frontend"})

    daemon.requestLoop()
