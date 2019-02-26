import uuid
import Pyro4
from enums import Status, Method
from replica import Replica
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

    def add_replica(self, id, uri) -> None:

        print("{0} added".format(id))

        self.replicas[id] = uri
        self.prev.replicas[id] = 0

    def get_replica(self) -> Replica:

        overloaded = None

        for (name, uri) in self.replicas.items():

            replica: Replica = Pyro4.Proxy(uri)
            status: Status = Status(replica.get_status())

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

        print("\n{0} requested...".format(request))

        try:

            replica: Replica = self.get_replica()

            if request.method is Method.READ:

                query: FrontendRequest = FrontendRequest(self.prev, request)

                print("Sent {0}".format(self.prev))

                response: ReplicaResponse = replica.query(query)

            else:

                update: FrontendRequest = FrontendRequest(self.prev, request)

                print("Sent {0}".format(self.prev))

                response: ReplicaResponse = replica.update(update)

            print("Got  {0}".format(response.label))

            self.prev = response.label

            return response.value

        except ConnectionRefusedError:

            return "All replicas offline"


if __name__ == '__main__':

    print("Creating frontend...")

    daemon = Pyro4.Daemon()
    ns = Pyro4.locateNS()
    frontend = Frontend()

    uri = daemon.register(frontend)

    # So this is erroring without reporting it!

    print("{0} running".format(frontend.id), end="\n\n")

    ns.register(frontend.id, uri, metadata={"resource:frontend"})

    daemon.requestLoop()
