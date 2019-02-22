import uuid

import Pyro4

from enums import Status, Operation, Method
from replica import Replica

# TODO: fix race condition on replica creation

@Pyro4.behavior(instance_mode="single")
class Frontend(object):

    def __init__(self):

        self.id = uuid.uuid4()

    replicas = {}
    # A dictionary mapping replica names to their Pyro URIs. Places the overhead of a Pyro NS search on RM creation,
    # rather than every operation an FE performs.

    prev = {}
    # Reflects the version of the latest data values accessed by the FE. Contains an entry for every RM. Sent by the FE
    # in every request message to an RM, together with a description of the query or update operation itself. When a RM
    # returns a value as the result of a query operation, it supplies a new vector timestamp, since the RMs may be been
    # updated since the last operation. Each update operation returns a vector timestamp that is unique to the update;
    # each returned timestamp is merged with the FE's previous timestamp to record the version of the replicated data
    # observed by the client.

    @Pyro4.expose
    def add_replica(self, name, uri) -> None:

        print("Replica added", (name, uri))

        self.replicas[name] = uri

    def get_replica(self) -> Replica:

        print("Getting replica... ")

        overloaded = None

        for (name, uri) in self.replicas.items():

            replica: Replica = Pyro4.Proxy(uri)
            status: Status = Status(replica.get_status())

            print(name + " reporting status:", status)

            if status is Status.ACTIVE:

                print("\nUsing {0} ({1})".format(name, status), end="\n\n")

                return replica

            elif status is Status.OVERLOADED:

                overloaded = (name, uri, status, replica)

        if overloaded is not None:

            print("\nUsing {0} ({1})".format(overloaded[0], overloaded[2]), end="\n\n")

            return overloaded[3]

        raise ConnectionRefusedError("All replicas offline")

    @Pyro4.expose
    def request(self, request):

        print("\nRequest received:", request)

        query = {
            "id": uuid.uuid4(),
            "prev": self.prev,
            "op": {
                "request": request
            }
        }

        replica: Replica = self.get_replica()
        response = None

        if Method(request["method"]) is Method.READ:

            query["op"]["type"] = Operation.QUERY

            response = replica.query(query)

        else:

            query["op"]["type"] = Operation.UPDATE
            replica.update(query)

        print(response)

        if query["op"]["type"] is Operation.UPDATE:

            self.prev = response["label"]

        else:

            return response["value"]

        return response["value"]


if __name__ == '__main__':

    print("Creating frontend...")

    daemon = Pyro4.Daemon()
    ns = Pyro4.locateNS()
    frontend = Frontend()

    uri = daemon.register(frontend)

    print("Frontend created at", uri, end="\n\n")

    ns.register("frontend-" + str(frontend.id), uri, metadata={"resource:frontend"})

    daemon.requestLoop()
