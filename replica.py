import queue
import uuid
from typing import List, Any, Dict
from db import DB

import Pyro4
from requests import ClientRequest, FrontendRequest, ReplicaResponse, StabilityError
from timestamp import Timestamp

from enums import Status, Method
from replica_classes import Record, Log, Gossip


@Pyro4.expose
class Replica(object):

    def __init__(self):

        self.id = "replica-" + str(uuid.uuid4())
        self.value_timestamp = Timestamp({self.id: 0})
        self.replica_timestamp = Timestamp({self.id: 0})

    value = DB()
    # The value of the application state as maintained by the RM. Each RM is a state machine, which begins with a
    # specified initial value and is thereafter solely the result of applying update operations to that state.

    # value_timestamp = Timestamp()
    # Represents updates currently reflected in the value. Contains one entry for every entry manager, and is updated
    # whenever an update operation is applied to the value.

    update_log = Log()
    # All update operations are recorded in the log as soon as they arrive, although they may be applied in a different
    # order. An update is stable if it can be applied consistently with the desired ordering; an arriving update is
    # stored until it is stable and can be applied. It then remains in the log until the RM receives confirmation that
    # all other RMs have received the update.

    # A RM keeps updates in the log either because it is not yet stable (i.e. cannot be applied consistently with its
    # ordering guarantees OR, even though the update has become stable and has been applied to the value, the RM has not
    # received confirmation that this update has been received at all other RMs.

    # replica_timestamp = Timestamp()
    # Represents those updates that have been accepted by the RM (placed in the RM's update_log). Differs from the
    # value_timestamp because not all updates in the log are stable.

    executed_operation_table: List[str] = []
    # Maintained to prevent an update being applied twice. Is checked before an update is added to the log.
    # The same update may arrive at a given replica manager from a FE and in gossip messages from other RMs. To prevent
    # an update being applied twice, this table contains the unique FE IDs of updates that have been applied to the
    # value. The RM checks this table before adding an update to the log.

    timestamp_table: Dict[str, Timestamp] = {}
    # Contains a vector timestamp for each other RM, derived from gossip messages.

    # Contains a vector timestamp for each other RM. Fill with timestamps from gossip messages ("updates")
    # Used to determine whether a log record is known everywhere
    # Every gossip message contains the timestamp of its sender

    # Contains a vector timestamp for each other RM, filled with timestamps that arrive from them in gossip messages.
    # Used to establish whether an update has been applied to all RMs.

    hold_back: queue.Queue = queue.Queue()

    def query(self, query: FrontendRequest) -> ReplicaResponse:

        print("Received query from FE", query, end="...\n")

        prev: Timestamp = query.prev    # The previous timestamp
        request: ClientRequest = query.request  # The request passed to the FE

        # Ah, it's the old timestamp one! That's going to prove difficult. I could add it on replica creation, perhaps.

        # q can be applied to the replica's value if q.prev <= valueTS
        if prev <= self.value_timestamp:

            result = self.execute_client_request(request)

            return ReplicaResponse(result, self.value_timestamp)

        else:

            self.hold_back.put(query)  # Might want a block or a timeout # Throw an error.

            raise StabilityError("RM is holding out-dated information")

    def update(self, update: FrontendRequest) -> ReplicaResponse:

        print("Received update from FE", update, end="...\n")

        id: str = update.id
        prev: Timestamp = update.prev  # The previous timestamp
        request: ClientRequest = update.request  # The request passed to the FE

        if id not in self.executed_operation_table:

            print("id not in XO table")

            if id not in self.update_log:

                print("id not in update log")

                if prev <= self.value_timestamp:

                    # So we're failing at this comparison. Why? I'm passing the wrong thing, somewhere...

                    print("Passed prev check")

                    self.replica_timestamp.increment(self.id)

                    ts = prev.copy()
                    ts.set(self.id, self.replica_timestamp.get(self.id))

                    log = Record(self.id, ts, request, prev, id)

                    self.update_log.add(log)

                    result = self.apply_update(log)

                    print("Update result:", result)

                    return ReplicaResponse(result, self.value_timestamp)

                else:

                    # We've got an error boss!

                    self.chat_shit()

                    # And, having chatted shit, do something.
                    # But there's a real problem here. I can't leave messages hanging.
                    # The original queue method may in fact have been superior.
                    # But how would I know where to direct my responses to?
                    # I only have one client... but I'm determined to make this applicable to as many as I might want.

    def apply_update(self, record: Record) -> Any:

        if record.prev <= self.value_timestamp:

            print("Applying update:", record)

            result = self.execute_client_request(record.op)
            self.value_timestamp.merge(record.ts)
            self.executed_operation_table.append(record.id)

            return result

        else:

            raise StabilityError

    def gossip(self, gossip: Gossip, id):

        # *sigh* hasn't been deserialised properly. Who knows why.

        print(type(gossip))

        print("{0} gossiped {1}".format(id, gossip))

        log: Log = gossip.log
        ts: Timestamp = gossip.ts

        print("Merging update log")

        self.update_log.merge(log, self.replica_timestamp)

        print("Merging replica timestamp")

        self.replica_timestamp.merge(ts)

        print("Getting stable records")

        stable: List[Record] = self.update_log.stable(self.replica_timestamp)

        # No stable records. But why?

        for record in stable:

            self.apply_update(record)

            self.timestamp_table[id] = ts

            # But what do I do here?!

            # TODO: discard logs
            # TODO: eliminate executed operation entries

    def chat_shit(self):

        print("Gossip...")

        ns = Pyro4.locateNS()

        replicas = ns.list(metadata_all={"resource:replica"})

        # Shouldn't that make it a serialisation error? Let's restructure now, shall we?

        gossip = Gossip(self.update_log, self.replica_timestamp)

        for (name, uri) in replicas.items():

            if name != self.id:

                print("Gossiping with", name, end="...\n")

                replica: Replica = Pyro4.Proxy(uri)

                replica.gossip(gossip, self.id)

                # Only seemed to work with the latter.

            # Once I've chatted shit... try again? I need to tell everyone that I've received their gossip, right?

        # Ah. I can trust that the operation has been run, and it's an update, so I don't return anything! Of course it fucking hangs!

        print("Finished gossiping", end="\n\n")

    def get_status(self) -> Status:

        return Status.random

    def execute_client_request(self, request: ClientRequest):

        print("Executing client request", request)

        method: Method = request.method  # The method passed to the client
        params = request.params
        value = None

        if method == Method.CREATE:

            value = self.value.create(**params)

        elif method == Method.READ:

            value = self.value.read(**params)

        elif method == Method.UPDATE:

            value = self.value.update(**params)

        return value


if __name__ == '__main__':

    print("Creating replica...")

    # How does a replica know? Ah. If, when compared, it doesn't contain the entry, add it!
    # For the timestamps, I mean. Right?

    daemon = Pyro4.Daemon()
    ns = Pyro4.locateNS()
    replica = Replica()

    uri = daemon.register(replica)
    ns.register(replica.id, uri, metadata={"resource:replica"})

    print("{0} running".format(replica.id), end="\n\n")

    frontends = ns.list(metadata_all={"resource:frontend"})

    for (fe_name, fe_uri) in frontends.items():

        frontend = Pyro4.Proxy(fe_uri)
        frontend.add_replica(replica.id, uri)

        # print("{0} registered with {1}".format(replica.id, frontend.id))

    print("\n")

    daemon.requestLoop()
