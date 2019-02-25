import queue
import uuid
from typing import List, Any, Dict
from db import DB

import Pyro4
from requests import ClientRequest, FrontendRequest, ReplicaResponse
from timestamp import Timestamp

from enums import Status, Method
from replica_classes import Record, Log, Gossip, StabilityError


@Pyro4.expose
class Replica(object):

    def __init__(self):

        self.id = "replica-" + str(uuid.uuid4())

        # Represents updates currently reflected in the value. Contains one entry for every replica manager, and is
        # updated whenever an update operation is applied to the value.
        self.value_timestamp = Timestamp({self.id: 0})

        # Represents those updates that have been accepted by the RM (placed in the RM's update_log). Differs from the
        # value_timestamp because not all updates in the log are stable.
        self.replica_timestamp = Timestamp({self.id: 0})

    value = DB()
    # The value of the application state as maintained by the RM. Each RM is a state machine, which begins with a
    # specified initial value and is thereafter solely the result of applying update operations to that state.

    update_log = Log()
    # All update operations are recorded in the log as soon as they arrive, although they may be applied in a different
    # order. An update is stable if it can be applied consistently with the desired ordering; an arriving update is
    # stored until it is stable and can be applied. It then remains in the log until the RM receives confirmation that
    # all other RMs have received the update.

    # A RM keeps updates in the log either because it is not yet stable (i.e. cannot be applied consistently with its
    # ordering guarantees OR, even though the update has become stable and has been applied to the value, the RM has not
    # received confirmation that this update has been received at all other RMs.

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

    # When we add a request to the log, that's when we update the replica time-stamp!

    def query(self, query: FrontendRequest) -> ReplicaResponse:

        print("Received query from FE", query.prev, end="...\n")

        prev: Timestamp = query.prev    # The previous timestamp
        request: ClientRequest = query.request  # The request passed to the FE

        # q can be applied to the replica's value if q.prev <= valueTS
        if prev <= self.value_timestamp:

            result = self.execute_client_request(request)

        # RM is holding out-dated information: request gossip from the other RMs and then respond.
        else:

            self.request_gossip()

            result = self.execute_client_request(request)

        return ReplicaResponse(result, self.value_timestamp)

    # TODO: request gossip from specific managers

    def update(self, update: FrontendRequest) -> ReplicaResponse:

        print("Received update from FE", update.prev, end="...\n")

        id: str = update.id
        prev: Timestamp = update.prev  # The previous timestamp
        request: ClientRequest = update.request  # The request passed to the FE

        print(id, request)

        # Wow, we don't even get here!

        if id not in self.executed_operation_table:

            if id not in self.update_log:

                print("Incrementing replica")

                self.replica_timestamp.replicas[self.id] += 1

                ts = prev.copy()
                ts.replicas[self.id] = self.replica_timestamp.replicas[self.id]

                print("Copy ts")

                record = Record(self.id, ts, request, prev, id)

                print("Created record")

                # Could get out of sync HERE!

                self.update_log.add(record)

                print("Updated log")

                if (record.prev <= self.value_timestamp) is False:

                    self.request_gossip()

                else:

                    self.apply_update(record)

                return ReplicaResponse(None, ts)

        return ReplicaResponse(None, self.value_timestamp)  # Update has already been performed

    # At the moment, the replicas that we're goss

    def apply_update(self, record: Record) -> None:

        # I think I'm perhaps not updating this correctly.

        self.execute_client_request(record.op)
        self.value_timestamp.merge(record.ts)
        self.executed_operation_table.append(record.id)

    def receive_gossip(self, gossip: Gossip, id):

        print("{0} gossiped {1}".format(id, gossip.ts))

        # Interesting. I got 'old' gossip.
        # And it wasn't QUITE in order.

        log: Log = gossip.log
        ts: Timestamp = gossip.ts

        print("New records:", len(log.records))

        self.update_log.merge(log, self.replica_timestamp)
        self.replica_timestamp.merge(ts)

        print("New replica timestamp:", self.replica_timestamp)

        stable: List[Record] = self.update_log.stable(self.replica_timestamp)

        print("Stable:", stable)

        for record in stable:

            # Shouldn't need to worry about this now. It's stopped -0b3

            if record.id not in self.executed_operation_table:

                self.apply_update(record)

                self.timestamp_table[id] = ts

            # TODO: discard logs
            # TODO: eliminate executed operation entries

    def request_gossip(self):

        print("Requesting gossip...")

        ns = Pyro4.locateNS()
        replicas = ns.list(metadata_all={"resource:replica"})

        for (name, uri) in replicas.items():

            if name != self.id:

                print("Requesting gossip from", name, end="...\n")

                replica: Replica = Pyro4.Proxy(uri)

                replica.gossip()

        print("Finished requesting gossip\n")

    def gossip(self):

        print("Gossiping...")

        ns = Pyro4.locateNS()
        replicas = ns.list(metadata_all={"resource:replica"})

        gossip = Gossip(self.update_log, self.replica_timestamp)

        for (name, uri) in replicas.items():

            if name != self.id:

                print("Gossiping with", name, end="...\n")

                replica: Replica = Pyro4.Proxy(uri)

                replica.receive_gossip(gossip, self.id)

        print("Finished gossiping", end="\n\n")

    def get_status(self) -> Status:

        return Status.random

    def execute_client_request(self, request: ClientRequest):

        print("Executing client request", request)

        method: Method = request.method  # The method passed to the client
        params = request.params
        value = None

        if method == Method.CREATE:

            self.value.create(**params)

        elif method == Method.READ:

            value = self.value.read(**params)

        elif method == Method.UPDATE:

            self.value.update(**params)

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
