import uuid
from typing import List, Dict

import Pyro4

from db import DB
from enums import Status
from replica_classes import Record, Log
from requests import ClientRequest, FrontendRequest, ReplicaResponse
from timestamp import Timestamp


@Pyro4.expose
class Replica(object):

    @Pyro4.expose
    @property
    def id(self):

        return self._id

    @Pyro4.expose
    @property
    def replica_timestamp(self):

        return self._replica_timestamp

    @Pyro4.expose
    @property
    def update_log(self):

        return self._update_log

    def __init__(self):

        self._id = "replica-" + str(uuid.uuid4())
        self._replicas: Dict[str, Pyro4.URI] = dict()

        # Represents updates currently reflected in the value. Contains one entry for every replica manager, and is
        # updated whenever an update operation is applied to the value.
        self.value_timestamp = Timestamp({self.id: 0})

        # Represents those updates that have been accepted by the RM (placed in the RM's update log). Differs from the
        # value_timestamp because not all updates in the log are stable.
        self._replica_timestamp = Timestamp({self.id: 0})

        # The Pyro name server. Storing it locally removes the overhead of re-locating it every time this RM gossips.
        self.ns = Pyro4.locateNS()

    db = DB()
    # The value of the application state as maintained by the RM. Each RM is a state machine, which begins with a
    # specified initial value and is thereafter solely the result of applying update operations to that state.

    _update_log = Log()

    executed_operation_table: List[str] = []
    # Maintained to prevent an update being applied twice. Is checked before an update is added to the log.
    # The same update may arrive at a given replica manager from a FE and in gossip messages from other RMs. To prevent
    # an update being applied twice, this table contains the unique FE IDs of updates that have been applied to the
    # value. The RM checks this table before adding an update to the log.

    timestamp_table: Dict[str, Timestamp] = {}
    # Contains a vector timestamp for each other RM. Fill with timestamps from gossip messages ("updates")
    # Used to determine whether a log record is known everywhere
    # Every gossip message contains the timestamp of its sender

    # Contains a vector timestamp for each other RM, filled with timestamps that arrive from them in gossip messages.
    # Used to establish whether an update has been applied to all RMs.

    def query(self, query: FrontendRequest) -> ReplicaResponse:
        """
        If this RM holds outdated information (i.e. FE's prev > RM's value), gossip, then complete the request.
        :param query: A FrontendRequest, comprising the FE's timestamp and the request from the client.
        :return: A ReplicaResponse, containing the requested value and this RM's value timestamp.
        """

        print("Received query from FE", query.prev)

        prev: Timestamp = query.prev    # The FE timestamp, representing the state of the information it last accessed.
        request: ClientRequest = query.request  # The request passed to the FE

        # q can be applied to the replica's value if q.prev <= valueTS. If it can't, gossip so that it can.
        if (prev <= self.value_timestamp) is False:

            self.gossip(prev)

        result = self.db.execute_request(request)

        return ReplicaResponse(result, self.value_timestamp)

    def update(self, update: FrontendRequest) -> ReplicaResponse:

        print("Received update from FE", update.prev)

        id: str = update.id
        prev: Timestamp = update.prev  # The previous timestamp
        request: ClientRequest = update.request  # The request passed to the FE

        if id not in self.executed_operation_table:

            if id not in self._update_log:

                self._replica_timestamp.replicas[self.id] += 1

                ts = prev.copy()
                ts.replicas[self.id] = self._replica_timestamp.replicas[self.id]

                record = Record(self.id, ts, request, prev, id)
                self._update_log += record

                if (record.prev <= self.value_timestamp) is False:

                    self.gossip(record.prev)

                else:

                    self.apply_update(record)

                return ReplicaResponse(None, ts)

        return ReplicaResponse(None, self.value_timestamp)  # Update has already been performed

    def apply_update(self, record: Record) -> None:

        self.db.execute_request(record.request)
        self.value_timestamp.merge(record.ts)
        self.executed_operation_table.append(record.id)

    def get_status(self) -> Status:

        return Status.random

    def gossip(self, prev: Timestamp):

        replicas_with_required_updates = self._replica_timestamp.lt(prev)

        for replica_id in replicas_with_required_updates:

            if replica_id in self._replicas:

                uri = self._replicas[replica_id]

            else:

                uri = self.ns.lookup(replica_id)
                self._replicas[replica_id] = uri

            with Pyro4.Proxy(uri) as replica:

                print("Gossiping with {0}".format(replica.id))

                log: Log = replica.update_log
                ts: Timestamp = replica.replica_timestamp
                old_log_length = len(self._update_log)

                self._update_log.merge(log, self._replica_timestamp)

                print("Merging update logs ({} new record(s))".format(len(self._update_log) - old_log_length))

                self._replica_timestamp.merge(ts)

                print("Merging replica timestamps", self._replica_timestamp)

                stable: List[Record] = self._update_log.stable(self._replica_timestamp)

                print("{} updates now stable".format(len(stable)))

                for record in stable:

                    if record.id not in self.executed_operation_table:

                        self.apply_update(record)

                        self.timestamp_table[id] = ts

                        # TODO: discard logs
                        # TODO: eliminate executed operation entries

        print("Finished gossiping")
# TODO: handle errors in replica, and gracefully shut down the process.

# Okay: now we're fine. Weird. We had multiple of each. I SUSPECT it's because the classes holding others don't
# delete then when they open then, essentially.


if __name__ == '__main__':

    print("Creating replica...")

    daemon = Pyro4.Daemon()
    replica = Replica()

    uri = daemon.register(replica)
    replica.ns.register(replica.id, uri, metadata={"resource:replica"})

    frontends = replica.ns.list(metadata_all={"resource:frontend"})

    for (fe_name, fe_uri) in frontends.items():

        with Pyro4.Proxy(fe_uri) as frontend:

            frontend.register_replica(replica.id, uri)

            print("Registering with {0}...".format(frontend.id))

    print("{0} running".format(replica.id), end="\n\n")

    daemon.requestLoop()

    # I suspect I'm not actually closing my loops./
