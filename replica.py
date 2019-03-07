import uuid
from typing import List, Dict

import Pyro4
from Pyro4.errors import CommunicationError

from db import DB
from enums import Status
from replica_classes import Record, Log
from requests import ClientRequest, FrontendRequest, ReplicaResponse
from timestamp import Timestamp


@Pyro4.expose
class Replica(object):
    # Accessor methods for properties of this RM; used for gossip
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

        self._id = "replica-" + str(uuid.uuid4())  # This RM's ID

        # Represents updates currently reflected in the value. Contains one entry for every replica manager, and is
        # updated whenever an update operation is applied to the value.
        self.value_timestamp = Timestamp({self.id: 0})

        # Represents those updates that have been accepted by the RM (placed in the RM's update log). Differs from the
        # value timestamp because not all updates in the log are stable.
        self._replica_timestamp = Timestamp({self.id: 0})

        # The Pyro name server. Storing it locally removes the overhead of re-locating it every time this RM gets
        # replicas_with_updates.
        self.ns = Pyro4.locateNS()

        # This RM's update log, containing Records.
        self._update_log = Log()

    # The value of the application state as maintained by the RM. Each RM is a state machine, which begins with a
    # specified initial value and is thereafter solely the result of applying update operations to that state.
    database = DB()

    # The same update may arrive at a given replica manager from a FE and in gossip messages from other RMs. To prevent
    # an update being applied twice, this table contains the unique FE IDs of updates that have been applied to the
    # value. The RM checks this table before adding an update to the log.
    executed_operation_table: List[str] = []

    # Contains a vector timestamp for each other RM, filled with timestamps that arrive from them in gossip messages.
    # Used to establish whether an update has been applied to all RMs.
    timestamp_table: Dict[str, Timestamp] = {}

    def query(self, query: FrontendRequest) -> ReplicaResponse:
        """
        Execute a query from an FE. If this RM holds outdated information (i.e. FE's prev > RM's value), gossip, then
        execute the query.
        :param query: A FrontendRequest, comprising the FE's timestamp and the request from the client.
        :return: A ReplicaResponse, containing the requested value and this RM's value timestamp.
        """

        print("Received query from FE", query.prev, end="\n\n")

        prev: Timestamp = query.prev  # The FE timestamp, representing the state of the information it last accessed.
        request: ClientRequest = query.request  # The request passed to the FE

        # q can be applied to the replica's value if q.prev <= valueTS. If it can't, gossip so that it can.
        if (prev <= self.value_timestamp) is False:
            self.gossip(prev)

        result = self.database.execute_request(request)

        return ReplicaResponse(result, self.value_timestamp)

    def update(self, update: FrontendRequest) -> ReplicaResponse:
        """
        Execute an update from an FE. If this RM holds outdated information (i.e. FE's prev > RM's value), gossip, then
        execute the update.
        :param update: A FrontendRequest, comprising the FE's timestamp and the request from the client.
        :return: A ReplicaResponse, containing a database message and this RM's value timestamp.
        """

        print("Received update from FE", update.prev, end="\n\n")

        id: str = update.id
        prev: Timestamp = update.prev  # The previous timestamp
        request: ClientRequest = update.request  # The request passed to the FE

        if id not in self.executed_operation_table:  # Update has not already been applied

            if id not in self._update_log:  # Update has not been seen before

                self._replica_timestamp.replicas[self.id] += 1  # This RM has accepted an update

                if (prev <= self.value_timestamp) is False:  # If we're missing information, gossip

                    self.gossip(prev)

                ts = prev.copy()
                ts.replicas[self.id] = self._replica_timestamp.replicas[self.id]  # Update the timestamp to reflect this

                record = Record(self.id, ts, request, prev, id)  # Create a record of this update
                self._update_log += record  # Add it to the log

                result = self.apply_update(record)

                return ReplicaResponse(result, ts)

        return ReplicaResponse("Update has already been performed", self.value_timestamp)

    def apply_update(self, record: Record) -> str:
        """
        Apply an update, held within a Record in this RM's update log
        :param record: a stable Record in this RM's update log
        :return: a message from the Database
        """

        self.value_timestamp.merge(record.ts)  # Merge this RM's value timestamp with the timestamp of the record
        self.executed_operation_table.append(record.id)  # Add the record's ID to the executed operation table

        return self.database.execute_request(record.request)  # Execute the request

    def get_status(self) -> Status:
        """
        :return: An arbitrary status
        """

        return Status.random

    def apply_gossip(self, replica: 'Replica'):

        print("Gossiping with {0}".format(replica.id))

        log: Log = replica.update_log  # The RM's update_log
        ts: Timestamp = replica.replica_timestamp  # The RM's replica timestamp
        old_log_length = len(self._update_log)

        self._update_log.merge(log, self._replica_timestamp)  # Merge update logs

        print("Merging update logs ({} new record(s))".format(len(self._update_log) - old_log_length))

        self._replica_timestamp.merge(ts)  # Merge replica timestamps

        print("New replica timestamp", self._replica_timestamp)

        stable: List[Record] = self._update_log.stable(self._replica_timestamp)  # Get stable records
        applied_updates = 0

        print()

        for record in stable:  # Iterate through stable records

            if record.id not in self.executed_operation_table:  # If the record has not already been applied

                self.apply_update(record)  # Apply the update

                self.timestamp_table[id] = ts  # Update the timestamp table

                applied_updates += 1

        print("Applied {} stable update(s)\n".format(applied_updates))

    def gossip(self, prev: Timestamp) -> None:
        """
        Gossip with other replicas.
        :param prev:
        :return: None
        """

        for replica_id in self._replica_timestamp.compare(prev):  # Iterate through a  list of RMs that this RM needs
            # updates from

            uri = self.ns.lookup(replica_id)  # Get the URI of the RM

            try:

                with Pyro4.Proxy(uri) as replica:

                    self.apply_gossip(replica)  # Apply gossip

            except CommunicationError:

                print("{} with required updates reporting Status.OFFLINE\n".format(replica_id))  # A replica with a
                # required update is offline. Shouldn't matter, as requests are sent to multiple RMs when they are
                # received at a FM.

        print("Finished gossiping\n")


if __name__ == '__main__':
    print("Creating replica...")

    daemon = Pyro4.Daemon()
    replica = Replica()

    uri = daemon.register(replica)
    replica.ns.register(replica.id, uri, metadata={"resource:replica"})

    print("{0} running".format(replica.id), end="\n\n")

    daemon.requestLoop()
