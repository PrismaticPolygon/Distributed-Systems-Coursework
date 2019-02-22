import queue
import sqlite3
import time
import uuid
# from frontend import Frontend
from typing import List, Any

import Pyro4

from enums import Status, Method
# from frontend_message import FrontEndMessage
from gossip_message import GossipMessage
from log import Log
from timestamp import Timestamp


@Pyro4.expose
class Replica(object):

    def __init__(self):

        self.id = uuid.uuid4()

    def create_db(self, user_id, movie_id, rating) -> None:

        connection = sqlite3.connect("./database/ratings.db")

        cursor = connection.cursor()

        cursor.execute("INSERT INTO ratings VALUES (%, %, %, %)".format(user_id, movie_id, rating, time.time()))

        connection.commit()

    def read_db(self, movie_id: str) -> float:

        connection = sqlite3.connect("./database/ratings.db")

        cursor = connection.cursor()

        print("Getting rating for {0}...".format(movie_id))

        cursor.execute("SELECT ROUND(AVG(rating)) FROM ratings WHERE movieId=?", (movie_id, ))

        rating = cursor.fetchone()[0]

        print("Calculated", rating, end="...\n")

        return rating

    def update_db(self, movie_id, user_id, rating) -> None:

        connection = sqlite3.connect("./database/ratings.db")

        cursor = self.connection.cursor()

        cursor.execute("UPDATE ratings SET rating=? WHERE (movieID=? AND userId=?)", (rating, movie_id, user_id,))

        connection.commit()

    # The value of the application state as maintained by the RM. Each RM is a state machine, which begins with a
    # specified initial value and is thereafter solely the result of applying update operations to that state.

    value_timestamp = Timestamp()
    # Represents updates currently reflected in the value. Contains one entry for every entry manager, and is updated
    # whenever an update operation is applied to the value.

    update_log: List[Log] = []
    # All update operations are recorded in the log as soon as they arrive, although they may be applied in a different
    # order. An update is stable if it can be applied consistently with the desired ordering; an arriving update is
    # stored until it is stable and can be applied. It then remains in the log until the RM receives confirmation that
    # all other RMs have received the update.

    # A RM keeps updates in the log either because it is not yet stable (i.e. cannot be applied consistently with its
    # ordering guarantees OR, even though the update has become stable and has been applied to the value, the RM has not
    # received confirmation that this update has been received at all other RMs.

    replica_timestamp = Timestamp()
    # Represents those updates that have been accepted by the RM (placed in the RM's update_log). Differs from the
    # value_timestamp because not all updates in the log are stable.

    executed_operation_table: List[int] = []
    # Maintained to prevent an update being applied twice. Is checked before an update is added to the log.
    # The same update may arrive at a given replica manager from a FE and in gossip messages from other RMs. To prevent
    # an update being applied twice, this table contains the unique FE IDs of updates that have been applied to the
    # value. The RM checks this table before adding an update to the log.

    timestamp_table: List[Timestamp] = []
    # Contains a vector timestamp for each other RM, derived from gossip messages.

    # Contains a vector timestamp for each other RM. Fill with timestamps from gossip messages ("updates")
    # Used to determine whether a log record is known everywhere
    # Every gossip message contains the timestamp of its sender

    # Contains a vector timestamp for each other RM, filled with timestamps that arrive from them in gossip messages.
    # Used to establish whether an update has been applied to all RMs.

    hold_back: queue.Queue = queue.Queue()
    id: int = 1

    # Querys and

    def query(self, query: Any):

        print("Received query", query, end="...\n")

        operation = query["op"]
        prev = query["prev"]    # The previous timestamp
        request = operation["request"]  # The request passed to the FE
        method: Method = Method(request["method"])  # The method passed to the client
        params = request["params"]

        if method == Method.CREATE:

            value = self.create_db(**params), prev

        elif method == Method.READ:

            value = self.read_db(**params), prev

        elif method == Method.UPDATE:

            value = self.update_db(**params), prev

        else:

            value = "Lol"

        result = {
            "value": value[0],  # Result of operation
            "label": self.value_timestamp
        }

        print("Returning", result, end="\n\n")

        return result

        # else:

            # self.hold_back.put(query)  # Might want a block or a timeout

            # print("Putting in hold-back queue")  # So when is this queue updated?

            # Either waits for the missing updates, or requests the updates from the RMs concerned.



            # That corresponds to the hold_back queue.

            # If it hasn't already seen it before (i.e. the id is NOT in... executed operations table

            # Prev is the current label possessed by the front-end.
            # Raise an exception here?

            # This returns... the value of the actual query, and the label itself.
            # Ah, so we don't even need multiple methods! Nice.

    def update(self, update: Any) -> None:

        # Let's get at least ONE of these working. Current block is the Operation class.

        print("Update received from FE")

        if self.compare_timestamps(update.prev):

            # Condition ensures that all the updates on which this update depends have already been applied.
            # If not met: check again when gossip messages arrive.

            # How do I check the update_log? Ah. Check whether the created log is already there?

            if update.id not in self.executed_operation_table:
                self.replica_timestamp.increment(self.id)

                ts = update.prev.copy()
                ts[self.id] = self.replica_timestamp.get(self.id)

                update_log = Log(self.id, ts, update.operation, update.prev, update.id)

                print("Adding to update log", update_log)

                self.update_log.append(update_log)



                # Increment the ith element. Implies that we're giving a key (or number) by the FE
                # on registration./

    # Exchange gossip messages when a replica finds that it is missing an update sent to one of its peers
    # that it needs to process a request.

    def ack(self):

        self.replica_timestamp[self.id] += 1

        return True

    def gossip(self, gossip: GossipMessage):

        print("Gossip received from an RM")

        # We either merge the arriving log with its own (as it may contain updates not seen by the receiving RM before);
        # apply any updates that may become stable and have not been executed before (stable updates in the arrived log
        # may in turn make pending updates become stable); and to eliminate records from the log and entries in the
        # executed operations table when it is known that updates have been applied everywhere and for which there is no
        # danger of repeats.

        # The RM uses the entries in its timestamp table to estimate which updates any other replica manager has not yet
        # received.

        self.merge_logs(gossip.log)

        self.replica_timestamp = self.replica_timestamp.merge(gossip.ts)

        # The replica manager collects the set S of any updates that are now stable. Then we sort them.

    def chat_shit(self):

        ns = Pyro4.locateNS()

        replicas = ns.list(metadata_all={"resource:replica"})

        print("Chatting shit to: ", replicas)


    def get_stable_updates(self):

        print("Getting stable updates")

        # An stable update is one that may be applied consistently with its ordering guarantees (causal, forced, or
        # immediate). Causal ordering takes into account causal relationships between messages, in that if a message
        # happens before another message in the distributed system, this soo-called causal relationship will be
        # preserved in the delivery of the associated messages at all processes.

        # If the issue of request r happened before the issue of request r', then any correct RM that handles r' handles
        # r before it. This is necessarily true, because we're just appending, right? But then.. up until when?
        # Would every gossip message not simply clear the update_log?

        # Aha! So: sort them in order of all the replica managers, right? Any update which... has ANY component greater than
        # those now held in the log...? Mayhaps let's leave the gossip stuff until tomorrow?

    def get_status(self) -> Status:

        return Status.random

    def merge_logs(self, log):

        print("Merging logs: ", self.log, log)

        for record in log:

            if self.compare_timestamps(record):

                self.update_log.append(record)


if __name__ == '__main__':

    print("Creating replica...")

    daemon = Pyro4.Daemon()
    ns = Pyro4.locateNS()
    replica = Replica()

    uri = daemon.register(replica)
    name = "replica-" + str(replica.id)
    ns.register(name, uri, metadata={"resource:replica"})

    print("Replica created at", uri, end="\n\n")

    frontends = ns.list(metadata_all={"resource:frontend"})

    for (fe_name, fe_uri) in frontends.items():

        frontend = Pyro4.Proxy(fe_uri)
        frontend.add_replica(name, uri)

    daemon.requestLoop()
