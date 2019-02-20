import Pyro4
import pandas as pd
import os
import socket
import random
import queue
import uuid
from typing import List, Dict, Any

WORKERNAME = "Worker_%d@%s" % (os.getpid(), socket.gethostname())
statuses = ["ACTIVE", "OVERLOADED", "OFFLINE"]


class Operation:

    i = 1


class FrontEndMessage:

    def __init__(self, operation: Operation, prev):

        self.id = uuid.uuid4()

        # The operation contains DATA. It's not an operation as they state it.

        self.operation = operation
        self.prev = prev


class Timestamp:

    replicas: Dict[int, int] = []

    def set(self, i: int, value: int) -> None:

        self.replicas[i] = value

    def increment(self, id: int) -> None:

        self.replicas[id] += 1

    def get(self, i: int) -> int:

        return self.replicas[i]

    def size(self) -> int:

        return len(self.replicas)

    def compare_timestamps(self, prev: 'Timestamp') -> bool:

        print("Comparing timestamps: ", self.replicas, prev)

        assert (prev.size() == self.size())

        for i in range(self.size()):

            if prev.get(i) > self.get(i):

                return False

        return True

    def merge_timestamps(self, ts: 'Timestamp') -> None:

        print("Merging timestamps: ", self.replicas, ts)

        assert(ts.size() == self.size())

        for x in range(len(self.replicas)):

            if ts.get(x) > self.replicas.get(x):

                self.replicas[x] = ts.get(x)


class GossipMessage:

    def __init__(self, ts: Timestamp, log):

        self.ts = ts
        self.log = log


class Log:

    def __init__(self, i: int, ts: Timestamp, op: Operation, prev: Timestamp, id: str):

        self.i = i
        self.ts = ts
        self.op = op
        self.prev = prev
        self.id = id

@Pyro4.expose
class Server(object):

    # Types, server-side, are CRUD. I will have already distinguished this at the FE, though.
    # I.e. no, this is untrue.
    # A CUD operation would be routed through query.

    value: pd.DataFrame = pd.read_csv("./database/ratings.csv")
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

    # def read(self, movie_id):
    #
    #     return self.value.loc[self.value["movieId"] == movie_id]["rating"].mean().round()

    def query(self, query: FrontEndMessage):

        operation = query.operation
        prev = query.prev

        if self.compare_timestamps(prev):

            print("Applying operation")

            return {
                "value": "",  # Result of operation
                "label": self.value_timestamp
            }

        else:

            self.hold_back.put(query)  # Might want a block or a timeout

            print("Putting in hold-back queue")     # So when is this queue updated?

            # Either waits for the missing updates, or requests the updates from the RMs concerned.



        # That corresponds to the hold_back queue.

        # If it hasn't already seen it before (i.e. the id is NOT in... executed operations table

        # Prev is the current label possessed by the front-end.
        # Raise an exception here?

        # This returns... the value of the actual query, and the label itself.
        # Ah, so we don't even need multiple methods! Nice.

    def update(self, update: FrontEndMessage) -> None:

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

        # We

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



    def get_status(self):

        return random.choice(statuses)


    def merge_logs(self, log):

        print("Merging logs: ", self.log, log)

        for record in log:

            if self.compare_timestamps(record):

                self.update_log.append(record)



daemon = Pyro4.Daemon()
uri = daemon.register(Server())

dispatcher = Pyro4.core.Proxy("PYRONAME:example.distributed.dispatcher")
dispatcher.add_server(uri)

# Okay. Now I can query the dispatcher for server URIs, and then send it directly.
# Or perhaps every server ought to maintain a list... Yeah, I like that. This is not as OO as I'd like.

daemon.requestLoop()

# So how do I get my URI out? I can look it up, if I know my object name

# And these are all in different threads. So there's no problem having it like this.

# def process(item):
#
#     print("Processing: ", item)
#
#     return 0
#
#
# def main():
#
#     dispatcher = Pyro4.core.Proxy("PYRONAME:example.distributed.dispatcher")
#
#     print("This is worker %s" % WORKERNAME)
#     print("getting work from dispatcher")
#
#     while True:
#
#         try:
#
#             item = dispatcher.getWork()
#
#         except ValueError:
#
#             print("No work available")
#
#         else:
#
#             process(item)
#             dispatcher.putResult(item)
#
#
# if __name__ == "__main__":
#
#     main()

# On creation...

# @Pyro4.expose
# class GreetingMaker(object):
#
#     def get_fortunate(self, name):
#
#         return "Hello, {}. Here is your fortune message:\nBehold the warranty --  the bold print giveth and the fine " \
#                "print giveth away".format(name)
#
#
# daemon = Pyro4.Daemon()
# uri = daemon.register(GreetingMaker)

# print("Ready. Object uri = ", uri)
# daemon.requestLoop()

# Ah. I'd presumably have to encapsulate this entire thing.

# We're going to need to load in the database.
# Shouldn't that be a simple query, though? Why not just host the back-end in three different places?
# Client interacts with front-end, they send off to "replica-managers"
# RM's periodically exchange gossip messages to cnvey the updates that they have received from their clients.
# The front-ends send queries and updates to any replica manager that they choose.cls

# Maybe I should just java this.

# Am I being stupid? Maybe have no index? Yeah.

# Okay! Nice. Now, I can have multiple ones of these.

# Now make this a Pyro object...



# Create object instances
# Give names to those instances, and register them with the name server
# Tell Pyro to take care of those instances
# Tell Pyro to sit idle in a loop waiting for incoming method calls

# The way I see it: I'm missing the distributor in the middle.

# What's the best way to query?
# We can switch to a mongoDB database later, I think. It rather defeats the purpose otherwise, right?
# Each one would need their own database. Which, in fact, is not crazy.
# Main should create the objects, name then, and assign them to particular names.

# Each server needs to be able to arbitrarily report itself as active, overloaded, of ffline.
# I think it's intended that I run these three, right? Is that possible?

# At least three servers should be implemented"
# Ah. So my front-end acts as this service-broker, essentially. B
# So I have my client program, a front-end server , and then my replications servers
# That's irrelevant for now. Let's just make the program/