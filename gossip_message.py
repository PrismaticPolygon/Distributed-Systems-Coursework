from timestamp import Timestamp


class GossipMessage:

    def __init__(self, ts: Timestamp, log):

        self.ts = ts
        self.log = log
