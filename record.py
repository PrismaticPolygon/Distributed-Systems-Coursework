from requests import Timestamp, ClientRequest
from typing import List


class Record:

    def __init__(self, i: str, ts: Timestamp, op: ClientRequest, prev: Timestamp, id: str):

        self.i = i
        self.ts = ts
        self.op = op
        self.prev = prev
        self.id = id

    def __eq__(self, other):

        return other.id is self.id


class Log:

    def __init__(self, records=None):

        if records is None:

            records = []

        self.records: List[Record] = records

    def __str__(self):

        return self.records

    def add(self, record: Record):

        self.records.append(record)

    def __contains__(self, other: Record):

        for record in self.records:

            if record.id == other.id:

                return True

        return False

    def stable(self, replica_ts: Timestamp) -> List[Record]:

        stable: [Record] = []

        for record in self.records:

            if record.ts <= replica_ts:

                stable.append(record)

        return sorted(stable, key=lambda record: record.ts)

    def merge(self, log: 'Log', replica_ts: Timestamp):

        for record in log.records:

            if record.ts <= replica_ts:

                self.records.append(record)
