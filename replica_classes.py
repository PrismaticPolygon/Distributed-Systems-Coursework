from typing import List, Dict
from timestamp import Timestamp
from requests import ClientRequest
from Pyro4.util import SerializerBase


class Record:

    def __init__(self, i: str, ts: Timestamp, op: ClientRequest, prev: Timestamp, id: str):

        self.i = i
        self.ts = ts
        self.op = op
        self.prev = prev
        self.id = id

    def __str__(self):

        return str(self.to_dict())

    def __eq__(self, other):

        return other.id is self.id

    def to_dict(self):

        return {
            "__class__": "Record",
            "i": self.i,
            "ts": self.ts.to_dict(),
            "op": self.op.to_dict(),
            "prev": self.prev.to_dict(),
            "id": self.id
        }

    @staticmethod
    def from_dict(classname: 'str', dict: Dict):

        return Record(
            dict["i"],
            Timestamp.from_dict("Timestamp", dict["ts"]),
            ClientRequest.from_dict("ClientRequest", dict["op"]),
            Timestamp.from_dict("Timestamp", dict["prev"]),
            dict["id"]
        )


class Log:

    def __init__(self, records=None):

        if records is None:

            records = []

        self.records: List[Record] = records

    def __str__(self):

        return str(self.records)

    def add(self, record: Record):

        self.records.append(record)

    def __contains__(self, id: str):

        for record in self.records:

            if record.id == id:

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

    def to_dict(self):

        return {
            "__class__": "Log",
            "records": [record.to_dict() for record in self.records]
        }

    @staticmethod
    def from_dict(classname: str, dict: Dict):

        return Log(
            [Record.from_dict("Record", record) for record in dict["records"]]
        )


class Gossip:

    def __init__(self, log: Log, ts: Timestamp):

        self.log = log
        self.ts = ts

    def __str__(self):

        return str(self.to_dict())

    def to_dict(self):

        return {
            "__class__": "Gossip",
            "log": self.log.to_dict(),
            "ts": self.ts.to_dict()
        }

    @staticmethod
    def from_dict(classname: str, dict: Dict):

        del dict["__class__"]

        return Gossip(
            Log.from_dict("Log", dict["log"]),
            Timestamp.from_dict("Timestamp", dict["ts"])
        )


SerializerBase.register_class_to_dict(Record, Record.to_dict)
SerializerBase.register_dict_to_class("Record", Record.from_dict)

SerializerBase.register_class_to_dict(Log, Log.to_dict)
SerializerBase.register_dict_to_class("Log", Log.from_dict)

SerializerBase.register_class_to_dict(Gossip, Gossip.to_dict)
SerializerBase.register_dict_to_class("Gossip", Gossip.from_dict)
