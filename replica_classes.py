from typing import List, Dict

from Pyro4.util import SerializerBase

from requests import ClientRequest
from timestamp import Timestamp


class Record:
    """
    Represents an update request received from an FE. Stored in an RM's update log.
    """

    def __init__(self, i: str, ts: Timestamp, request: ClientRequest, prev: Timestamp, id: str):
        self.i = i  # ID of the RM that received the update request
        self.ts = ts  # RM's replica timestamp IF update is applied
        self.request = request  # Actual request from the client
        self.prev = prev  # Timestamp sent from the FE
        self.id = id  # Unique ID of the update, generated by the FE

    def __str__(self):
        return str(self.to_dict())

    def __eq__(self, other: 'Record') -> bool:
        return other.id is self.id

    def to_dict(self) -> Dict:
        """
        Used for serpent serialisation
        :return: A dict representing this Record
        """

        return {
            "__class__": "Record",
            "i": self.i,
            "ts": self.ts.to_dict(),
            "request": self.request.to_dict(),
            "prev": self.prev.to_dict(),
            "id": self.id
        }

    @staticmethod
    def from_dict(classname: 'str', dict: Dict) -> 'Record':
        """
        Used for serpent deserialisation
        :return: A Record
        """

        return Record(
            dict["i"],
            Timestamp.from_dict("Timestamp", dict["ts"]),
            ClientRequest.from_dict("ClientRequest", dict["request"]),
            Timestamp.from_dict("Timestamp", dict["prev"]),
            dict["id"]
        )


class Log:
    """
     Represents a list of Records. All update requests to an RM are transformed to Records and placed in an RM's Log as
     soon as they arrive. An RM keeps a Log either because updates are not stable or, although an update may have become
     stable and been applied to the RM's value, the RM has not received confirmation that the update has been received
     by all other RMs.

     An update is stable if it can be applied with the desired ordering.
     """

    def __init__(self, records=None):

        self.records: List[Record] = [] if records is None else records

    def __str__(self):

        return str(self.records)

    def __len__(self):

        return len(self.records)

    def __iadd__(self, record: Record):

        self.records.append(record)

        return self

    def __contains__(self, record_id: str):

        for record in self.records:

            if record_id == record.id:
                return True

        return False

    def stable(self, replica_ts: Timestamp) -> List[Record]:
        """
        When new Records have been merged into the Log, the RM collects any updates that are now stable.
        :param replica_ts: The replica Timestamp of the RM that owns this Log.
        :return: A List of stable Records, sorted by the partial order <= defined between Timestamps.
        """

        stable: [Record] = []

        for record in self.records:

            if record.ts <= replica_ts:
                stable.append(record)

        return sorted(stable, key=lambda record: record.ts)

    def merge(self, log: 'Log', replica_ts: Timestamp) -> None:
        """
        Merges another Log in-place. A Record r in the Log is merged unless record.ts <= replica_ts, in which case
        it is already in the Log or it has bee applied to the RM's value and then discarded
        :param log: The Log to be merged into this one
        :param replica_ts: The replica Timestamp of the RM that owns this Log.
        :return: None
        """

        for record in log.records:

            if (record.ts <= replica_ts) is False:

                if not (record.id in self):
                    self.records.append(record)

    def to_dict(self) -> Dict:
        """
        Used for serpent serialisation
        :return: A dict representing this Log
        """

        return {
            "__class__": "Log",
            "records": [record.to_dict() for record in self.records]
        }

    @staticmethod
    def from_dict(classname: str, dict: Dict) -> 'Log':
        """
        Used for serpent deserialisation
        :return: A Log
        """

        return Log(
            [Record.from_dict("Record", record) for record in dict["records"]]
        )


SerializerBase.register_class_to_dict(Record, Record.to_dict)
SerializerBase.register_dict_to_class("Record", Record.from_dict)

SerializerBase.register_class_to_dict(Log, Log.to_dict)
SerializerBase.register_dict_to_class("Log", Log.from_dict)
