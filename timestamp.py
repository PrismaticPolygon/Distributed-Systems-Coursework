from typing import Dict, Iterator

from Pyro4.util import SerializerBase


class Timestamp:
    def __init__(self, replicas: Dict[str, int] = None):

        self.replicas: Dict[str, int] = replicas if replicas is not None else dict()

    def __str__(self):

        return str(self.replicas)

    def __le__(self, other: 'Timestamp') -> bool:
        """
        Overrides the "<=" operator. If any entry in `other` is greater, returns True.
        :param other: The Timestamp to compare with
        :return: A boolean
        """

        for (id, value) in other.replicas.items():

            if id not in self.replicas:

                self.replicas[id] = 0

            elif value < self.replicas[id]:

                return False

        for (id, value) in self.replicas.items():

            if id not in other.replicas and value > 0:
                return False

        return True

    def __lt__(self, other: 'Timestamp') -> bool:
        """
        Overrides the "<" operator. Used to sort Timestamps when getting stable updates.
        :param other: The Timestamp to compare with
        :return: A boolean
        """

        return self <= other

    def compare(self, other: 'Timestamp') -> Iterator[str]:
        """
        Get an Iterator of all replica IDs for which `other` has greater entries. Used to selectively gossip with other
        RMs.
        :param other: The Timestamp to compare with
        :return: An Iterator of the replica IDs for which `other` had greater entries
        """

        def filter_replica(replica):

            id = replica[0]
            value = replica[1]

            if id not in self.replicas:

                self.replicas[id] = 0

                if value > 0:
                    return True

            elif value < self.replicas[id]:

                return False

            return True

        return map(lambda entry: entry[0], filter(filter_replica, other.replicas.items()))

    def copy(self) -> 'Timestamp':
        """
        Creates a copy of this Timestamp. Used to create the Timestamp held by a Record
        :return: A copy of this Timestamp
        """

        return Timestamp(self.replicas.copy())

    def merge(self, ts: 'Timestamp') -> None:
        """
        Merges `ts` into this Timestamp. An entry is merged if it does not already exist or if the value is greater than
        that currently held by this Timestamp. An entry contains the ID of an RM and the number of operations it has
        performed.
        :param ts:
        :return:
        """

        for (id, value) in ts.replicas.items():

            if id not in self.replicas:

                self.replicas[id] = value

            elif value > self.replicas[id]:

                self.replicas[id] = ts.replicas[id]

    def to_dict(self) -> Dict:
        """
        Used for serpent serialisation
        :return: A dict representing this Timestamp
        """

        return {
            "__class__": "Timestamp",
            "replicas": self.replicas
        }

    @staticmethod
    def from_dict(classname: str, dict: Dict) -> 'Timestamp':
        """
        Used for serpent deserialisation
        :return: A Log
        """

        return Timestamp(dict["replicas"])


SerializerBase.register_class_to_dict(Timestamp, Timestamp.to_dict)
SerializerBase.register_dict_to_class('Timestamp', Timestamp.from_dict)
