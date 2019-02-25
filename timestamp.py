from typing import Dict
from Pyro4.util import SerializerBase


# I could cheat by using str to serialise it. I'd rather not, though: let's see if an error is thrown.
# https://stackoverflow.com/questions/4555932/public-or-private-attribute-in-python-what-is-the-best-way

class Timestamp:

    def __init__(self, replicas=None):

        if replicas is None:

            replicas = dict()

        self.replicas: Dict[str, int] = replicas
        self.frontend_updates = 0

    def __str__(self):

        return str(self.to_dict())

    def __le__(self, other: 'Timestamp') -> bool:

        print("Comparing ", other.replicas, self.replicas)


        ids = set()

        ids.update(list(other.replicas.keys()))
        ids.update(list(self.replicas.keys()))

        for id in ids:

            if id in self.replicas and id not in other.replicas:

                if self.replicas.get(id) > 0:

                    return False

            if id in self.replicas and id in other.replicas:

                if other.replicas.get(id) < self.replicas.get(id):

                    return False

            if id not in self.replicas and id in other.replicas:

                self.add(id)    # The id hasn't been seen before, so no updates have been read from it.

                if other.replicas.get(id) > 0:

                    return False

        return True

    def __lt__(self, other: 'Timestamp') -> bool:

        for (id, value) in other.replicas.items():

            if id in self.replicas:

                if value <= self.get(id):

                    return False

            else:

                self.add(id)    # This is fine: it's 0, so must be less than. Wait. This is only for sorting, after all.

        return True

    def __contains__(self, id: str):

        return id in self.replicas

    def __eq__(self, other) -> bool:

        for (id, value) in self.replicas.items():

            if other.get(id) != value:

                return False

        return True

    def set(self, i: str, value: int) -> None:

        self.replicas[i] = value

    def add(self, id: str):

        self.replicas[id] = 0

    def increment(self, id: str) -> None:

        self.replicas[id] += 1

    def get(self, i: str) -> int:

        return self.replicas[i]

    def size(self) -> int:

        return len(self.replicas)

    def copy(self) -> 'Timestamp':

        return Timestamp(self.replicas.copy())

    def merge(self, ts: 'Timestamp') -> None:

        for (id, value) in ts.replicas.items():

            if id not in self.replicas:

                self.replicas[id] = value

            elif value > self.replicas.get(id):

                self.replicas[id] = ts.get(id)

    def to_dict(self):

        return {
            "__class__": "Timestamp",
            "replicas": self.replicas
        }

    @staticmethod
    def from_dict(classname: str, dict: Dict):

        del dict["__class__"]

        return Timestamp(**dict)


SerializerBase.register_class_to_dict(Timestamp, Timestamp.to_dict)
SerializerBase.register_dict_to_class('Timestamp', Timestamp.from_dict)
