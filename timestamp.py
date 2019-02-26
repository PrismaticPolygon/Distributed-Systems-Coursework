from typing import Dict, List
from Pyro4.util import SerializerBase

# https://stackoverflow.com/questions/4555932/public-or-private-attribute-in-python-what-is-the-best-way

class Timestamp:

    def __init__(self, replicas=None):

        self.replicas: Dict[str, int] = replicas if replicas is not None else dict()

    def __str__(self):

        return str(self.replicas)

    def __le__(self, other: 'Timestamp') -> bool:

        for (id, value) in other.replicas.items():

            if id not in self.replicas:

                self.replicas[id] = 0

                if value > 0:

                    return False

            elif value < self.replicas[id]:

                return False

        for (id, value) in self.replicas.items():

            if id not in other.replicas and value > 0:

                return False

        return True

    # It's really not that clear, though, and it'd be hard to check. I'll extract this, and deal with it later.
    # __le__ then can just check the length of this list. Smart, huh?

    # No type 'merge' apparently I'm redi

    def lt(self, other: 'Timestamp'):

        lt = []

        def inner(replica_id: str, value: int):

            if replica_id not in self.replicas:

                self.replicas[id] = 0

                if value > 0:

                    return True

            elif value < self.replicas[replica_id]:

                return False

        for (id, value) in other.replicas.items():

            if inner(id, value):

                lt.append(id)

        return lt

    def __lt__(self, other: 'Timestamp') -> bool:

        # That's a good point, after all: what does it mean for it to be sorted?
        # It'd have to be in order for each replica, right?

        return self <= other

    def copy(self) -> 'Timestamp':

        return Timestamp(self.replicas.copy())

    def merge(self, ts: 'Timestamp') -> None:

        for (id, value) in ts.replicas.items():

            if id not in self.replicas:

                self.replicas[id] = value

            elif value > self.replicas[id]:

                self.replicas[id] = ts.replicas[id]

    def to_dict(self) -> Dict:

        return {
            "__class__": "Timestamp",
            "replicas": self.replicas
        }

    @staticmethod
    def from_dict(classname: str, dict: Dict) -> 'Timestamp':

        del dict["__class__"]

        return Timestamp(**dict)


SerializerBase.register_class_to_dict(Timestamp, Timestamp.to_dict)
SerializerBase.register_dict_to_class('Timestamp', Timestamp.from_dict)
