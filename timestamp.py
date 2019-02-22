from typing import Dict
import Pyro4.util


class Timestamp:

    def __init__(self, replicas=None):

        if replicas is None:

            replicas = dict()

        self.replicas: Dict[int, int] = replicas

    def set(self, i: int, value: int) -> None:

        self.replicas[i] = value

    def add(self):

        self.replicas[self.size()] = 0

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


Pyro4.util.SerializerBase.register_class_to_dict(Timestamp, lambda x: {"replicas": x.replicas})
Pyro4.util.SerializerBase.register_dict_to_class(Timestamp, lambda x: Timestamp(x["replicas"]))
