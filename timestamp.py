from typing import Dict


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
