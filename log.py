from timestamp import Timestamp
from operation import Operation


class Log:

    def __init__(self, i: int, ts: Timestamp, op: Operation, prev: Timestamp, id: str):

        self.i = i
        self.ts = ts
        self.op = op
        self.prev = prev
        self.id = id