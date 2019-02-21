from replica import Operation
import uuid


class FrontEndMessage:

    def __init__(self, operation: Operation, prev):

        self.id = uuid.uuid4()

        # The operation contains DATA. It's not an operation as they state it.

        self.operation = operation
        self.prev = prev