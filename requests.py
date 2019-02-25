from enums import Method, Operation
from typing import Dict, Any
from Pyro4.util import SerializerBase
import Pyro4
import uuid
from timestamp import Timestamp

Pyro4.config.SERIALIZER = "serpent"


class StabilityError(Exception):
    pass


class ClientRequest:

    def __init__(self, method: Method, params: Dict):

        self.method = method
        self.params = params

    def __str__(self):

        return str(self.to_dict())

    def to_dict(self):

        return {
            "__class__": "ClientRequest",
            "method": self.method,
            "params": self.params
        }

    @staticmethod
    def from_dict(classname: str, dict: Dict):

        return ClientRequest(
            Method(dict["method"]),
            dict["params"]
        )


class FrontendRequest:

    def __init__(self, prev: Timestamp, request: ClientRequest, operation: Operation, id: str=None):

        if id is None:

            id = str(uuid.uuid4())

        self.id = id
        self.prev = prev
        self.request = request
        self.operation = operation

    def __str__(self):

        return str(self.to_dict())

    def to_dict(self):

        return {
            "__class__": "FrontendRequest",
            "prev": self.prev.to_dict(),
            "request": self.request.to_dict(),
            "operation": self.operation,
            "id": self.id
        }

    @staticmethod
    def from_dict(classname: str, dict: Dict):

        return FrontendRequest(
            Timestamp.from_dict("Timestamp", dict["prev"]),
            ClientRequest.from_dict("ClientRequest", dict["request"]),
            dict["operation"],
            dict["id"]
        )

# I can have dictionaries of as many layers as I like, I suppose. Just nothing more complicated.


class ReplicaResponse:

    def __init__(self, value: Any, label: Timestamp):

        self.value = value
        self.label = label

    def __str__(self):

        return str(self.to_dict())

    def to_dict(self):

        return {
            "__class__": "ReplicaResponse",
            "value": self.value,
            "label": self.label.to_dict()
        }

    @staticmethod
    def from_dict(classname: str, dict: Dict):

        return ReplicaResponse(
            dict["value"],
            Timestamp.from_dict("Timestamp", dict["label"])
        )


SerializerBase.register_class_to_dict(ClientRequest, ClientRequest.to_dict)
SerializerBase.register_dict_to_class("ClientRequest", ClientRequest.from_dict)

SerializerBase.register_class_to_dict(FrontendRequest, FrontendRequest.to_dict)
SerializerBase.register_dict_to_class("FrontendRequest", FrontendRequest.from_dict)

SerializerBase.register_class_to_dict(ReplicaResponse, ReplicaResponse.to_dict)
SerializerBase.register_dict_to_class("ReplicaResponse", ReplicaResponse.from_dict)

