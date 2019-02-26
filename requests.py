import uuid
from typing import Dict, Any

from Pyro4.util import SerializerBase

from enums import Operation
from timestamp import Timestamp


class ClientRequest:
    """
    Represents a request from a client to a FE.
    """

    def __init__(self, method: Operation, params: Dict):

        self.method: Operation = method    # The method (CREATE, READ, or UPDATE) specified by the client
        self.params: Dict = params  # The params required to execute the request

    def __str__(self):

        return str({
            "method": str(self.method),
            "params": self.params
        })

    def to_dict(self) -> Dict:
        """
        Used for serpent serialisation
        :return: A dict representing this ClientRequest
        """

        return {
            "__class__": "ClientRequest",
            "method": self.method,
            "params": self.params
        }

    @staticmethod
    def from_dict(classname: str, dict: Dict):
        """
        Used for serpent deserialisation
        :return: A ClientRequest
        """

        return ClientRequest(
            Operation(dict["method"]),
            dict["params"]
        )


class FrontendRequest:
    """
    Represents a request from a FE to a RM.
    """

    def __init__(self, prev: Timestamp, request: ClientRequest, id: str=None):

        self.id = id if id is not None else str(uuid.uuid4())
        self.prev = prev
        self.request = request

    def __str__(self):

        dict = self.to_dict()
        del dict["__class__"]

        return str(dict)

    def to_dict(self) -> Dict:
        """
        Used for serpent serialisation
        :return: A dict representing this FrontendRequest
        """

        return {
            "__class__": "FrontendRequest",
            "prev": self.prev.to_dict(),
            "request": self.request.to_dict(),
            "id": self.id
        }

    @staticmethod
    def from_dict(classname: str, dict: Dict) -> 'FrontendRequest':
        """
        Used for serpent deserialisation
        :return: A FrontendRequest
        """

        return FrontendRequest(
            Timestamp.from_dict("Timestamp", dict["prev"]),
            ClientRequest.from_dict("ClientRequest", dict["request"]),
            dict["id"]
        )


class ReplicaResponse:
    """
    Represents a response to a FrontendRequest from a RM to a FE
    """

    def __init__(self, value: Any, label: Timestamp):

        self.value = value  # The value of the executed request. None if an update.
        self.label = label  # The value Timestamp of the RM that executed the request

    def __str__(self):

        dict = self.to_dict()
        del dict["__class__"]

        return str(dict)

    def to_dict(self):
        """
        Used for serpent serialisation
        :return: A dict representing this ReplicaResponse
        """

        return {
            "__class__": "ReplicaResponse",
            "value": self.value,
            "label": self.label.to_dict()
        }

    @staticmethod
    def from_dict(classname: str, dict: Dict):
        """
        Used for serpent deserialisation
        :return: A ReplicaResponse
        """

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

