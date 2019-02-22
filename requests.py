from enums import Method, Operation
from timestamp import Timestamp
from typing import Dict, Any
from Pyro4.util import SerializerBase
import Pyro4
import uuid

Pyro4.config.SERIALIZER = "serpent"


class ClientRequest:

    def __init__(self, method: Method, params: Dict):

        self.method = method
        self.params = params


def serialise_client_request(client_request: ClientRequest):

    print("Serialising client request")

    return {
        "method": client_request.method,
        "params": client_request.params
    }


def deserialise_client_request(client_request: Dict):

    print("Deserialising client request")

    return ClientRequest(
        Method(client_request["method"]),
        client_request["params"]
    )


class ReplicaResponse:

    def __init__(self, value: Any, label: Timestamp):

        self.value = value
        self.label = label


class FrontendRequest:

    def __init__(self, prev: Timestamp, request: ClientRequest, operation: Operation, id: str=None):

        if id is None:

            self.id = uuid.uuid4()

        else:

            self.id = uuid.UUID(id)

        self.prev = prev
        self.request = request
        self.operation = operation


def serialise_frontend_request(frontend_request: FrontendRequest) -> Dict:

    print("Serialising frontend request")

    return {
        "id": str(frontend_request.id),
        "prev": frontend_request.prev,
        "request": frontend_request.request,
        "operation": frontend_request.operation
    }


def deserialise_frontend_request(frontend_request: Dict) -> FrontendRequest:

    print("Deserialising frontend request")

    return FrontendRequest(
        Timestamp(**frontend_request["prev"]),
        ClientRequest(**frontend_request["request"]),
        Operation(frontend_request["operation"]),
        frontend_request["id"]
    )


print("Registering serialiser hooks")

SerializerBase.register_class_to_dict(ClientRequest, serialise_client_request)
SerializerBase.register_dict_to_class("ClientRequest", deserialise_client_request)

SerializerBase.register_class_to_dict(FrontendRequest, serialise_frontend_request)
SerializerBase.register_dict_to_class("FrontendRequest", deserialise_frontend_request)
