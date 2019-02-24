from enums import Method, Operation
from typing import Dict, Any
from Pyro4.util import SerializerBase
import Pyro4
import uuid

Pyro4.config.SERIALIZER = "serpent"


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


def timestamp_to_dict(timestamp: Timestamp) -> Dict:

    print("Serialising timestamp")

    return {
        "__class__": "FrontendRequest",
        "replicas": timestamp.replicas
    }


def dict_to_timestamp(classname: str, timestamp: Dict) -> Timestamp:

    print("Deserialising frontend request:", timestamp)

    return Timestamp(
        timestamp["replicas"]
    )


class ClientRequest:

    def __init__(self, method: Method, params: Dict):

        self.method = method
        self.params = params


def client_request_to_dict(client_request: ClientRequest):

    print("Serialising client request")

    return {
        "__class__": "ClientRequest",
        "method": client_request.method,
        "params": client_request.params
    }


def dict_to_client_request(classname: str, client_request: Dict) -> ClientRequest:

    print("Deserialising client request")

    return ClientRequest(
        Method(client_request["method"]),
        client_request["params"]
    )


class FrontendRequest:

    def __init__(self, prev: Timestamp, request: ClientRequest, operation: Operation, id: str=None):

        if id is None:

            self.id = uuid.uuid4()

        else:

            self.id = uuid.UUID(id)

        self.prev = prev
        self.request = request
        self.operation = operation


def frontend_request_to_dict(frontend_request: FrontendRequest) -> Dict:

    print("Serialising frontend request")

    return {
        "__class__": "FrontendRequest",
        "id": str(frontend_request.id),
        "prev": timestamp_to_dict(frontend_request.prev),
        "request": client_request_to_dict(frontend_request.request),
        "operation": frontend_request.operation
    }


def dict_to_frontend_request(classname: str, frontend_request: Dict) -> FrontendRequest:

    print("Deserialising frontend request:", frontend_request)

    return FrontendRequest(
        dict_to_timestamp("Timestamp", frontend_request["prev"]),
        dict_to_client_request("ClientRequest", frontend_request["request"]),
        Operation(frontend_request["operation"]),
        frontend_request["id"]
    )


class ReplicaResponse:

    def __init__(self, value: Any, label: Timestamp):

        self.value = value
        self.label = label
        
        
def replica_response_to_dict(replica_response: ReplicaResponse):
    
    return {
        "__class__": "ReplicaResponse",
        "value": replica_response.value,
        "label": timestamp_to_dict(replica_response.label)
    }
    
def dict_to_replica_response(classname: str, replica_response: Dict):
    
    return ReplicaResponse(
        replica_response["value"],
        dict_to_timestamp("Timestamp", replica_response["label"])
    )
    
    

print("Registering serialiser hooks")

SerializerBase.register_class_to_dict(ClientRequest, client_request_to_dict)
SerializerBase.register_dict_to_class("ClientRequest", dict_to_client_request)

SerializerBase.register_class_to_dict(FrontendRequest, frontend_request_to_dict)
SerializerBase.register_dict_to_class("FrontendRequest", dict_to_frontend_request)

SerializerBase.register_class_to_dict(Timestamp, timestamp_to_dict)
SerializerBase.register_dict_to_class("Timestamp", dict_to_timestamp)

SerializerBase.register_class_to_dict(ReplicaResponse, replica_response_to_dict)
SerializerBase.register_dict_to_class("ReplicaResponse", dict_to_replica_response)

