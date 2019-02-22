from enums import Method


class Request:

    def __init__(self, method: Method, params):
        self.method = method
        self.params = params
