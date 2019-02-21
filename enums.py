import random
from enum import Enum


class EnumProperty(object):
    # https://stackoverflow.com/questions/47353555/how-to-get-random-value-of-attribute-of-enum-on-each-iteration#47354673

    def __init__(self, fget):
        self.fget = fget

    def __get__(self, instance, ownerclass=None):
        if ownerclass is None:
            ownerclass = instance.__class__
        return self.fget(ownerclass)

    def __set__(self, instance, value):
        raise AttributeError("can't set pseudo-member %r" % self.name)

    def __delete__(self, instance):
        raise AttributeError("can't delete pseudo-member %r" % self.name)


class ReplicaStatus(Enum):
    ACTIVE = "ACTIVE"
    OVERLOADED = "OVERLOADED"
    # OFFLINE = "OFFLINE"

    @EnumProperty
    def random(self):
        return random.choice(list(self.__members__.values()))


class Method(Enum):
    CREATE = 0
    READ = 1
    UPDATE = 2
    DELETE = 3


class Operation(Enum):
    QUERY = 0
    UPDATE = 1
