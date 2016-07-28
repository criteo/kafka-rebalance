import enum
import os

@enum.unique
class Dc(enum.Enum):

    XXX = 'xxx'

    YYY = 'yyy'


@enum.unique
class KafkaClusterType(enum.Enum):

    CENTRAL = 'central'

    LOCAL = 'local'


@enum.unique
class Env(enum.Enum):

    PROD = 'prod'

    PREPROD = 'preprod'

    @staticmethod
    def getCurrent():
        raise NotImplemented()
