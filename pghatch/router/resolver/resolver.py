from abc import ABC


class Resolver(ABC):
    def resolve(self, *args, **kwargs):
        raise NotImplementedError()
