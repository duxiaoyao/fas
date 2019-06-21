import os
import pathlib

import dynaconf


class Environment:
    def __init__(self, name: str, root_dir: pathlib.Path = None) -> None:
        self.name = name.lower()
        self.root_dir = root_dir or pathlib.Path(os.path.abspath(__file__)).parents[1]

    @property
    def is_dev(self):
        return self.name in {'dev', 'development'}

    @property
    def is_test(self):
        return self.name in {'test', 'testing'}

    @property
    def is_stag(self):
        return self.name in {'stag', 'staging'}

    @property
    def is_prod(self):
        return self.name in {'prod', 'production'}

    def __eq__(self, other):
        if not isinstance(other, Environment):
            return False
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)


ENV = Environment(dynaconf.settings.ENV_FOR_DYNACONF)
