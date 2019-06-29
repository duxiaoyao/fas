from functools import lru_cache
from typing import Tuple, Any

from pydantic import BaseModel


class Entity(BaseModel):

    @property
    def primary_key(self) -> Tuple[str]:
        return 'id',

    @property
    def identifiable(self) -> bool:
        return self.id > 0

    def __eq__(self, other: Any) -> bool:
        if not self.identifiable:
            return super().__eq__(other)
        if not isinstance(other, type(self)):
            return False
        return all(getattr(self, attr_name) == getattr(other, attr_name) for attr_name in self.primary_key)

    def __hash__(self) -> int:
        if not self.identifiable:
            return super().__hash__()
        return self.__hash_entity__()

    @lru_cache(maxsize=8192)
    def __hash_entity__(self) -> int:
        return hash(tuple(getattr(self, attr_name) for attr_name in self.primary_key))
