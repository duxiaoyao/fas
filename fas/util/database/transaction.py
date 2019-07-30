import functools
from typing import Union, Callable, Any

from .interface import DBInterface


def transactional(isolation: Union[str, Callable] = 'read_committed', readonly: bool = False,
                  deferrable: bool = False) -> Callable:
    if callable(isolation):
        func = isolation
        return TransactionDecorator()(func)
    else:
        return TransactionDecorator(isolation=isolation, readonly=readonly, deferrable=deferrable)


class TransactionDecorator:
    __slots__ = ('isolation', 'readonly', 'deferrable')

    def __init__(self, *, isolation: str = 'read_committed', readonly: bool = False, deferrable: bool = False) -> None:
        self.isolation: str = isolation
        self.readonly: bool = readonly
        self.deferrable: bool = deferrable

    def __call__(self, func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(db: DBInterface, *args: Any, **kwargs: Any) -> Any:
            if not isinstance(db, DBInterface):
                raise AssertionError(
                    f'The first argument of a transactional function should be a DBInterface instance, not {type(db)}')
            if db.is_in_transaction:
                return await func(db, *args, **kwargs)
            else:
                async with await db.transaction(isolation=self.isolation, readonly=self.readonly,
                                                deferrable=self.deferrable):
                    return await func(db, *args, **kwargs)

        return wrapper
