from __future__ import annotations

import abc
import inspect
import logging
from typing import Any, Dict, Optional, Tuple, Union, Sequence, Callable, List

import asyncpg
import buildpg

LOGGER = logging.getLogger(__name__)


class DBInterface(abc.ABC):
    __slots__ = ()

    @property
    @abc.abstractmethod
    def conn(self) -> asyncpg.Connection:
        raise NotImplementedError('This is an abstract property, should not come here')

    @property
    @abc.abstractmethod
    def is_connected(self) -> asyncpg.Connection:
        raise NotImplementedError('This is an abstract property, should not come here')

    @abc.abstractmethod
    async def acquire(self) -> DBInterface:
        raise NotImplementedError('This is an abstract property, should not come here')

    @abc.abstractmethod
    async def release(self) -> None:
        raise NotImplementedError('This is an abstract property, should not come here')

    async def _acquire_if_necessary(self) -> None:
        if not self.is_connected:
            await self.acquire()

    async def execute(self, sql: str, *, timeout: float = None, **kwargs: Any) -> int:
        return await self._execute(sql, timeout=timeout, **kwargs)

    async def exists(self, sql: str, *, timeout: float = None, **kwargs: Any) -> bool:
        return await self.get_scalar('SELECT EXISTS ({})'.format(sql), timeout=timeout, **kwargs)

    async def list(self, sql: str, *, to_cls: Optional[Callable[[Any], Any]] = None, timeout: float = None,
                   **kwargs: Any) -> List:
        rows = await self._query(sql, timeout=timeout, **kwargs)
        return [to_cls(**row) for row in rows] if to_cls else rows

    async def list_scalar(self, sql: str, *, to_cls: Optional[Callable[[Any], Any]] = None, timeout: float = None,
                          **kwargs: Any) -> List:
        rows = await self._query(sql, timeout=timeout, **kwargs)
        if rows and len(rows[0]) > 1:
            raise Exception(f'More than one columns returned: sql={sql} and kwargs={kwargs}')
        return [to_cls(**row[0]) if to_cls else row[0] for row in rows]

    async def get(self, sql: str, *, to_cls: Optional[Callable[[Any], Any]] = None, timeout: float = None,
                  **kwargs: Any) -> Any:
        rows = await self._query(sql, timeout=timeout, **kwargs)
        if not rows:
            LOGGER.debug(f'No rows returned: sql={sql} and kwargs={kwargs}')
            return None
        if len(rows) > 1:
            LOGGER.warning(f'More than one rows returned: sql={sql} and kwargs={kwargs}')
        return to_cls(**rows[0]) if to_cls else rows[0]

    async def get_scalar(self, sql: str, *, to_cls: Optional[Callable[[Any], Any]] = None, timeout: float = None,
                         **kwargs: Any) -> Any:
        rows = await self._query(sql, timeout=timeout, **kwargs)
        if not rows:
            LOGGER.debug(f'No rows returned: sql={sql} and kwargs={kwargs}')
            return None
        if len(rows) > 1:
            LOGGER.warning(f'More than one rows returned: sql={sql} and kwargs={kwargs}')
        if len(rows[0]) > 1:
            raise Exception(f'More than one columns returned: sql={sql} and kwargs={kwargs}')
        return to_cls(**rows[0][0]) if to_cls else rows[0][0]

    async def insert(self, table: str, objects: Optional[Union[Dict[str, Any], Sequence]] = None, *,
                     returns_id: Union[bool, str, Tuple[str]] = False, returns_record: Union[bool, Tuple[str]] = False,
                     to_cls: Optional[Callable[[Any], Any]] = None,
                     should_insert: Optional[Callable[[Any], bool]] = None, include_attrs: Optional[Tuple[str]] = None,
                     exclude_attrs: Tuple[str] = (), conflict_target: str = '', conflict_action: str = '',
                     timeout: float = None, **value_providers: Any) -> Union[int, List[Any], Any]:
        """
        include_attrs:
            when it is None, add all attributes not in exclude_attrs to columns;
            when it is empty tuple, do not add attributes to columns;
            when it is not None and not empty tuple, add include_attrs not in exclude_attrs to columns;
        """
        if exclude_attrs:
            value_providers = {k: v for k, v in value_providers.items() if k not in exclude_attrs}

        if objects is None:
            if not value_providers:
                raise Exception('Nothing to insert: value providers not found')
        else:
            if should_insert:
                objects = [o for o in objects if should_insert(o)]
            if not objects:
                return [] if returns_id or returns_record else 0

        specified_columns = True
        columns = tuple(value_providers)
        if objects:
            if include_attrs:
                columns += tuple(a for a in include_attrs if a not in exclude_attrs and a not in columns)
            elif include_attrs is None:
                some_object = next(iter(objects))
                if isinstance(some_object, dict):
                    columns += tuple(k for k in some_object if k not in exclude_attrs and k not in columns)
                elif not columns:
                    columns = tuple(range(len(some_object)))
                    specified_columns = False

        def get_rows_values():
            if objects is not None:
                for column in columns:
                    if column in value_providers:
                        value_provider = value_providers[column]
                        if callable(value_provider):
                            value_providers[column] = FunctionValueProvider(value_provider)
                        else:
                            value_providers[column] = ConstValueProvider(value_provider)
                    else:
                        value_providers[column] = DictValueProvider(
                            column if specified_columns else columns.index(column))
                for o in objects:
                    yield [value_providers[column](o) for column in columns]
            else:
                yield [value_providers[column] for column in columns]

        fragments = ['INSERT INTO ', table]
        if conflict_target or conflict_action:
            fragments.append(' AS C')  # means `CURRENT`
        if specified_columns:
            fragments.append(f' ({", ".join(columns)})')
        fragments.append(' VALUES ')
        arg_index = 0
        args = {}
        first_row_values = True
        for row_values in get_rows_values():
            if first_row_values:
                first_row_values = False
            else:
                fragments.append(', ')
            first_column_value = True
            fragments.append('(')
            for column_value in row_values:
                if first_column_value:
                    first_column_value = False
                else:
                    fragments.append(', ')
                arg_index += 1
                arg_name = f'a{arg_index}'
                fragments.append(f':{arg_name}')
                args[arg_name] = column_value
            fragments.append(')')
        if conflict_target or conflict_action:
            fragments.append(f' ON CONFLICT {conflict_target} {conflict_action}')
        if returns_id:
            if returns_id is True:
                key_names = ('id',)
            elif isinstance(returns_id, str):
                key_names = (returns_id,)
            else:
                key_names = returns_id
            fragments.append(f' RETURNING {", ".join(key_names)}')
            if len(key_names) == 1:
                if objects:
                    return await self.list_scalar(''.join(fragments), to_cls=to_cls, timeout=timeout, **args)
                else:
                    return await self.get_scalar(''.join(fragments), to_cls=to_cls, timeout=timeout, **args)
            else:
                if objects:
                    return await self.list(''.join(fragments), to_cls=to_cls, timeout=timeout, **args)
                else:
                    return await self.get(''.join(fragments), to_cls=to_cls, timeout=timeout, **args)
        elif returns_record:
            fragments.append(f' RETURNING {"*" if returns_record is True else ", ".join(returns_record)}')
            if objects:
                return await self.list(''.join(fragments), to_cls=to_cls, timeout=timeout, **args)
            else:
                return await self.get(''.join(fragments), to_cls=to_cls, timeout=timeout, **args)
        else:
            return await self.execute(''.join(fragments), timeout=timeout, **args)

    async def _query(self, sql: str, *, timeout: float = None, **kwargs: Any) -> List:
        query, args = buildpg.render(sql, **kwargs)
        await self._acquire_if_necessary()
        LOGGER.debug(f'query: {query} \nargs: {args}')
        return await self.conn.fetch(query, *args, timeout=timeout)

    async def _execute(self, sql: str, *, timeout: float = None, **kwargs: Any) -> int:
        query, args = buildpg.render(sql, **kwargs)
        await self._acquire_if_necessary()
        LOGGER.debug(f'query: {query} \nargs: {args}')
        last_sql_status = await self.conn.execute(query, *args, timeout=timeout)
        try:
            return int(last_sql_status.split()[-1])
        except (ValueError, AttributeError, IndexError):
            return 0


class FunctionValueProvider:
    __slots__ = ('func', 'multiple_args')

    def __init__(self, func: Callable):
        self.func: Callable = func
        self.multiple_args: bool = len(inspect.signature(func).parameters) > 1

    def __call__(self, obj):
        if self.multiple_args:
            return self.func(*obj)
        else:
            return self.func(obj)


class ConstValueProvider:
    __slots__ = ('const',)

    def __init__(self, const):
        self.const = const

    def __call__(self, obj):
        return self.const


class DictValueProvider:
    __slots__ = ('key',)

    def __init__(self, key):
        self.key = key

    def __call__(self, obj):
        try:
            return obj[self.key]
        except TypeError:
            return getattr(obj, self.key)
