import re
from functools import partial
from typing import Mapping, Any, Callable, AnyStr, Match, Sequence, Tuple

REGEX = re.compile(r'(?<!:):([a-z][a-z\d_]*)', flags=re.A)


def render(sql: str, params: Sequence[Mapping[str, Any]] = None, **param_name2value: Mapping[str, Any]) \
        -> Tuple[str, Tuple]:
    query, param_names = _render(sql)
    if params:
        args = tuple(tuple(p[name] for name in param_names) for p in params)
    else:
        args = tuple(param_name2value[name] for name in param_names)
    return query, args


def _render(sql: str):
    param_names = []

    def add_param(name):
        try:
            index = param_names.index(name)
        except ValueError:
            index = len(param_names)
            param_names.append(name)
        return f'${index + 1}'

    repl: Callable[[Match[AnyStr]], AnyStr] = partial(_replace, add_param=add_param)
    return REGEX.sub(repl, sql), param_names


def _replace(m: Match[AnyStr], *, add_param: Callable) -> AnyStr:
    name = m.group(1)
    return add_param(name)
