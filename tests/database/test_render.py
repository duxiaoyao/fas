import pytest

from fas.util.database.parameter import render, render_many

args = 'template', 'ctx', 'expected_query', 'expected_params'
TESTS = [
    {'template': 'simple: :v', 'ctx': lambda: dict(v=1), 'expected_query': 'simple: $1', 'expected_params': (1,)},
    {'template': 'simple: v', 'ctx': lambda: dict(v=1), 'expected_query': 'simple: v', 'expected_params': ()},
    {
        'template': 'multiple: :a :c :b',
        'ctx': lambda: dict(a=1, b=2, c=3),
        'expected_query': 'multiple: $1 $2 $3',
        'expected_params': (1, 3, 2),
    },
    {
        'template': 'values: :a',
        'ctx': lambda: dict(a=[1, 2, 3]),
        'expected_query': 'values: $1',
        'expected_params': ([1, 2, 3],),
    },
    {
        'template': 'SELECT * FROM a WHERE x=:a AND y=:b',
        'ctx': lambda: {'a': 123, 'b': 456},
        'expected_query': 'SELECT * FROM a WHERE x=$1 AND y=$2',
        'expected_params': (123, 456),
    },
    {
        'template': 'SELECT * FROM a WHERE x=:a AND y=:a',
        'ctx': lambda: {'a': 123, 'b': 123},
        'expected_query': 'SELECT * FROM a WHERE x=$1 AND y=$1',
        'expected_params': (123,),
    },
    {
        'template': 'numeric: :1000 :v1000',
        'ctx': lambda: {'1000': 1, 'v1000': 2},
        'expected_query': 'numeric: :1000 $1',
        'expected_params': (2,),
    },
]


@pytest.mark.parametrize(','.join(args), [[t[a] for a in args] for t in TESTS], ids=[t['template'] for t in TESTS])
def test_render(template, ctx, expected_query, expected_params):
    ctx = ctx()
    query, params = render(template, **ctx)
    assert expected_query == query
    assert expected_params == params


def test_render_errors():
    with pytest.raises(KeyError):
        render(':a :b', a=1)
    with pytest.raises(KeyError):
        render(':a :b', a=1, c=3)


def test_render_many():
    query, params = render_many(':new_name :name',
                                [{'name': 'n2', 'new_name': 'nn2'}, {'name': 'n1', 'new_name': 'nn1'}])
    assert '$1 $2' == query
    assert (('nn2', 'n2'), ('nn1', 'n1')) == params


def test_render_many_errors():
    with pytest.raises(KeyError):
        render_many(':a :b', [{'a': 1, 'b': 2}, {'a': 10}])
    with pytest.raises(KeyError):
        render_many(':a :b', [{'a': 1, 'b': 11}, {'a': 2, 'bb': 21}])
