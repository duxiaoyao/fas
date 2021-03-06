import time
from typing import Callable, Union

import pytest
from starlette.requests import Request
from starlette.responses import Response
from starlette.testclient import TestClient

from fas.util.web import get_cookie, set_cookie, get_secure_cookie, set_secure_cookie, delete_cookie, delete_all_cookies
from . import present, past


@pytest.mark.parametrize('value', ['|', ' ', 'val|', 'va|l', 'va l', 'Hello, | cookies!'])
@pytest.mark.parametrize('name', ['key', 'key1', '_key', 'key_', 'ke_y'])
@pytest.mark.parametrize('get_cookie_, set_cookie_',
                         [(get_cookie, set_cookie), (get_secure_cookie, set_secure_cookie)])
def test_cookie(get_cookie_: Callable, set_cookie_: Callable, name: str, value: Union[str, bytes]):
    async def app(scope, receive, send):
        request = Request(scope, receive)
        mycookie = get_cookie_(request, name)
        if mycookie:
            response = Response(mycookie, media_type='text/plain')
        else:
            response = Response('Hello, world!', media_type='text/plain')
            set_cookie_(response, name, value)

        await response(scope, receive, send)

    client = TestClient(app)
    response = client.get('/')
    assert response.text == 'Hello, world!'
    response = client.get('/')
    assert response.text == value


@pytest.mark.parametrize('name', ['|', '|key', 'ke|y', 'key|', '', ' ', 'ke y', '1key', 'ke-y'])
@pytest.mark.parametrize('set_cookie_', [set_cookie, set_secure_cookie])
def test_bad_cookie_name(set_cookie_: Callable, name: str):
    async def app(scope, receive, send):
        response = Response('Hello, world!', media_type='text/plain')
        with pytest.raises(ValueError) as e:
            set_cookie_(response, name, 'Hello, cookies!')
        assert str(e.value) == f'Invalid cookie name: {repr(name)}'

        await response(scope, receive, send)

    client = TestClient(app)
    client.get('/')


@pytest.mark.parametrize(('value', 'exc_type'), [(None, ValueError), (b'\x9c', UnicodeDecodeError)])
@pytest.mark.parametrize('set_cookie_', [set_cookie, set_secure_cookie])
def test_bad_cookie_value(set_cookie_: Callable, value: Union[str, bytes],
                          exc_type: Union[ValueError, UnicodeDecodeError]):
    async def app(scope, receive, send):
        response = Response('Hello, world!', media_type='text/plain')
        with pytest.raises(exc_type):
            set_cookie_(response, 'key', value)

        await response(scope, receive, send)

    client = TestClient(app)
    client.get('/')


def atest_secure_cookie_name_swap(monkeypatch):
    monkeypatch.setattr(time, 'time', present)

    async def app(scope, receive, send):
        response = Response('Hello, world!', media_type='text/plain')
        set_secure_cookie(response, 'key1', 'Hello, cookies!')
        set_secure_cookie(response, 'key2', 'Hello, cookies!')

        await response(scope, receive, send)

    client = TestClient(app)
    response = client.get('/')
    assert response.cookies['mycookie']
    response = client.get('/')
    assert response.cookies['mycookie']

    response = client.get('/')
    assert not response.cookies.get('mycookie')


def test_expired_cookie(monkeypatch):
    expect_cookie = None

    async def app(scope, receive, send):
        request = Request(scope, receive)
        response = Response('Hello, world!', media_type='text/plain')
        if expect_cookie is None:
            set_secure_cookie(response, 'mycookie', 'Hello, cookies!')
        elif expect_cookie:
            assert get_secure_cookie(request, 'mycookie') == 'Hello, cookies!'
        else:
            assert get_secure_cookie(request, 'mycookie') is None

        await response(scope, receive, send)

    client = TestClient(app)

    monkeypatch.setattr(time, 'time', past)
    response = client.get('/')
    assert response.cookies['mycookie']
    expect_cookie = True
    client.get('/')
    monkeypatch.setattr(time, 'time', present)
    expect_cookie = False
    client.get('/')


def test_delete_cookie():
    async def app(scope, receive, send):
        request = Request(scope, receive)
        response = Response('Hello, world!', media_type='text/plain')
        if get_cookie(request, 'mycookie'):
            delete_cookie(response, 'mycookie')
        else:
            set_cookie(response, 'mycookie', 'my value')
        await response(scope, receive, send)

    client = TestClient(app)
    response = client.get('/')
    assert response.cookies['mycookie']
    response = client.get('/')
    assert not response.cookies.get('mycookie')


def test_delete_all_cookies():
    async def app(scope, receive, send):
        request = Request(scope, receive)
        response = Response('Hello, world!', media_type='text/plain')
        if get_cookie(request, 'mycookie1') and get_cookie(request, 'mycookie2'):
            delete_all_cookies(request, response)
        else:
            set_cookie(response, 'mycookie1', 'my value1')
            set_cookie(response, 'mycookie2', 'my value2')
        await response(scope, receive, send)

    client = TestClient(app)
    response = client.get('/')
    assert response.cookies['mycookie1']
    assert response.cookies['mycookie2']
    response = client.get('/')
    assert not response.cookies.get('mycookie1')
    assert not response.cookies.get('mycookie2')
