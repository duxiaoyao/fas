import calendar
import email.utils
import http.cookies
import time
from datetime import datetime, timedelta
from typing import Union, Optional, Tuple

from dynaconf import settings
from starlette.requests import Request
from starlette.responses import Response

from .escape import utf8, unicode
from .hash import create_signed_value, decode_signed_value


def get_cookie(request: Request, name: str, *, default: Optional[str] = None) -> Optional[str]:
    """Returns the value of the request cookie with the given name.

    If the named cookie is not present, returns ``default``.

    This method only returns cookies that were present in the request.
    It does not see the outgoing cookies set by `set_cookie` in this
    handler.
    """
    return request.cookies.get(name, default)


def set_cookie(response: Response, name: str, value: Union[str, bytes], *, domain: Optional[str] = None,
               path: str = '/', expires: Optional[Union[float, Tuple, datetime]] = None,
               expires_days: Optional[int] = None, max_age: Optional[int] = None, secure=False, httponly=True,
               samesite: Optional[str] = 'Lax') -> None:
    """Sets an outgoing cookie name/value with the given options.

    Newly-set cookies are not immediately visible via `get_cookie`;
    they are not present until the next request.

    expires may be a numeric timestamp as returned by `time.time`,
    a time tuple as returned by `time.gmtime`, or a
    `datetime.datetime` object.
    """
    if not name.isidentifier():
        # Don't let us accidentally inject bad stuff
        raise ValueError(f'Invalid cookie name: {repr(name)}')
    if value is None:
        raise ValueError(f'Invalid cookie value: {repr(value)}')
    value = unicode(value)
    cookie = http.cookies.SimpleCookie()
    cookie[name] = value
    morsel = cookie[name]
    if domain:
        morsel['domain'] = domain
    if path:
        morsel['path'] = path
    if expires_days is not None and not expires:
        expires = datetime.utcnow() + timedelta(days=expires_days)
    if expires:
        morsel['expires'] = format_http_timestamp(expires)
    if max_age is not None:
        morsel['max-age'] = max_age
    parts = [cookie.output(header='').strip()]
    if secure:
        parts.append('Secure')
    if httponly:
        parts.append('HttpOnly')
    if samesite:
        parts.append(f'SameSite={http.cookies._quote(samesite)}')
    cookie_val = '; '.join(parts)
    response.raw_headers.append((b'set-cookie', cookie_val.encode('latin-1')))


def get_secure_cookie(request: Request, name: str, *, max_age_days: Optional[int] = 31,
                      secret: Optional[Union[str, bytes]] = None) -> Optional[str]:
    """Returns the given signed cookie if it validates, or None.

    Note that the ``max_age_days`` parameter

    The decoded cookie value is returned as a byte string (unlike
    `get_cookie`).

    Similar to `get_cookie`, this method only returns cookies that
    were present in the request. It does not see outgoing cookies set by
    `set_secure_cookie` in this handler.
    """
    if secret is None:
        secret = settings.COOKIE_SECRET
    signed_value = get_cookie(request, name)
    signed_value = decode_signed_value(secret, signed_value, max_age_days=max_age_days)
    if signed_value is None:
        return None
    try:
        _, passed_name, value = signed_value.split(b'|', maxsplit=2)
    except ValueError:
        return None
    if passed_name != utf8(name):
        return None
    return unicode(value)


def set_secure_cookie(response: Response, name: str, value: Union[str, bytes], *, domain: Optional[str] = None,
                      path: str = '/', expires: Optional[Union[float, Tuple, datetime]] = None,
                      expires_days: Optional[int] = 30, max_age: Optional[int] = None, secure=False, httponly=True,
                      samesite: Optional[str] = 'Lax', secret: Optional[Union[str, bytes]] = None) -> None:
    """Signs and timestamps a cookie so it cannot be forged.

    Note that the ``expires_days`` parameter sets the lifetime of the
    cookie in the browser, but is independent of the ``max_age_days``
    parameter to `get_secure_cookie`.
    A value of None limits the lifetime to the current browser session.

    If ``secret`` is None, it is set to the ``cookie_secret`` setting
    in your configuration file ``.secrets.toml``. It should be a long
    random sequence of bytes to be used as the HMAC secret for the
    signature.

    Secure cookies may contain arbitrary byte values, not just unicode
    strings (unlike regular cookies)

    Similar to `set_cookie`, the effect of this method will not be
    seen until the following request.
    """
    if secret is None:
        secret = settings.COOKIE_SECRET
    if not name.isidentifier():
        # Don't let us accidentally inject bad stuff
        raise ValueError(f'Invalid cookie name: {repr(name)}')
    if value is None:
        raise ValueError(f'Invalid cookie value: {repr(value)}')
    value = unicode(value)
    version = b'1'  # the version of the format of secured cookie, reserved for possible future upgrade
    value_to_sign = b'|'.join([version, utf8(name), utf8(value)])
    signed_value = create_signed_value(secret, value_to_sign, with_timestamp=True)
    set_cookie(response, name, signed_value, domain=domain, path=path, expires=expires, expires_days=expires_days,
               max_age=max_age, secure=secure, httponly=httponly, samesite=samesite)


def delete_cookie(response: Response, name: str, *, domain: Optional[str] = None, path: str = '/') -> None:
    """Deletes the cookie with the given name.

    Due to limitations of the cookie protocol, you must pass the same
    path and domain to clear a cookie as were used when that cookie
    was set (but there is no way to find out on the server side
    which values were used for a given cookie).

    Similar to `set_cookie`, the effect of this method will not be
    seen until the following request.
    """
    expires = datetime.utcnow() - timedelta(days=365)
    set_cookie(response, name, value='', domain=domain, path=path, expires=expires, max_age=0)


def delete_all_cookies(request: Request, response: Response, *, domain: Optional[str] = None, path: str = '/') -> None:
    """Deletes all the cookies the user sent with this request.

    See `clear_cookie` for more information on the path and domain
    parameters.

    Similar to `set_cookie`, the effect of this method will not be
    seen until the following request.
    """
    for name in request.cookies:
        delete_cookie(response, name, domain=domain, path=path)


def format_http_timestamp(ts: Union[int, float, tuple, time.struct_time, datetime]) -> str:
    """Formats a timestamp in the format used by HTTP.
    The argument may be a numeric timestamp as returned by `time.time`,
    a time tuple as returned by `time.gmtime`, or a `datetime.datetime`
    object.
    >>> format_http_timestamp(1359312200)
    'Sun, 27 Jan 2013 18:43:20 GMT'
    """
    if isinstance(ts, (int, float)):
        time_num = ts
    elif isinstance(ts, (tuple, time.struct_time)):
        time_num = calendar.timegm(ts)
    elif isinstance(ts, datetime):
        time_num = calendar.timegm(ts.utctimetuple())
    else:
        raise TypeError(f'unknown timestamp type: {repr(ts)}')
    return email.utils.formatdate(time_num, usegmt=True)
