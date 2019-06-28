import typing


@typing.overload
def utf8(value: bytes) -> bytes:
    pass


@typing.overload
def utf8(value: str) -> bytes:
    pass


@typing.overload
def utf8(value: None) -> None:
    pass


def utf8(value: typing.Union[None, str, bytes]) -> typing.Optional[bytes]:
    """Converts a string argument to a byte string.
    If the argument is already a byte string or None, it is returned unchanged.
    Otherwise it must be a unicode string and is encoded as utf8.
    """
    if isinstance(value, (bytes, type(None))):
        return value
    if not isinstance(value, str):
        raise TypeError(f'Expected bytes, unicode, or None; got {repr(type(value))}')
    return value.encode('UTF-8')


@typing.overload
def unicode(value: str) -> str:
    pass


@typing.overload
def unicode(value: bytes) -> str:
    pass


@typing.overload
def unicode(value: None) -> None:
    pass


def unicode(value: typing.Union[None, str, bytes]) -> typing.Optional[str]:
    """Converts a string argument to a unicode string.
    If the argument is already a unicode string or None, it is returned
    unchanged.  Otherwise it must be a byte string and is decoded as utf8.
    """
    if isinstance(value, (str, type(None))):
        return value
    if not isinstance(value, bytes):
        raise TypeError(f'Expected bytes, unicode, or None; got {repr(type(value))}')
    return value.decode('UTF-8')
