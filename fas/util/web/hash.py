import base64
import hashlib
import hmac
import random
import string
import time
from typing import Union, Optional

from argon2 import PasswordHasher
from argon2.exceptions import Argon2Error, InvalidHash

from .escape import utf8

_PH = PasswordHasher()


def hash_password(plain_password: Union[str, bytes]) -> str:
    return _PH.hash(plain_password)


def verify_password(password_hash: Union[str, bytes], plain_password: Union[str, bytes]) -> bool:
    try:
        _PH.verify(password_hash, plain_password)
    except (Argon2Error, InvalidHash):
        return False
    else:
        return True


READABLE_ALPHANUMERIC = (string.ascii_letters + string.digits).translate(str.maketrans('', '', '0oO1l'))


def generate_password(*, size: int = 8, charset: str = READABLE_ALPHANUMERIC) -> str:
    return ''.join(random.choices(charset, k=size))


def create_signed_value(secret: Union[str, bytes], value: Union[str, bytes], *, with_timestamp: bool = False) -> bytes:
    """Signs a value so it cannot be forged.

    If ``with_timestamp`` is True, signs with the timestamp. It is
    used to check if the signature is expired in `decode_signed_value`
    """
    value = base64.b64encode(utf8(value))
    parts = [value, b'', b'']
    if with_timestamp:
        timestamp = utf8(str(int(time.time())))
        parts[1] = timestamp
    to_sign = b'|'.join(parts)

    signature = _create_signature(secret, to_sign)
    return to_sign + signature


def decode_signed_value(secret: Union[str, bytes], value: Union[None, str, bytes], *,
                        max_age_days: Optional[int] = None) -> Optional[bytes]:
    """Decode the given signed value if it validates, or None.

    If ``max_age_days`` is not None, check against the timestamp added
    in `create_signed_value` to see if the signature is expired
    """
    if value is None:
        return None
    value = utf8(value)
    try:
        value_field, timestamp, passed_sig = value.split(b'|', maxsplit=2)
    except ValueError:
        return None

    signed = value[: -len(passed_sig)]
    expected_sig = _create_signature(secret, signed)
    if not hmac.compare_digest(passed_sig, expected_sig):
        return None
    if max_age_days is not None:
        try:
            timestamp = int(timestamp)
        except ValueError:
            return None
        if timestamp < time.time() - max_age_days * 86400:
            # The signature has expired.
            return None
    try:
        return base64.b64decode(value_field)
    except Exception:
        return None


def _create_signature(secret: Union[str, bytes], value: bytes) -> bytes:
    return utf8(hmac.new(utf8(secret), value, digestmod=hashlib.sha256).hexdigest())
