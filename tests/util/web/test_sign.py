import time

import pytest

from fas.util.web import create_signed_value, decode_signed_value
from . import SECRET, present, past


@pytest.fixture(autouse=True)
def set_present_time(monkeypatch):
    monkeypatch.setattr(time, 'time', present)


def test_known_values():
    signed = create_signed_value(SECRET, 'value', with_timestamp=True)
    assert signed == b'dmFsdWU=|1300000000|155da75456b0fb678e6a40235af8481665b5cc90320a1b3006e803b398645564'

    decoded = decode_signed_value(SECRET, signed)
    assert decoded == b'value'


def test_expired(monkeypatch):
    monkeypatch.setattr(time, 'time', past)

    signed = create_signed_value(SECRET, 'value', with_timestamp=True)
    decoded_past = decode_signed_value(SECRET, signed, max_age_days=31)
    assert decoded_past == b'value'

    monkeypatch.setattr(time, 'time', present)

    decoded_present = decode_signed_value(SECRET, signed, max_age_days=31)
    assert decoded_present is None


def test_payload_tampering():
    # These cookies are variants of the one in test_known_values.
    sig = '155da75456b0fb678e6a40235af8481665b5cc90320a1b3006e803b398645564'

    def validate(prefix):
        return b'value' == decode_signed_value(SECRET, prefix + sig)

    assert validate('dmFsdWU=|1300000000|')
    assert not validate('dmFsdWU=|130000000|')
    assert not validate('dmFsdWU=|13000000000|')
    assert not validate('xdmFsdWU=|13000000000|')


def test_signature_tampering():
    prefix = 'dmFsdWU=|1300000000|'
    sig = '155da75456b0fb678e6a40235af8481665b5cc90320a1b3006e803b398645564'

    def validate(sig):
        return b'value' == decode_signed_value(SECRET, prefix + sig)

    assert validate(sig)
    # All zeros
    assert not validate('0' * len(sig))
    # Change one character
    assert not validate('255da75456b0fb678e6a40235af8481665b5cc90320a1b3006e803b398645564')
    # Change another character
    assert not validate('155da75456b0fb678e6a40235af8481665b5cc90320a1b3006e803b398645565')
    # Truncate
    assert not validate('155da75456b0fb678e6a40235af8481665b5cc90320a1b3006e803b39864556')
    # Lengthen
    assert not validate('155da75456b0fb678e6a40235af8481665b5cc90320a1b3006e803b3986455648')


def test_non_ascii():
    value = b'\xe9'
    signed = create_signed_value(SECRET, value)
    decoded = decode_signed_value(SECRET, signed)
    assert value == decoded
