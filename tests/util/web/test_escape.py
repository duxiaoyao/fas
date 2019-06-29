import pytest

from fas.util.web.escape import utf8, unicode


def test_unicode_escapes():
    assert utf8(u'\u00e9') == b'\xc3\xa9'
    with pytest.raises(UnicodeDecodeError) as e:
        unicode(b'\x9c')


def test_none():
    assert utf8(None) is None
    assert unicode(None) is None
