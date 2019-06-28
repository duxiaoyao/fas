from fas.util.web.escape import utf8


def test_unicode_escapes():
    assert utf8(u'\u00e9') == b'\xc3\xa9'
