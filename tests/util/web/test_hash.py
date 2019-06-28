from fas.util.web import hash_password, verify_password

SECRET = "It's a secret to everybody"


def test_hash_password():
    assert verify_password(hash_password('pass1'), 'pass1')
    assert not verify_password(hash_password('pass1'), 'pass2')
    assert not verify_password(hash_password('pass2'), 'pass1')
