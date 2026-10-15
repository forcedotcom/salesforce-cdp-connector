"""A throwaway RSA key for tests (JWT keys are validated eagerly)."""

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

TEST_RSA_PRIVATE_KEY_PEM = _key.private_bytes(
    serialization.Encoding.PEM,
    serialization.PrivateFormat.PKCS8,
    serialization.NoEncryption(),
).decode()

TEST_RSA_PUBLIC_KEY = _key.public_key()
