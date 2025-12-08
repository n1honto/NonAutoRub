import os
from typing import Tuple

KUZNECHIK_SBOX = bytes(
    [
        0xFC,
        0xEE,
        0xDD,
        0x11,
        0xCF,
        0x6E,
        0x31,
        0x16,
        0xFB,
        0xC4,
        0xFA,
        0xDA,
        0x23,
        0xC5,
        0x04,
        0x4D,
        0xE9,
        0x77,
        0xF0,
        0xDB,
        0x93,
        0x2E,
        0x99,
        0xBA,
        0x17,
        0x36,
        0xF1,
        0xBB,
        0x14,
        0xCD,
        0x5F,
        0xC1,
        0xF9,
        0x18,
        0x65,
        0x5A,
        0xE2,
        0x5C,
        0xEF,
        0x21,
        0x81,
        0x1C,
        0x3C,
        0x42,
        0x8B,
        0x01,
        0x8E,
        0x4F,
        0x05,
        0x84,
        0x02,
        0xAE,
        0xE3,
        0x6A,
        0x8F,
        0xA0,
        0x06,
        0x0B,
        0xED,
        0x98,
        0x7F,
        0xD4,
        0xD3,
        0x1F,
        0xEB,
        0x34,
        0x2C,
        0x51,
        0xEA,
        0xC8,
        0x48,
        0xAB,
        0xF2,
        0x2A,
        0x68,
        0xA2,
        0xFD,
        0x3A,
        0xCE,
        0xCC,
        0xB5,
        0x70,
        0x0E,
        0x56,
        0x08,
        0x0C,
        0x76,
        0x12,
        0xBF,
        0x72,
        0x13,
        0x47,
        0x9C,
        0xB7,
        0x5D,
        0x87,
        0x15,
        0xA1,
        0x96,
        0x29,
        0x10,
        0x7B,
        0x9A,
        0xC7,
        0xF3,
        0x91,
        0x78,
        0x6F,
        0x9D,
        0x9E,
        0xB2,
        0xB1,
        0x32,
        0x75,
        0x19,
        0x3D,
        0xFF,
        0x35,
        0x8A,
        0x7E,
        0x6D,
        0x54,
        0xC6,
        0x80,
        0xC3,
        0xBD,
        0x0D,
        0x57,
        0xDF,
        0xF5,
        0x24,
        0xA9,
        0x3E,
        0xA8,
        0x43,
        0xC9,
        0xD7,
        0x79,
        0xD6,
        0xF6,
        0x7C,
        0x22,
        0xB9,
        0x03,
        0xE0,
        0x0F,
        0xEC,
        0xDE,
        0x7A,
        0x94,
        0xB0,
        0xBC,
        0xDC,
        0xE8,
        0x28,
        0x50,
        0x4E,
        0x33,
        0x0A,
        0x4A,
        0xA7,
        0x97,
        0x60,
        0x73,
        0x1E,
        0x00,
        0x62,
        0x44,
        0x1A,
        0xB8,
        0x38,
        0x82,
        0x64,
        0x9F,
        0x26,
        0x41,
        0xAD,
        0x45,
        0x46,
        0x92,
        0x27,
        0x5E,
        0x55,
        0x2F,
        0x8C,
        0xA3,
        0xA5,
        0x7D,
        0x69,
        0xD5,
        0x95,
        0x3B,
        0x07,
        0x58,
        0xB3,
        0x40,
        0x86,
        0xAC,
        0x1D,
        0xF7,
        0x30,
        0x37,
        0x6B,
        0xE4,
        0x88,
        0xD9,
        0xE7,
        0x89,
        0xE1,
        0x1B,
        0x83,
        0x49,
        0x4C,
        0x3F,
        0xF8,
        0xFE,
        0x8D,
        0x53,
        0xAA,
        0x90,
        0xCA,
        0xD8,
        0x85,
        0x61,
        0x20,
        0x71,
        0x67,
        0xA4,
        0x2D,
        0x2B,
        0x09,
        0x5B,
        0xCB,
        0x9B,
        0x25,
        0xD0,
        0xBE,
        0xE5,
        0x6C,
        0x52,
        0x59,
        0xA6,
        0x74,
        0xD2,
        0xE6,
        0xF4,
        0xB4,
        0xC0,
        0xD1,
        0x66,
        0xAF,
        0xC2,
        0x39,
        0x4B,
        0x63,
        0xB6,
    ]
)

KUZNECHIK_SBOX_INV = bytes([KUZNECHIK_SBOX.index(i) for i in range(256)])

KUZNECHIK_L_VEC = bytes(
    [
        0x94,
        0x20,
        0x85,
        0x10,
        0xC2,
        0xC0,
        0x01,
        0xFB,
        0x01,
        0xC0,
        0xC2,
        0x10,
        0x85,
        0x20,
        0x94,
        0x01,
    ]
)


def generate_key() -> bytes:
    return os.urandom(32)


def _gf_mul(a: int, b: int) -> int:
    result = 0
    while b:
        if b & 1:
            result ^= a
        a <<= 1
        if a & 0x100:
            a ^= 0x1C3
        b >>= 1
    return result


def _kuznechik_l_step(block: bytes) -> bytes:
    result = 0
    for i in range(16):
        result ^= _gf_mul(block[i], KUZNECHIK_L_VEC[i])
    return bytes([result]) + block[:15]


def _kuznechik_l(block: bytes) -> bytes:
    for _ in range(16):
        block = _kuznechik_l_step(block)
    return block


def _kuznechik_l_inv(block: bytes) -> bytes:
    for _ in range(16):
        block = block[1:] + bytes([block[0]])
        result = 0
        for i in range(16):
            result ^= _gf_mul(block[i], KUZNECHIK_L_VEC[i])
        block = block[:15] + bytes([result])
    return block


def _kuznechik_s(block: bytes) -> bytes:
    return bytes([KUZNECHIK_SBOX[b] for b in block])


def _kuznechik_s_inv(block: bytes) -> bytes:
    return bytes([KUZNECHIK_SBOX_INV[b] for b in block])


def _xor_bytes(a: bytes, b: bytes) -> bytes:
    return bytes([x ^ y for x, y in zip(a, b)])


class KuznechikCipher:
    BLOCK_SIZE = 16
    KEY_SIZE = 32
    ROUNDS = 10

    def __init__(self, key: bytes):
        if len(key) != self.KEY_SIZE:
            raise ValueError("Invalid key length")
        self._round_keys = self._expand_key(key)

    def _expand_key(self, key: bytes) -> list[bytes]:
        k1 = key[:16]
        k2 = key[16:]
        round_keys = [k1, k2]
        c = []
        for i in range(1, 33):
            c.append(_kuznechik_l(bytes([i]) + bytes(15)))
        for i in range(4):
            for j in range(8):
                idx = 8 * i + j
                temp = _xor_bytes(k1, c[idx])
                temp = _kuznechik_s(temp)
                temp = _kuznechik_l(temp)
                temp = _xor_bytes(temp, k2)
                k2, k1 = k1, temp
            round_keys.extend([k1, k2])
        return round_keys

    def encrypt_block(self, block: bytes) -> bytes:
        if len(block) != self.BLOCK_SIZE:
            raise ValueError("Invalid block length")
        for i in range(9):
            block = _xor_bytes(block, self._round_keys[i])
            block = _kuznechik_s(block)
            block = _kuznechik_l(block)
        block = _xor_bytes(block, self._round_keys[9])
        return block

    def decrypt_block(self, block: bytes) -> bytes:
        if len(block) != self.BLOCK_SIZE:
            raise ValueError("Invalid block length")
        block = _xor_bytes(block, self._round_keys[9])
        for i in range(8, -1, -1):
            block = _kuznechik_l_inv(block)
            block = _kuznechik_s_inv(block)
            block = _xor_bytes(block, self._round_keys[i])
        return block


def _inc_counter(counter: bytearray) -> None:
    for i in range(len(counter) - 1, -1, -1):
        counter[i] = (counter[i] + 1) & 0xFF
        if counter[i] != 0:
            break


def encrypt_ctr(key: bytes, plaintext: bytes) -> Tuple[bytes, bytes]:
    cipher = KuznechikCipher(key)
    iv = os.urandom(16)
    counter = bytearray(iv)
    keystream = bytearray()
    while len(keystream) < len(plaintext):
        block = cipher.encrypt_block(bytes(counter))
        keystream.extend(block)
        _inc_counter(counter)
    ciphertext = bytes(p ^ k for p, k in zip(plaintext, keystream))
    return iv, ciphertext


def decrypt_ctr(key: bytes, iv: bytes, ciphertext: bytes) -> bytes:
    cipher = KuznechikCipher(key)
    counter = bytearray(iv)
    keystream = bytearray()
    while len(keystream) < len(ciphertext):
        block = cipher.encrypt_block(bytes(counter))
        keystream.extend(block)
        _inc_counter(counter)
    plaintext = bytes(c ^ k for c, k in zip(ciphertext, keystream))
    return plaintext
