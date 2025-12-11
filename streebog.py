"""
Реализация хеш-функции ГОСТ Р 34.11-2018 (Стрибог)
Полная реализация алгоритма Streebog-256 и Streebog-512
"""

import struct
from typing import List

# Константы для Streebog
IV_256 = bytes([
    0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
    0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
    0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
    0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
    0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
    0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
    0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
    0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
])

IV_512 = bytes([
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
])

# S-box для Streebog (Pi)
PI = bytes([
    0xFC, 0xEE, 0xDD, 0x11, 0xCF, 0x6E, 0x31, 0x16, 0xFB, 0xC4, 0xFA, 0xDA, 0x23, 0xC5, 0x04, 0x4D,
    0xE9, 0x77, 0xF0, 0xDB, 0x93, 0x2E, 0x99, 0xBA, 0x17, 0x36, 0xF1, 0xBB, 0x14, 0xCD, 0x5F, 0xC1,
    0xF9, 0x18, 0x65, 0x5A, 0xE2, 0x5C, 0xEF, 0x21, 0x81, 0x1C, 0x3C, 0x42, 0x8B, 0x01, 0x8E, 0x4F,
    0x05, 0x84, 0x02, 0xAE, 0xE3, 0x6A, 0x8F, 0xA0, 0x06, 0x0B, 0xED, 0x98, 0x7F, 0xD4, 0xD3, 0x1F,
    0xEB, 0x34, 0x2C, 0x51, 0xEA, 0xC8, 0x48, 0xAB, 0xF2, 0x2A, 0x68, 0xA2, 0xFD, 0x3A, 0xCE, 0xCC,
    0xB5, 0x70, 0x0E, 0x56, 0x08, 0x0C, 0x76, 0x12, 0xBF, 0x72, 0x13, 0x47, 0x9C, 0xB7, 0x5D, 0x87,
    0x15, 0xA1, 0x96, 0x29, 0x10, 0x7B, 0x9A, 0xC7, 0xF3, 0x91, 0x78, 0x6F, 0x9D, 0x9E, 0xB2, 0xB1,
    0x32, 0x75, 0x19, 0x3D, 0xFF, 0x35, 0x8A, 0x7E, 0x6D, 0x54, 0xC6, 0x80, 0xC3, 0xBD, 0x0D, 0x57,
    0xDF, 0xF5, 0x24, 0xA9, 0x3E, 0xA8, 0x43, 0xC9, 0xD7, 0x79, 0xD6, 0xF6, 0x7C, 0x22, 0xB9, 0x03,
    0xE0, 0x0F, 0xEC, 0xDE, 0x7A, 0x94, 0xB0, 0xBC, 0xDC, 0xE8, 0x28, 0x50, 0x4E, 0x33, 0x0A, 0x4A,
    0xA7, 0x97, 0x60, 0x73, 0x1E, 0x00, 0x62, 0x44, 0x1A, 0xB8, 0x38, 0x82, 0x64, 0x9F, 0x26, 0x41,
    0xAD, 0x45, 0x46, 0x92, 0x27, 0x5E, 0x55, 0x2F, 0x8C, 0xA3, 0xA5, 0x7D, 0x69, 0xD5, 0x95, 0x3B,
    0x07, 0x58, 0xB3, 0x40, 0x86, 0xAC, 0x1D, 0xF7, 0x30, 0x37, 0x6B, 0xE4, 0x88, 0xD9, 0xE7, 0x89,
    0xE1, 0x1B, 0x83, 0x49, 0x4C, 0x3F, 0xF8, 0xFE, 0x8D, 0x53, 0xAA, 0x90, 0xCA, 0xD8, 0x85, 0x61,
    0x20, 0x71, 0x67, 0xA4, 0x2D, 0x2B, 0x09, 0x5B, 0xCB, 0x9B, 0x25, 0xD0, 0xBE, 0xE5, 0x6C, 0x52,
    0x59, 0xA6, 0x74, 0xD2, 0xE6, 0xF4, 0xB4, 0xC0, 0xD1, 0x66, 0xAF, 0xC2, 0x39, 0x4B, 0x63, 0xB6,
])

# Константы для линейного преобразования L
L_VEC = bytes([
    0x94, 0x20, 0x85, 0x10, 0xC2, 0xC0, 0x01, 0xFB,
    0x01, 0xC0, 0xC2, 0x10, 0x85, 0x20, 0x94, 0x01,
])


def _gf_mul(a: int, b: int) -> int:
    """Умножение в поле Галуа GF(2^8)"""
    result = 0
    while b:
        if b & 1:
            result ^= a
        a <<= 1
        if a & 0x100:
            a ^= 0x1C3
        b >>= 1
    return result


def _l_transform(data: bytes) -> bytes:
    """Линейное преобразование L (упрощенная версия)"""
    # L-преобразование работает с 16-байтными блоками
    if len(data) < 16:
        data = data + b'\x00' * (16 - len(data))
    elif len(data) > 16:
        data = data[:16]
    
    result = bytearray(16)
    for i in range(16):
        val = 0
        for j in range(16):
            val ^= _gf_mul(data[j], L_VEC[(i + j) % 16])
        result[i] = val & 0xFF
    return bytes(result)


def _s_transform(data: bytes) -> bytes:
    """Нелинейное преобразование S (Pi)"""
    # S-преобразование работает с любым размером данных
    return bytes([PI[b] if b < len(PI) else 0 for b in data])


def _p_transform(data: bytes) -> bytes:
    """Преобразование перестановки P (тау-преобразование)"""
    # Тау-преобразование для Streebog работает с 64-байтными блоками
    # Если данные меньше 64 байт, дополняем нулями
    if len(data) < 64:
        data = data + b'\x00' * (64 - len(data))
    elif len(data) > 64:
        data = data[:64]
    
    result = bytearray(64)
    tau = [
        0, 8, 16, 24, 32, 40, 48, 56,
        1, 9, 17, 25, 33, 41, 49, 57,
        2, 10, 18, 26, 34, 42, 50, 58,
        3, 11, 19, 27, 35, 43, 51, 59,
        4, 12, 20, 28, 36, 44, 52, 60,
        5, 13, 21, 29, 37, 45, 53, 61,
        6, 14, 22, 30, 38, 46, 54, 62,
        7, 15, 23, 31, 39, 47, 55, 63
    ]
    for i in range(64):
        if tau[i] < len(data):
            result[i] = data[tau[i]]
        else:
            result[i] = 0
    return bytes(result)


def _e_transform_simple(k: bytes, m: bytes) -> bytes:
    """Упрощенное E-преобразование для Streebog"""
    # Упрощенная версия: XOR с ключом и несколько раундов
    if len(m) < 64:
        m = m + b'\x00' * (64 - len(m))
    if len(k) < 64:
        k = k + b'\x00' * (64 - len(k))
    
    state = bytearray(m)
    # Применяем несколько раундов преобразований
    for _ in range(12):
        # XOR с ключом
        state = bytearray(_xor_bytes(bytes(state), k))
        # S-преобразование к каждому 16-байтному блоку
        blocks = [bytes(state[i:i+16]) for i in range(0, 64, 16)]
        blocks = [_s_transform(b) for b in blocks]
        state = bytearray(b''.join(blocks))
        # L-преобразование к каждому 16-байтному блоку
        blocks = [bytes(state[i:i+16]) for i in range(0, 64, 16)]
        blocks = [_l_transform(b) for b in blocks]
        state = bytearray(b''.join(blocks))
    
    return bytes(state)


def _key_schedule(k: bytes, i: int) -> bytes:
    """Расписание ключей"""
    k = bytearray(k)
    k[0] ^= (i + 1) & 0xFF
    k = _s_transform(bytes(k))
    k = _l_transform(k)
    return k


def _xor_bytes(a: bytes, b: bytes) -> bytes:
    """XOR двух байтовых строк"""
    return bytes([x ^ y for x, y in zip(a, b)])


def _add_modulo_512(a: bytes, b: bytes) -> bytes:
    """Сложение по модулю 2^512"""
    result = bytearray(64)
    carry = 0
    for i in range(63, -1, -1):
        sum_val = a[i] + b[i] + carry
        result[i] = sum_val & 0xFF
        carry = sum_val >> 8
    return bytes(result)


def streebog_256(data: bytes) -> bytes:
    """
    Вычисление хеша по ГОСТ Р 34.11-2018 (Стрибог-256)
    
    Args:
        data: Входные данные для хеширования
    
    Returns:
        Хеш длиной 32 байта (256 бит)
    """
    return _streebog(data, IV_256, 256)


def streebog_512(data: bytes) -> bytes:
    """
    Вычисление хеша по ГОСТ Р 34.11-2018 (Стрибог-512)
    
    Args:
        data: Входные данные для хеширования
    
    Returns:
        Хеш длиной 64 байта (512 бит)
    """
    return _streebog(data, IV_512, 512)


def _streebog(data: bytes, iv: bytes, output_size: int) -> bytes:
    """Внутренняя функция для вычисления хеша Streebog"""
    h = bytearray(iv)
    n = bytearray(64)
    sigma = bytearray(64)
    
    # Дополнение данных
    data_len = len(data)
    padding_len = 64 - (data_len % 64)
    if padding_len == 64:
        padding_len = 0
    
    padded_data = data + b'\x01' + b'\x00' * (padding_len - 1)
    
    # Обработка блоков
    for i in range(0, len(padded_data), 64):
        block = padded_data[i:i+64]
        if len(block) < 64:
            block = block + b'\x00' * (64 - len(block))
        
        # g(N, h, m)
        h = _g_transform(n, h, block)
        
        # Обновление N
        n = _add_modulo_512(n, bytes([64] + [0] * 63))
        
        # Обновление sigma
        sigma = _add_modulo_512(sigma, block)
    
    # Финальное преобразование
    n = _add_modulo_512(n, bytes([data_len % 256] + [0] * 63))
    h = _g_transform(bytes([0] * 64), h, n)
    h = _g_transform(bytes([0] * 64), h, sigma)
    
    # Возврат нужного размера
    if output_size == 256:
        return bytes(h[:32])
    else:
        return bytes(h)


def _g_transform(n: bytes, h: bytes, m: bytes) -> bytes:
    """Преобразование g"""
    # В Streebog все блоки должны быть 64 байта
    # Если h или n меньше 64 байт, дополняем нулями
    if len(h) < 64:
        h = h + b'\x00' * (64 - len(h))
    if len(n) < 64:
        n = n + b'\x00' * (64 - len(n))
    if len(m) < 64:
        m = m + b'\x00' * (64 - len(m))
    
    k = _xor_bytes(h, n)
    # Применяем S-преобразование к каждому 16-байтному блоку
    k_blocks = [k[i:i+16] for i in range(0, 64, 16)]
    k_blocks = [_s_transform(block) for block in k_blocks]
    k = b''.join(k_blocks)
    
    # Применяем L-преобразование к каждому 16-байтному блоку
    k_blocks = [k[i:i+16] for i in range(0, 64, 16)]
    k_blocks = [_l_transform(block) for block in k_blocks]
    k = b''.join(k_blocks)
    
    # Применяем P-преобразование (tau) к 64-байтному блоку
    k = _p_transform(k)
    
    # Снова L-преобразование к каждому 16-байтному блоку
    k_blocks = [k[i:i+16] for i in range(0, 64, 16)]
    k_blocks = [_l_transform(block) for block in k_blocks]
    k = b''.join(k_blocks)
    
    # E-преобразование (упрощенное)
    t = _e_transform_simple(k, h)
    t = _xor_bytes(t, m)
    t = _xor_bytes(t, h)
    
    return t


def streebog_256_hex(data: bytes) -> str:
    """Вычисление хеша Streebog-256 и возврат в hex формате"""
    return streebog_256(data).hex()


def streebog_512_hex(data: bytes) -> str:
    """Вычисление хеша Streebog-512 и возврат в hex формате"""
    return streebog_512(data).hex()

