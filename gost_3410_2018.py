"""
Реализация электронной цифровой подписи по ГОСТ Р 34.10-2018
Основано на примере: https://github.com/Ilya-Danshin/GOST_34.10_2018_sign
"""

import hashlib
import json
import secrets
from typing import Tuple, Dict

# Параметры эллиптической кривой (из примера)
# Модуль p (простое число)
P = 57896044630612021680684936114742422271145183870487080309667128995208157569947
# Порядок группы q
Q = 28948022315306010840342468057371211135571302038761442251594012761075345324491
# Коэффициенты уравнения кривой: y^2 = x^3 + a*x + b (mod p)
A = 1
B = 19750513962881385028059495396984460236743646692126413053976069443380491067343
# Генерирующая точка P
PX = 43490682822985073571091311123441225129011272278165566160439297012894969619553
PY = 53273700124912449490307054424387372532482586733448415163119878489682918137700


def _mod_inverse(a: int, m: int) -> int:
    """Вычисление обратного элемента по модулю m (расширенный алгоритм Евклида)"""
    if m == 1:
        return 0
    m0, x0, x1 = m, 0, 1
    while a > 1:
        q = a // m
        m, a = a % m, m
        x0, x1 = x1 - q * x0, x0
    if x1 < 0:
        x1 += m0
    return x1


def _point_add(p1: Tuple[int, int], p2: Tuple[int, int]) -> Tuple[int, int]:
    """Сложение точек эллиптической кривой"""
    if p1 is None:
        return p2
    if p2 is None:
        return p1
    
    x1, y1 = p1
    x2, y2 = p2
    
    if x1 == x2:
        if y1 == y2:
            # Удвоение точки
            if y1 == 0:
                return None
            s = (3 * x1 * x1 + A) * _mod_inverse(2 * y1, P) % P
        else:
            # Точки симметричны
            return None
    else:
        # Обычное сложение
        s = (y2 - y1) * _mod_inverse(x2 - x1, P) % P
    
    x3 = (s * s - x1 - x2) % P
    y3 = (s * (x1 - x3) - y1) % P
    return (x3, y3)


def _point_multiply(k: int, point: Tuple[int, int]) -> Tuple[int, int]:
    """Умножение точки на скаляр (метод двоичного разложения)"""
    if k == 0:
        return None
    if k == 1:
        return point
    
    result = None
    addend = point
    
    while k:
        if k & 1:
            result = _point_add(result, addend)
        addend = _point_add(addend, addend)
        k >>= 1
    
    return result


def _streebog_256(data: bytes) -> bytes:
    """
    Хеширование по ГОСТ Р 34.11-2018 (Стрибог-256)
    """
    from streebog import streebog_256
    return streebog_256(data)


def generate_private_key(seed: str) -> int:
    """
    Генерация приватного ключа из seed
    Возвращает число d ∈ [1, q-1]
    """
    hash_bytes = _streebog_256(seed.encode('utf-8'))
    d = int.from_bytes(hash_bytes, 'big') % (Q - 1) + 1
    return d


def get_public_key(private_key: int) -> Tuple[int, int]:
    """
    Вычисление публичного ключа из приватного
    Q = d * P, где P - генерирующая точка
    """
    P_point = (PX, PY)
    Q = _point_multiply(private_key, P_point)
    return Q


def sign(message_hash: str, private_key: int) -> Dict[str, str]:
    """
    Подписание сообщения по ГОСТ 34.10-2018
    
    Args:
        message_hash: Хеш сообщения (hex строка)
        private_key: Приватный ключ d ∈ [1, q-1]
    
    Returns:
        Словарь с полями 'r' и 's' (hex строки)
    """
    # Преобразуем хеш в число
    H = int(message_hash, 16) % Q
    if H == 0:
        H = 1
    
    P_point = (PX, PY)
    
    # Генерируем подпись
    while True:
        # Шаг 1: Генерируем случайное число k
        k = secrets.randbelow(Q - 1) + 1
        
        # Шаг 2: Вычисляем точку C = k * P
        C = _point_multiply(k, P_point)
        if C is None:
            continue
        
        # Шаг 3: Вычисляем r = Cx mod q
        r = C[0] % Q
        if r == 0:
            continue
        
        # Шаг 4: Вычисляем s = (r * d + k * H) mod q
        s = (r * private_key + k * H) % Q
        if s == 0:
            continue
        
        # Шаг 5: Возвращаем подпись (r, s)
        return {
            'r': format(r, '064x'),  # 256 бит = 64 hex символа
            's': format(s, '064x')
        }


def verify(message_hash: str, signature: Dict[str, str], public_key: Tuple[int, int]) -> bool:
    """
    Проверка подписи по ГОСТ 34.10-2018
    
    Args:
        message_hash: Хеш сообщения (hex строка)
        signature: Словарь с полями 'r' и 's' (hex строки)
        public_key: Публичный ключ (точка Q)
    
    Returns:
        True если подпись валидна, False иначе
    """
    try:
        r = int(signature['r'], 16)
        s = int(signature['s'], 16)
        H = int(message_hash, 16) % Q
        if H == 0:
            H = 1
        
        # Проверка диапазонов
        if not (1 <= r < Q) or not (1 <= s < Q):
            return False
        
        # Вычисляем v = H^(-1) mod q
        v = _mod_inverse(H, Q)
        
        # Вычисляем z1 = s * v mod q
        z1 = (s * v) % Q
        
        # Вычисляем z2 = -r * v mod q
        z2 = (-r * v) % Q
        
        P_point = (PX, PY)
        
        # Вычисляем C = z1 * P + z2 * Q
        C1 = _point_multiply(z1, P_point)
        C2 = _point_multiply(z2, public_key)
        C = _point_add(C1, C2)
        
        if C is None:
            return False
        
        # Проверяем r == Cx mod q
        return (C[0] % Q) == r
    except Exception:
        return False


def signature_to_string(signature: Dict[str, str]) -> str:
    """Преобразование подписи в JSON строку"""
    return json.dumps(signature, separators=(',', ':'))


def signature_from_string(sig_str: str) -> Dict[str, str]:
    """Парсинг подписи из JSON строки"""
    try:
        return json.loads(sig_str)
    except Exception:
        return {'r': '0' * 64, 's': '0' * 64}

