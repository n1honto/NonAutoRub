import hashlib
import json
import secrets
from typing import Tuple, Dict

P = 57896044630612021680684936114742422271145183870487080309667128995208157569947
Q = 28948022315306010840342468057371211135571302038761442251594012761075345324491
A = 1
B = 19750513962881385028059495396984460236743646692126413053976069443380491067343
PX = 43490682822985073571091311123441225129011272278165566160439297012894969619553
PY = 53273700124912449490307054424387372532482586733448415163119878489682918137700


def _mod_inverse(a: int, m: int) -> int:
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
    if p1 is None:
        return p2
    if p2 is None:
        return p1
    
    x1, y1 = p1
    x2, y2 = p2
    
    if x1 == x2:
        if y1 == y2:
            if y1 == 0:
                return None
            s = (3 * x1 * x1 + A) * _mod_inverse(2 * y1, P) % P
        else:
            return None
    else:
        s = (y2 - y1) * _mod_inverse(x2 - x1, P) % P
    
    x3 = (s * s - x1 - x2) % P
    y3 = (s * (x1 - x3) - y1) % P
    return (x3, y3)


def _point_multiply(k: int, point: Tuple[int, int]) -> Tuple[int, int]:
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
    from streebog import streebog_256
    return streebog_256(data)


def generate_private_key(seed: str) -> int:
    hash_bytes = _streebog_256(seed.encode('utf-8'))
    d = int.from_bytes(hash_bytes, 'big') % (Q - 1) + 1
    return d


def get_public_key(private_key: int) -> Tuple[int, int]:
    P_point = (PX, PY)
    Q = _point_multiply(private_key, P_point)
    return Q


def sign(message_hash: str, private_key: int) -> Dict[str, str]:
    H = int(message_hash, 16) % Q
    if H == 0:
        H = 1
    
    P_point = (PX, PY)
    
    while True:
        k = secrets.randbelow(Q - 1) + 1
        
        C = _point_multiply(k, P_point)
        if C is None:
            continue
        
        r = C[0] % Q
        if r == 0:
            continue
        
        s = (r * private_key + k * H) % Q
        if s == 0:
            continue
        
        return {
            'r': format(r, '064x'),
            's': format(s, '064x')
        }


def verify(message_hash: str, signature: Dict[str, str], public_key: Tuple[int, int]) -> bool:
    try:
        r = int(signature['r'], 16)
        s = int(signature['s'], 16)
        H = int(message_hash, 16) % Q
        if H == 0:
            H = 1
        
        if not (1 <= r < Q) or not (1 <= s < Q):
            return False
        
        v = _mod_inverse(H, Q)
        z1 = (s * v) % Q
        z2 = (-r * v) % Q
        
        P_point = (PX, PY)
        
        C1 = _point_multiply(z1, P_point)
        C2 = _point_multiply(z2, public_key)
        C = _point_add(C1, C2)
        
        if C is None:
            return False
        
        return (C[0] % Q) == r
    except Exception:
        return False


def signature_to_string(signature: Dict[str, str]) -> str:
    return json.dumps(signature, separators=(',', ':'))


def signature_from_string(sig_str: str) -> Dict[str, str]:
    try:
        return json.loads(sig_str)
    except Exception:
        return {'r': '0' * 64, 's': '0' * 64}

