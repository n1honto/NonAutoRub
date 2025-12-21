# АНАЛИЗ РАСХОЖДЕНИЙ КОНСЕНСУСА RAFT С ТРЕБОВАНИЯМИ ДИПЛОМА

## ТРЕБОВАНИЯ ИЗ ДИПЛОМА

### 1. Адаптация Raft под гибридную архитектуру
- **ЦБ по умолчанию LEADER** - не участвует в выборах в штатном режиме
- **ФО только FOLLOWER** - голосуют за принятие блоков, предложенных лидером
- **Выборы лидера** инициируются только при детектировании отказа ЦБ (таймаут heartbeat)
- **Временный лидер** выбирается среди ФО по наибольшему `log_index`
- **При восстановлении ЦБ** автоматически возвращается к роли лидера

### 2. Структура данных
- ✅ `raft_state` - хранение состояния узла (current_term)
- ✅ `raft_log` - лог записей Raft (term, log_index, block_hash, timestamp)
- ✅ `raft_votes` - голоса в выборах (term, candidate_id, voter_id, timestamp)

### 3. События консенсуса
- ✅ ELECTION_START - начало выборов
- ✅ VOTE_GRANTED - голос получен
- ✅ LEADER_ELECTED - лидер избран
- ✅ ELECTION_FAILED - выборы не удались
- ✅ APPEND_ENTRIES - запись добавлена в лог
- ✅ LEADER_APPEND - лидер добавил запись
- ✅ REPLICATION - репликация выполнена
- ✅ COMMITTED - блок зафиксирован
- ✅ ENTRY_APPLIED - запись применена

---

## ТЕКУЩАЯ РЕАЛИЗАЦИЯ

### Файл: `consensus.py`

**Текущее поведение:**
1. Все узлы начинают как `FOLLOWER` (строка 42)
2. Любой узел может стать лидером через выборы
3. Выборы инициируются при таймауте heartbeat (строка 311)
4. Нет логики определения типа узла (ЦБ или банк)
5. Нет логики выбора временного лидера по `log_index`
6. Нет логики автоматического возврата ЦБ к роли лидера

---

## РАСХОЖДЕНИЯ С ДИПЛОМОМ

### ❌ РАСХОЖДЕНИЕ 1: ЦБ не является лидером по умолчанию

**Требование диплома:**
> "Центральный банк по умолчанию находится в состоянии LEADER и не участвует в выборах в штатном режиме"

**Текущая реализация:**
```python
# consensus.py, строка 42
self.state = RaftState.FOLLOWER  # ❌ Все узлы начинают как FOLLOWER
```

**Проблема:** ЦБ должен начинать как LEADER, а не FOLLOWER

---

### ❌ РАСХОЖДЕНИЕ 2: ЦБ участвует в выборах

**Требование диплома:**
> "Центральный банк по умолчанию находится в состоянии LEADER и не участвует в выборах в штатном режиме"

**Текущая реализация:**
```python
# consensus.py, строка 143-196
def start_election(self) -> bool:
    if self._is_leader():
        return True
    # ... любой узел может инициировать выборы
```

**Проблема:** ЦБ не должен участвовать в выборах в штатном режиме

---

### ❌ РАСХОЖДЕНИЕ 3: ФО могут стать лидером в штатном режиме

**Требование диплома:**
> "Финансовые организации находятся в состоянии FOLLOWER и только голосуют за принятие блоков, предложенных лидером"

**Текущая реализация:**
```python
# consensus.py, строка 143-196
def start_election(self) -> bool:
    # ... любой узел может стать лидером
```

**Проблема:** ФО не должны становиться лидером в штатном режиме, только при отказе ЦБ

---

### ❌ РАСХОЖДЕНИЕ 4: Выборы инициируются любым узлом

**Требование диплома:**
> "Выборы лидера в адаптированной версии инициируются только при детектировании отказа Центрального банка (таймаут heartbeat превышает установленный порог)"

**Текущая реализация:**
```python
# consensus.py, строка 310-315
if time.time() - self.last_heartbeat > self.election_timeout:
    if self.start_election():  # ❌ Любой узел может инициировать выборы
```

**Проблема:** Выборы должны инициироваться только при отказе ЦБ, а не любым узлом

---

### ❌ РАСХОЖДЕНИЕ 5: Нет выбора временного лидера по log_index

**Требование диплома:**
> "При выборе временного лидера среди финансовых организаций предпочтение отдается узлу с наибольшим последним логическим индексом в логе"

**Текущая реализация:**
```python
# consensus.py, строка 143-196
def start_election(self) -> bool:
    # ❌ Нет логики выбора по log_index
```

**Проблема:** Временный лидер должен выбираться по наибольшему `log_index`

---

### ❌ РАСХОЖДЕНИЕ 6: Нет автоматического возврата ЦБ к роли лидера

**Требование диплома:**
> "При восстановлении Центрального банка после сбоя он автоматически возвращается к роли лидера, а временный лидер добровольно передает управление обратно"

**Текущая реализация:**
```python
# ❌ Нет логики автоматического возврата ЦБ к роли лидера
```

**Проблема:** При восстановлении ЦБ должен автоматически вернуться к роли лидера

---

### ❌ РАСХОЖДЕНИЕ 7: Нет определения типа узла

**Требование диплома:**
> "Центральный банк является не просто узлом, а оператором системы с исключительными полномочиями"

**Текущая реализация:**
```python
# consensus.py, строка 37
def __init__(self, db: DatabaseManager, node_id: str = "ЦБ РФ (лидер)") -> None:
    # ❌ Нет определения, является ли узел ЦБ или банком
```

**Проблема:** Нужно определять тип узла (ЦБ или банк) и вести себя соответственно

---

## ПЛАН ИСПРАВЛЕНИЙ

### ИЗМЕНЕНИЕ 1: Добавить определение типа узла

**Файл:** `consensus.py`

**Добавить в `__init__`:**
```python
def __init__(self, db: DatabaseManager, node_id: str = "ЦБ РФ (лидер)") -> None:
    self.db = db
    self.node_id = node_id
    
    # Определяем тип узла: ЦБ или банк
    self.is_central_bank = self._is_central_bank_node()
    
    # ... остальной код
```

**Добавить метод:**
```python
def _is_central_bank_node(self) -> bool:
    """
    Определяет, является ли узел Центральным банком или банком (ФО).
    
    Returns:
        True если это ЦБ, False если банк
    """
    # Проверяем по node_id или по типу БД
    cbr_indicators = ["CBR", "ЦБ", "Центральный банк", "ЦБ РФ"]
    return any(indicator in self.node_id for indicator in cbr_indicators) or \
           self.db.is_central_bank()
```

---

### ИЗМЕНЕНИЕ 2: ЦБ по умолчанию LEADER

**Файл:** `consensus.py`

**Изменить в `__init__`:**
```python
# БЫЛО:
self.state = RaftState.FOLLOWER

# СТАНЕТ:
if self.is_central_bank:
    self.state = RaftState.LEADER  # ЦБ по умолчанию лидер
    self.leader_id = self.node_id
else:
    self.state = RaftState.FOLLOWER  # Банки по умолчанию последователи
```

---

### ИЗМЕНЕНИЕ 3: ЦБ не участвует в выборах в штатном режиме

**Файл:** `consensus.py`

**Изменить метод `start_election`:**
```python
def start_election(self) -> bool:
    # ЦБ не участвует в выборах в штатном режиме
    if self.is_central_bank:
        return False  # ЦБ не инициирует выборы
    
    if self._is_leader():
        return True
    
    # ... остальной код выборов
```

---

### ИЗМЕНЕНИЕ 4: ФО не могут стать лидером в штатном режиме

**Файл:** `consensus.py`

**Изменить метод `start_election`:**
```python
def start_election(self) -> bool:
    # ЦБ не участвует в выборах
    if self.is_central_bank:
        return False
    
    # ФО могут стать лидером только при отказе ЦБ
    # Проверяем, что ЦБ действительно недоступен
    if not self._is_cbr_failed():
        return False  # ЦБ доступен, выборы не нужны
    
    # ... остальной код выборов
```

**Добавить метод:**
```python
def _is_cbr_failed(self) -> bool:
    """
    Проверяет, недоступен ли ЦБ (таймаут heartbeat превышает порог).
    
    Returns:
        True если ЦБ недоступен, False если доступен
    """
    # Проверяем последний heartbeat от ЦБ
    # Если heartbeat не получен в течение election_timeout, ЦБ считается недоступным
    if self.leader_id and "CBR" in self.leader_id:
        time_since_heartbeat = time.time() - self.last_heartbeat
        return time_since_heartbeat > self.election_timeout
    return True  # Если лидер не установлен, считаем ЦБ недоступным
```

---

### ИЗМЕНЕНИЕ 5: Выборы только при отказе ЦБ

**Файл:** `consensus.py`

**Изменить метод `run_round`:**
```python
def run_round(self, block_hash: str) -> List[ConsensusEvent]:
    timeline: List[ConsensusEvent] = []
    
    # ЦБ всегда лидер в штатном режиме
    if self.is_central_bank:
        if not self._is_leader():
            # Восстановление ЦБ после сбоя
            self.state = RaftState.LEADER
            self.leader_id = self.node_id
            self.record_event(
                block_hash,
                "ЦБ восстановлен и вернулся к роли лидера",
                self.node_id,
                "LEADER_RESTORED",
            )
    else:
        # Банки проверяют доступность ЦБ
        if time.time() - self.last_heartbeat > self.election_timeout:
            # ЦБ недоступен, инициируем выборы временного лидера
            if not self._is_leader():
                if self.start_election():
                    timeline.extend(self.get_recent_events(limit=10))
        else:
            # ЦБ доступен, остаемся последователями
            if self._is_leader():
                # Если мы временный лидер, но ЦБ восстановился, передаем управление
                self._transfer_leadership_to_cbr()
            return timeline
    
    # ... остальной код
```

---

### ИЗМЕНЕНИЕ 6: Выбор временного лидера по log_index

**Файл:** `consensus.py`

**Изменить метод `start_election`:**
```python
def start_election(self) -> bool:
    # ЦБ не участвует в выборах
    if self.is_central_bank:
        return False
    
    # Проверяем, что ЦБ действительно недоступен
    if not self._is_cbr_failed():
        return False
    
    # Получаем все узлы-кандидаты (только банки)
    candidate_nodes = self._get_candidate_nodes()
    
    if not candidate_nodes:
        return False
    
    # Выбираем кандидата с наибольшим log_index
    best_candidate = self._select_best_candidate(candidate_nodes)
    
    if best_candidate != self.node_id:
        # Мы не лучший кандидат, голосуем за лучшего
        return False
    
    # Мы лучший кандидат, инициируем выборы
    self.current_term += 1
    self.state = RaftState.CANDIDATE
    # ... остальной код выборов
```

**Добавить методы:**
```python
def _get_candidate_nodes(self) -> List[str]:
    """
    Получает список узлов-кандидатов (только банки, не ЦБ).
    
    Returns:
        Список node_id банков
    """
    all_nodes = self.get_nodes()
    return [node for node in all_nodes if node != self.node_id and not self._is_cbr_node(node)]

def _is_cbr_node(self, node_id: str) -> bool:
    """Проверяет, является ли узел ЦБ"""
    cbr_indicators = ["CBR", "ЦБ", "Центральный банк", "ЦБ РФ"]
    return any(indicator in node_id for indicator in cbr_indicators)

def _select_best_candidate(self, candidates: List[str]) -> str:
    """
    Выбирает лучшего кандидата по наибольшему log_index.
    
    Returns:
        node_id лучшего кандидата
    """
    best_candidate = None
    max_log_index = -1
    
    for candidate in candidates:
        # Получаем log_index кандидата (через БД или сеть)
        log_index = self._get_node_log_index(candidate)
        if log_index > max_log_index:
            max_log_index = log_index
            best_candidate = candidate
    
    return best_candidate or self.node_id

def _get_node_log_index(self, node_id: str) -> int:
    """
    Получает последний log_index узла.
    В реальной системе это делается через сеть, здесь упрощенно.
    """
    # Упрощенная версия: для текущего узла берем из БД
    if node_id == self.node_id:
        return self._get_last_log_index()
    # Для других узлов нужно запрашивать через сеть
    # Пока возвращаем 0 (нужно реализовать через P2P)
    return 0
```

---

### ИЗМЕНЕНИЕ 7: Автоматический возврат ЦБ к роли лидера

**Файл:** `consensus.py`

**Добавить метод:**
```python
def _transfer_leadership_to_cbr(self) -> None:
    """
    Передает управление обратно ЦБ при его восстановлении.
    Вызывается временным лидером (банком) при обнаружении восстановления ЦБ.
    """
    if not self.is_central_bank and self._is_leader():
        # Мы временный лидер, но ЦБ восстановился
        self.state = RaftState.FOLLOWER
        self.leader_id = None  # Будет установлен при получении heartbeat от ЦБ
        self.record_event(
            "leadership-transfer",
            f"{self.node_id} передает управление обратно ЦБ",
            self.node_id,
            "LEADERSHIP_TRANSFERRED",
        )
```

**Изменить метод `append_entries`:**
```python
def append_entries(self, block_hash: str, leader_id: str, leader_term: int) -> bool:
    # Если мы временный лидер, но получили запись от ЦБ, передаем управление
    if self._is_leader() and not self.is_central_bank:
        if self._is_cbr_node(leader_id):
            self._transfer_leadership_to_cbr()
    
    # ... остальной код
```

---

### ИЗМЕНЕНИЕ 8: Обновить метод `run_round` для ЦБ

**Файл:** `consensus.py`

**Изменить метод `run_round`:**
```python
def run_round(self, block_hash: str) -> List[ConsensusEvent]:
    timeline: List[ConsensusEvent] = []
    
    # ЦБ всегда лидер в штатном режиме
    if self.is_central_bank:
        if not self._is_leader():
            # Восстановление ЦБ после сбоя
            self.state = RaftState.LEADER
            self.leader_id = self.node_id
            self.last_heartbeat = time.time()
            self.record_event(
                block_hash,
                "ЦБ восстановлен и вернулся к роли лидера",
                self.node_id,
                "LEADER_RESTORED",
            )
        
        # ЦБ не участвует в выборах
        if not self._is_leader():
            return timeline
        
        # ЦБ как лидер добавляет запись в лог
        log_index = self._append_log_entry(block_hash)
        self.record_event(
            block_hash,
            f"Лидер добавил запись: индекс {log_index}",
            self.node_id,
            "LEADER_APPEND",
        )
        
        # Репликация на последователей
        successful, failed = self.replicate_to_followers(block_hash)
        timeline.extend(self.get_recent_events(limit=20))
        return timeline
    
    # Банки (ФО) - только последователи
    else:
        # Проверяем доступность ЦБ
        if time.time() - self.last_heartbeat > self.election_timeout:
            # ЦБ недоступен, проверяем, нужны ли выборы
            if not self._is_leader():
                # Инициируем выборы временного лидера
                if self.start_election():
                    timeline.extend(self.get_recent_events(limit=10))
        else:
            # ЦБ доступен
            if self._is_leader():
                # Если мы временный лидер, но ЦБ восстановился, передаем управление
                self._transfer_leadership_to_cbr()
            # Остаемся последователями, ждем инструкций от ЦБ
            return timeline
        
        # Если мы временный лидер, обрабатываем блок
        if self._is_leader():
            log_index = self._append_log_entry(block_hash)
            self.record_event(
                block_hash,
                f"Временный лидер добавил запись: индекс {log_index}",
                self.node_id,
                "LEADER_APPEND",
            )
            successful, failed = self.replicate_to_followers(block_hash)
            timeline.extend(self.get_recent_events(limit=20))
        
        return timeline
```

---

## ИТОГОВЫЙ ЧЕКЛИСТ ИЗМЕНЕНИЙ

- [ ] Добавить определение типа узла (ЦБ или банк)
- [ ] ЦБ по умолчанию LEADER
- [ ] ЦБ не участвует в выборах в штатном режиме
- [ ] ФО только FOLLOWER в штатном режиме
- [ ] Выборы только при отказе ЦБ
- [ ] Выбор временного лидера по наибольшему log_index
- [ ] Автоматический возврат ЦБ к роли лидера при восстановлении
- [ ] Передача управления от временного лидера обратно ЦБ

---

## ЗАКЛЮЧЕНИЕ

После реализации всех изменений консенсус будет соответствовать требованиям диплома:
1. ✅ ЦБ по умолчанию лидер, не участвует в выборах
2. ✅ ФО только последователи, голосуют за блоки
3. ✅ Выборы только при отказе ЦБ
4. ✅ Временный лидер выбирается по log_index
5. ✅ Автоматический возврат ЦБ к роли лидера

