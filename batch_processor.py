from typing import List, Dict, Callable, Optional
from datetime import datetime
import threading
from queue import Queue
import time


class BatchProcessor:
    def __init__(self, batch_size: int = 10, max_wait_seconds: float = 1.0):
        self.batch_size = batch_size
        self.max_wait_seconds = max_wait_seconds
        self.queue: Queue = Queue()
        self.processing = False
        self.thread: Optional[threading.Thread] = None
        self.lock = threading.Lock()
    
    def add_item(self, item: Dict) -> None:
        self.queue.put(item)
        if not self.processing:
            self._start_processing()
    
    def _start_processing(self) -> None:
        if self.thread and self.thread.is_alive():
            return
        
        self.processing = True
        self.thread = threading.Thread(target=self._process_batches, daemon=True)
        self.thread.start()
    
    def _process_batches(self) -> None:
        batch: List[Dict] = []
        last_process_time = time.time()
        
        while self.processing or not self.queue.empty():
            try:
                item = self.queue.get(timeout=0.1)
                batch.append(item)
                
                current_time = time.time()
                time_since_last_process = current_time - last_process_time
                
                if len(batch) >= self.batch_size or time_since_last_process >= self.max_wait_seconds:
                    if batch:
                        self._process_batch(batch)
                        batch = []
                        last_process_time = current_time
                
            except:
                if batch and (time.time() - last_process_time) >= self.max_wait_seconds:
                    self._process_batch(batch)
                    batch = []
                    last_process_time = time.time()
    
    def _process_batch(self, batch: List[Dict]) -> None:
        raise NotImplementedError("Subclass must implement _process_batch")
    
    def stop(self) -> None:
        self.processing = False
        if self.thread:
            self.thread.join(timeout=5.0)


class TransactionBatchProcessor(BatchProcessor):
    def __init__(self, process_func: Callable, batch_size: int = 10, max_wait_seconds: float = 1.0):
        super().__init__(batch_size, max_wait_seconds)
        self.process_func = process_func
    
    def _process_batch(self, batch: List[Dict]) -> None:
        try:
            self.process_func(batch)
        except Exception as e:
            print(f"Ошибка при обработке батча транзакций: {e}")


class OfflineTransactionBatchProcessor(BatchProcessor):
    def __init__(self, process_func: Callable, batch_size: int = 10, max_wait_seconds: float = 1.0):
        super().__init__(batch_size, max_wait_seconds)
        self.process_func = process_func
    
    def _process_batch(self, batch: List[Dict]) -> None:
        try:
            self.process_func(batch)
        except Exception as e:
            print(f"Ошибка при обработке батча оффлайн-транзакций: {e}")


class ContractBatchProcessor(BatchProcessor):
    def __init__(self, process_func: Callable, batch_size: int = 10, max_wait_seconds: float = 1.0):
        super().__init__(batch_size, max_wait_seconds)
        self.process_func = process_func
    
    def _process_batch(self, batch: List[Dict]) -> None:
        try:
            self.process_func(batch)
        except Exception as e:
            print(f"Ошибка при обработке батча контрактов: {e}")

