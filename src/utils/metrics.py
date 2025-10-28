import time
from dataclasses import dataclass, field
from typing import Dict, Optional
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST


@dataclass
class MetricsRegistry:
    raft_elections: Counter = field(default_factory=lambda: Counter(
        "raft_elections_total", "Total number of Raft elections", ["node_id", "result"]
    ))
    raft_term: Gauge = field(default_factory=lambda: Gauge(
        "raft_current_term", "Current Raft term", ["node_id"]
    ))
    raft_state: Gauge = field(default_factory=lambda: Gauge(
        "raft_state", "Current Raft state (0=follower, 1=candidate, 2=leader)", ["node_id"]
    ))
    raft_log_entries: Gauge = field(default_factory=lambda: Gauge(
        "raft_log_entries", "Number of entries in Raft log", ["node_id"]
    ))
    
    lock_operations: Counter = field(default_factory=lambda: Counter(
        "lock_operations_total", "Total lock operations", ["node_id", "operation", "lock_type"]
    ))
    lock_wait_time: Histogram = field(default_factory=lambda: Histogram(
        "lock_wait_seconds", "Time spent waiting for lock", ["node_id", "lock_type"]
    ))
    active_locks: Gauge = field(default_factory=lambda: Gauge(
        "active_locks", "Number of currently held locks", ["node_id", "lock_type"]
    ))
    deadlock_detections: Counter = field(default_factory=lambda: Counter(
        "deadlock_detections_total", "Total deadlock detections", ["node_id"]
    ))
    
    queue_messages: Counter = field(default_factory=lambda: Counter(
        "queue_messages_total", "Total queue messages", ["node_id", "operation"]
    ))
    queue_size: Gauge = field(default_factory=lambda: Gauge(
        "queue_size", "Current queue size", ["node_id", "queue_name"]
    ))
    message_latency: Histogram = field(default_factory=lambda: Histogram(
        "message_latency_seconds", "Message processing latency", ["node_id"]
    ))
    
    cache_operations: Counter = field(default_factory=lambda: Counter(
        "cache_operations_total", "Total cache operations", ["node_id", "operation", "result"]
    ))
    cache_size: Gauge = field(default_factory=lambda: Gauge(
        "cache_size", "Current cache size", ["node_id"]
    ))
    cache_state_transitions: Counter = field(default_factory=lambda: Counter(
        "cache_state_transitions_total", "MESI state transitions", ["node_id", "from_state", "to_state"]
    ))
    
    rpc_requests: Counter = field(default_factory=lambda: Counter(
        "rpc_requests_total", "Total RPC requests", ["node_id", "method", "status"]
    ))
    rpc_latency: Histogram = field(default_factory=lambda: Histogram(
        "rpc_latency_seconds", "RPC request latency", ["node_id", "method"]
    ))


class MetricsCollector:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.registry = MetricsRegistry()
        self._timers: Dict[str, float] = {}
    
    def record_election(self, result: str):
        self.registry.raft_elections.labels(node_id=self.node_id, result=result).inc()
    
    def set_term(self, term: int):
        self.registry.raft_term.labels(node_id=self.node_id).set(term)
    
    def set_state(self, state: int):
        self.registry.raft_state.labels(node_id=self.node_id).set(state)
    
    def set_log_entries(self, count: int):
        self.registry.raft_log_entries.labels(node_id=self.node_id).set(count)
    
    def record_lock_operation(self, operation: str, lock_type: str):
        self.registry.lock_operations.labels(
            node_id=self.node_id, operation=operation, lock_type=lock_type
        ).inc()
    
    def record_lock_wait(self, lock_type: str, duration: float):
        self.registry.lock_wait_time.labels(node_id=self.node_id, lock_type=lock_type).observe(duration)
    
    def set_active_locks(self, lock_type: str, count: int):
        self.registry.active_locks.labels(node_id=self.node_id, lock_type=lock_type).set(count)
    
    def record_deadlock(self):
        self.registry.deadlock_detections.labels(node_id=self.node_id).inc()
    
    def record_queue_message(self, operation: str):
        self.registry.queue_messages.labels(node_id=self.node_id, operation=operation).inc()
    
    def set_queue_size(self, queue_name: str, size: int):
        self.registry.queue_size.labels(node_id=self.node_id, queue_name=queue_name).set(size)
    
    def record_message_latency(self, latency: float):
        self.registry.message_latency.labels(node_id=self.node_id).observe(latency)
    
    def record_cache_operation(self, operation: str, result: str):
        self.registry.cache_operations.labels(
            node_id=self.node_id, operation=operation, result=result
        ).inc()
    
    def set_cache_size(self, size: int):
        self.registry.cache_size.labels(node_id=self.node_id).set(size)
    
    def record_state_transition(self, from_state: str, to_state: str):
        self.registry.cache_state_transitions.labels(
            node_id=self.node_id, from_state=from_state, to_state=to_state
        ).inc()
    
    def record_rpc(self, method: str, status: str):
        self.registry.rpc_requests.labels(node_id=self.node_id, method=method, status=status).inc()
    
    def record_rpc_latency(self, method: str, latency: float):
        self.registry.rpc_latency.labels(node_id=self.node_id, method=method).observe(latency)
    
    def start_timer(self, name: str):
        self._timers[name] = time.time()
    
    def stop_timer(self, name: str) -> Optional[float]:
        if name in self._timers:
            elapsed = time.time() - self._timers.pop(name)
            return elapsed
        return None
    
    def get_metrics(self) -> bytes:
        return generate_latest()
    
    def get_content_type(self) -> str:
        return CONTENT_TYPE_LATEST
