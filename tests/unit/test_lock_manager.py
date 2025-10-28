import pytest
from unittest.mock import MagicMock, AsyncMock
import asyncio

from src.nodes.lock_manager import (
    LockManager, LockType, LockStatus, 
    DeadlockDetector, Lock, LockRequest
)
from src.utils.config import Config, NodeConfig, ClusterConfig, RaftConfig


@pytest.fixture
def config():
    return Config(
        node=NodeConfig(node_id="node1", host="localhost", port=8000),
        cluster=ClusterConfig(nodes=[
            NodeConfig(node_id="node1", host="localhost", port=8000),
            NodeConfig(node_id="node2", host="localhost", port=8001),
            NodeConfig(node_id="node3", host="localhost", port=8002),
        ]),
        raft=RaftConfig(
            election_timeout_min=50,
            election_timeout_max=100,
            heartbeat_interval=25,
        ),
    )


class TestDeadlockDetector:
    def test_add_wait(self):
        detector = DeadlockDetector()
        detector.add_wait("client1", {"client2", "client3"})
        
        assert "client2" in detector.wait_for_graph["client1"]
        assert "client3" in detector.wait_for_graph["client1"]
    
    def test_remove_wait(self):
        detector = DeadlockDetector()
        detector.add_wait("client1", {"client2"})
        detector.remove_wait("client1")
        
        assert "client1" not in detector.wait_for_graph
    
    def test_detect_no_cycle(self):
        detector = DeadlockDetector()
        detector.add_wait("A", {"B"})
        detector.add_wait("B", {"C"})
        
        cycle = detector.detect_cycle("A")
        assert cycle is None
    
    def test_detect_simple_cycle(self):
        detector = DeadlockDetector()
        detector.add_wait("A", {"B"})
        detector.add_wait("B", {"A"})
        
        cycle = detector.detect_cycle("A")
        assert cycle is not None
        assert "A" in cycle
        assert "B" in cycle
    
    def test_detect_longer_cycle(self):
        detector = DeadlockDetector()
        detector.add_wait("A", {"B"})
        detector.add_wait("B", {"C"})
        detector.add_wait("C", {"A"})
        
        cycle = detector.detect_cycle("A")
        assert cycle is not None
        assert len(cycle) == 4
    
    def test_has_deadlock(self):
        detector = DeadlockDetector()
        detector.add_wait("A", {"B"})
        detector.add_wait("B", {"C"})
        detector.add_wait("C", {"A"})
        
        cycle = detector.has_deadlock()
        assert cycle is not None


class TestLockType:
    def test_shared_lock_type(self):
        assert LockType.SHARED.value == "shared"
    
    def test_exclusive_lock_type(self):
        assert LockType.EXCLUSIVE.value == "exclusive"


class TestLockStatus:
    def test_all_statuses_exist(self):
        assert LockStatus.ACQUIRED
        assert LockStatus.WAITING
        assert LockStatus.RELEASED
        assert LockStatus.DENIED
        assert LockStatus.TIMEOUT
        assert LockStatus.DEADLOCK


class TestLock:
    def test_lock_creation(self):
        lock = Lock(
            resource_id="resource1",
            lock_type=LockType.EXCLUSIVE,
        )
        
        assert lock.resource_id == "resource1"
        assert lock.lock_type == LockType.EXCLUSIVE
        assert len(lock.holders) == 0
        assert len(lock.waiting) == 0
    
    def test_lock_with_holders(self):
        lock = Lock(
            resource_id="resource1",
            lock_type=LockType.SHARED,
            holders={"client1", "client2"},
        )
        
        assert "client1" in lock.holders
        assert "client2" in lock.holders


class TestLockRequest:
    def test_lock_request_creation(self):
        request = LockRequest(
            client_id="client1",
            resource_id="resource1",
            lock_type=LockType.EXCLUSIVE,
        )
        
        assert request.client_id == "client1"
        assert request.resource_id == "resource1"
        assert request.lock_type == LockType.EXCLUSIVE
        assert request.timestamp > 0
