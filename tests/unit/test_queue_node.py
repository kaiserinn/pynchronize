import pytest
import tempfile
import os

from src.nodes.queue_node import (
    ConsistentHash, QueueMessage, MessageStore, QueueNode
)


class TestConsistentHash:
    def test_add_node(self):
        ch = ConsistentHash()
        ch.add_node("node1")
        
        assert "node1" in ch.nodes
        assert len(ch.ring) == ch.replicas
    
    def test_remove_node(self):
        ch = ConsistentHash(["node1", "node2"])
        ch.remove_node("node1")
        
        assert "node1" not in ch.nodes
        assert "node2" in ch.nodes
    
    def test_get_node(self):
        ch = ConsistentHash(["node1", "node2", "node3"])
        
        node = ch.get_node("some-key")
        assert node in ["node1", "node2", "node3"]
    
    def test_get_node_consistent(self):
        ch = ConsistentHash(["node1", "node2", "node3"])
        
        node1 = ch.get_node("test-key")
        node2 = ch.get_node("test-key")
        
        assert node1 == node2
    
    def test_get_nodes_for_key(self):
        ch = ConsistentHash(["node1", "node2", "node3"])
        
        nodes = ch.get_nodes_for_key("key", count=2)
        
        assert len(nodes) == 2
        assert len(set(nodes)) == 2
    
    def test_empty_ring(self):
        ch = ConsistentHash()
        
        assert ch.get_node("key") is None
        assert ch.get_nodes_for_key("key", 3) == []


class TestQueueMessage:
    def test_message_creation(self):
        msg = QueueMessage(
            id="msg-1",
            queue_name="test-queue",
            payload={"data": "value"},
        )
        
        assert msg.id == "msg-1"
        assert msg.queue_name == "test-queue"
        assert msg.payload == {"data": "value"}
        assert msg.attempts == 0
        assert msg.acked is False
    
    def test_message_to_dict(self):
        msg = QueueMessage(
            id="msg-1",
            queue_name="test-queue",
            payload={"x": 1},
        )
        
        d = msg.to_dict()
        
        assert d["id"] == "msg-1"
        assert d["queue_name"] == "test-queue"
        assert d["payload"] == {"x": 1}
    
    def test_message_from_dict(self):
        d = {
            "id": "msg-2",
            "queue_name": "queue2",
            "payload": {"y": 2},
            "created_at": 1234567890.0,
            "attempts": 3,
            "last_attempt": 1234567900.0,
            "acked": True,
        }
        
        msg = QueueMessage.from_dict(d)
        
        assert msg.id == "msg-2"
        assert msg.attempts == 3
        assert msg.acked is True


class TestMessageStore:
    def test_add_and_get(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = MessageStore(tmpdir)
            
            msg = QueueMessage(
                id="msg-1",
                queue_name="queue1",
                payload={"test": True},
            )
            store.add(msg)
            
            retrieved = store.get("msg-1")
            assert retrieved is not None
            assert retrieved.id == "msg-1"
    
    def test_get_pending(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = MessageStore(tmpdir)
            
            for i in range(5):
                msg = QueueMessage(
                    id=f"msg-{i}",
                    queue_name="queue1",
                    payload={"i": i},
                )
                store.add(msg)
            
            pending = store.get_pending("queue1", limit=3)
            assert len(pending) == 3
    
    def test_ack(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = MessageStore(tmpdir)
            
            msg = QueueMessage(
                id="msg-1",
                queue_name="queue1",
                payload={},
            )
            store.add(msg)
            
            result = store.ack("msg-1")
            assert result is True
            
            pending = store.get_pending("queue1")
            assert len(pending) == 0
    
    def test_nack(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = MessageStore(tmpdir)
            
            msg = QueueMessage(
                id="msg-1",
                queue_name="queue1",
                payload={},
            )
            store.add(msg)
            
            store.nack("msg-1")
            
            retrieved = store.get("msg-1")
            assert retrieved.attempts == 1
    
    def test_queue_size(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = MessageStore(tmpdir)
            
            for i in range(3):
                msg = QueueMessage(
                    id=f"msg-{i}",
                    queue_name="queue1",
                    payload={},
                )
                store.add(msg)
            
            assert store.get_queue_size("queue1") == 3
            
            store.ack("msg-0")
            assert store.get_queue_size("queue1") == 2
