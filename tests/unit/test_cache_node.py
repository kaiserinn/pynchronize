import pytest
import time

from src.nodes.cache_node import (
    CacheState, CacheEntry, LRUCache, MESIProtocol
)


class TestCacheState:
    def test_mesi_states_exist(self):
        assert CacheState.MODIFIED.value == "M"
        assert CacheState.EXCLUSIVE.value == "E"
        assert CacheState.SHARED.value == "S"
        assert CacheState.INVALID.value == "I"


class TestCacheEntry:
    def test_entry_creation(self):
        entry = CacheEntry(
            key="key1",
            value="value1",
            state=CacheState.EXCLUSIVE,
        )
        
        assert entry.key == "key1"
        assert entry.value == "value1"
        assert entry.state == CacheState.EXCLUSIVE
        assert entry.version == 0
    
    def test_entry_touch(self):
        entry = CacheEntry(
            key="key1",
            value="value1",
            state=CacheState.SHARED,
        )
        
        old_time = entry.last_access
        time.sleep(0.01)
        entry.touch()
        
        assert entry.last_access > old_time


class TestLRUCache:
    def test_put_and_get(self):
        cache = LRUCache(max_size=10)
        entry = CacheEntry(key="k1", value="v1", state=CacheState.EXCLUSIVE)
        
        cache.put("k1", entry)
        retrieved = cache.get("k1")
        
        assert retrieved is not None
        assert retrieved.value == "v1"
    
    def test_get_nonexistent(self):
        cache = LRUCache(max_size=10)
        
        result = cache.get("nonexistent")
        assert result is None
    
    def test_eviction(self):
        cache = LRUCache(max_size=3)
        
        for i in range(5):
            entry = CacheEntry(key=f"k{i}", value=f"v{i}", state=CacheState.SHARED)
            cache.put(f"k{i}", entry)
        
        assert cache.size() == 3
        assert cache.get("k0") is None
        assert cache.get("k1") is None
        assert cache.get("k4") is not None
    
    def test_lru_ordering(self):
        cache = LRUCache(max_size=3)
        
        cache.put("k1", CacheEntry(key="k1", value="v1", state=CacheState.SHARED))
        cache.put("k2", CacheEntry(key="k2", value="v2", state=CacheState.SHARED))
        cache.put("k3", CacheEntry(key="k3", value="v3", state=CacheState.SHARED))
        
        cache.get("k1")
        
        cache.put("k4", CacheEntry(key="k4", value="v4", state=CacheState.SHARED))
        
        assert cache.get("k1") is not None
        assert cache.get("k2") is None
        assert cache.get("k3") is not None
    
    def test_remove(self):
        cache = LRUCache(max_size=10)
        cache.put("k1", CacheEntry(key="k1", value="v1", state=CacheState.SHARED))
        
        removed = cache.remove("k1")
        
        assert removed is not None
        assert cache.get("k1") is None
    
    def test_contains(self):
        cache = LRUCache(max_size=10)
        cache.put("k1", CacheEntry(key="k1", value="v1", state=CacheState.SHARED))
        
        assert cache.contains("k1") is True
        assert cache.contains("k2") is False


class TestMESIProtocol:
    def test_local_read_miss(self):
        protocol = MESIProtocol("node1")
        
        state = protocol.on_local_read(None, from_other=False)
        assert state == CacheState.EXCLUSIVE
        
        state = protocol.on_local_read(None, from_other=True)
        assert state == CacheState.SHARED
    
    def test_local_read_hit(self):
        protocol = MESIProtocol("node1")
        entry = CacheEntry(key="k1", value="v1", state=CacheState.MODIFIED)
        
        state = protocol.on_local_read(entry, from_other=False)
        assert state == CacheState.MODIFIED
    
    def test_local_write(self):
        protocol = MESIProtocol("node1")
        
        state = protocol.on_local_write(None)
        assert state == CacheState.MODIFIED
        
        entry = CacheEntry(key="k1", value="v1", state=CacheState.SHARED)
        state = protocol.on_local_write(entry)
        assert state == CacheState.MODIFIED
    
    def test_bus_read_modified(self):
        protocol = MESIProtocol("node1")
        entry = CacheEntry(key="k1", value="v1", state=CacheState.MODIFIED)
        
        value, new_state, writeback = protocol.on_bus_read(entry)
        
        assert value == "v1"
        assert new_state == CacheState.SHARED
        assert writeback is True
    
    def test_bus_read_exclusive(self):
        protocol = MESIProtocol("node1")
        entry = CacheEntry(key="k1", value="v1", state=CacheState.EXCLUSIVE)
        
        value, new_state, writeback = protocol.on_bus_read(entry)
        
        assert value == "v1"
        assert new_state == CacheState.SHARED
        assert writeback is False
    
    def test_bus_read_invalid(self):
        protocol = MESIProtocol("node1")
        
        value, new_state, writeback = protocol.on_bus_read(None)
        
        assert value is None
        assert new_state == CacheState.INVALID
    
    def test_bus_write(self):
        protocol = MESIProtocol("node1")
        entry = CacheEntry(key="k1", value="v1", state=CacheState.SHARED)
        
        new_state = protocol.on_bus_write(entry)
        
        assert new_state == CacheState.INVALID
