import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set
from enum import Enum
from collections import OrderedDict

from src.nodes.base_node import BaseNode
from src.utils.config import Config
from src.communication.message_passing import Message, MessageType


class CacheState(Enum):
    MODIFIED = "M"
    EXCLUSIVE = "E"
    SHARED = "S"
    INVALID = "I"


@dataclass
class CacheEntry:
    key: str
    value: Any
    state: CacheState
    version: int = 0
    last_access: float = field(default_factory=time.time)
    
    def touch(self):
        self.last_access = time.time()


class LRUCache:
    def __init__(self, max_size: int):
        self.max_size = max_size
        self.cache: OrderedDict[str, CacheEntry] = OrderedDict()
    
    def get(self, key: str) -> Optional[CacheEntry]:
        if key in self.cache:
            self.cache.move_to_end(key)
            entry = self.cache[key]
            entry.touch()
            return entry
        return None
    
    def put(self, key: str, entry: CacheEntry) -> Optional[CacheEntry]:
        evicted = None
        
        if key in self.cache:
            self.cache.move_to_end(key)
        else:
            if len(self.cache) >= self.max_size:
                evicted = self._evict()
        
        self.cache[key] = entry
        return evicted
    
    def remove(self, key: str) -> Optional[CacheEntry]:
        return self.cache.pop(key, None)
    
    def _evict(self) -> Optional[CacheEntry]:
        for key in list(self.cache.keys()):
            entry = self.cache[key]
            if entry.state != CacheState.MODIFIED:
                return self.cache.pop(key)
        
        if self.cache:
            key, entry = self.cache.popitem(last=False)
            return entry
        return None
    
    def get_all(self) -> List[CacheEntry]:
        return list(self.cache.values())
    
    def size(self) -> int:
        return len(self.cache)
    
    def contains(self, key: str) -> bool:
        return key in self.cache


class MESIProtocol:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.logger = logging.getLogger(f"mesi.{node_id}")
    
    def on_local_read(self, entry: Optional[CacheEntry], from_other: bool) -> CacheState:
        if entry is None:
            return CacheState.EXCLUSIVE if not from_other else CacheState.SHARED
        
        if entry.state == CacheState.INVALID:
            return CacheState.EXCLUSIVE if not from_other else CacheState.SHARED
        
        return entry.state
    
    def on_local_write(self, entry: Optional[CacheEntry]) -> CacheState:
        return CacheState.MODIFIED
    
    def on_bus_read(self, entry: Optional[CacheEntry]) -> tuple:
        if entry is None or entry.state == CacheState.INVALID:
            return (None, CacheState.INVALID, False)
        
        if entry.state == CacheState.MODIFIED:
            return (entry.value, CacheState.SHARED, True)
        
        if entry.state == CacheState.EXCLUSIVE:
            return (entry.value, CacheState.SHARED, False)
        
        return (entry.value, CacheState.SHARED, False)
    
    def on_bus_write(self, entry: Optional[CacheEntry]) -> CacheState:
        if entry is None:
            return CacheState.INVALID
        return CacheState.INVALID


class CacheNode(BaseNode):
    def __init__(self, config: Config):
        super().__init__(config)
        
        self.cache = LRUCache(config.cache.max_size)
        self.protocol = MESIProtocol(self.node_id)
        
        self.directory: Dict[str, Set[str]] = {}
        self.versions: Dict[str, int] = {}
        
        self._pending_writes: Dict[str, asyncio.Future] = {}
    
    async def _on_start(self):
        self.transport.register_handler(MessageType.CACHE_READ, self._handle_cache_read)
        self.transport.register_handler(MessageType.CACHE_WRITE, self._handle_cache_write)
        self.transport.register_handler(MessageType.CACHE_INVALIDATE, self._handle_cache_invalidate)
    
    async def _on_stop(self):
        for future in self._pending_writes.values():
            if not future.done():
                future.set_exception(asyncio.CancelledError())
    
    async def read(self, key: str) -> Optional[Any]:
        entry = self.cache.get(key)
        
        if entry and entry.state != CacheState.INVALID:
            self.metrics.record_cache_operation("read", "hit")
            return entry.value
        
        self.metrics.record_cache_operation("read", "miss")
        
        other_nodes = self.config.cluster.get_other_nodes(self.node_id)
        
        for node in other_nodes:
            message = Message(
                type=MessageType.CACHE_READ,
                sender_id=self.node_id,
                term=0,
                payload={"key": key},
            )
            
            response = await self.transport.send(node.host, node.port, message)
            
            if response and response.payload.get("found"):
                value = response.payload["value"]
                version = response.payload.get("version", 0)
                shared = response.payload.get("shared", True)
                
                new_state = CacheState.SHARED if shared else CacheState.EXCLUSIVE
                
                old_state = entry.state if entry else CacheState.INVALID
                self.metrics.record_state_transition(old_state.value, new_state.value)
                
                new_entry = CacheEntry(
                    key=key,
                    value=value,
                    state=new_state,
                    version=version,
                )
                
                evicted = self.cache.put(key, new_entry)
                if evicted and evicted.state == CacheState.MODIFIED:
                    await self._writeback(evicted)
                
                self.metrics.set_cache_size(self.cache.size())
                
                return value
        
        return None
    
    async def write(self, key: str, value: Any) -> bool:
        entry = self.cache.get(key)
        
        await self._broadcast_invalidate(key)
        
        version = self.versions.get(key, 0) + 1
        self.versions[key] = version
        
        old_state = entry.state if entry else CacheState.INVALID
        new_state = self.protocol.on_local_write(entry)
        
        self.metrics.record_state_transition(old_state.value, new_state.value)
        
        new_entry = CacheEntry(
            key=key,
            value=value,
            state=new_state,
            version=version,
        )
        
        evicted = self.cache.put(key, new_entry)
        if evicted and evicted.state == CacheState.MODIFIED:
            await self._writeback(evicted)
        
        self.metrics.record_cache_operation("write", "success")
        self.metrics.set_cache_size(self.cache.size())
        
        return True
    
    async def _broadcast_invalidate(self, key: str):
        other_nodes = self.config.cluster.get_other_nodes(self.node_id)
        
        message = Message(
            type=MessageType.CACHE_INVALIDATE,
            sender_id=self.node_id,
            term=0,
            payload={
                "key": key,
                "version": self.versions.get(key, 0),
            },
        )
        
        await self.transport.broadcast(other_nodes, message)
    
    async def _writeback(self, entry: CacheEntry):
        self.logger.debug(f"Writeback for key {entry.key}")
    
    async def _handle_cache_read(self, message: Message) -> Optional[Message]:
        key = message.payload["key"]
        entry = self.cache.get(key)
        
        if entry is None or entry.state == CacheState.INVALID:
            return Message(
                type=MessageType.CACHE_RESPONSE,
                sender_id=self.node_id,
                term=0,
                payload={"found": False},
            )
        
        value, new_state, needs_writeback = self.protocol.on_bus_read(entry)
        
        if new_state != entry.state:
            old_state = entry.state
            entry.state = new_state
            self.metrics.record_state_transition(old_state.value, new_state.value)
        
        if needs_writeback:
            await self._writeback(entry)
        
        return Message(
            type=MessageType.CACHE_RESPONSE,
            sender_id=self.node_id,
            term=0,
            payload={
                "found": True,
                "value": value,
                "version": entry.version,
                "shared": new_state == CacheState.SHARED,
            },
        )
    
    async def _handle_cache_write(self, message: Message) -> Optional[Message]:
        key = message.payload["key"]
        value = message.payload["value"]
        version = message.payload.get("version", 0)
        
        entry = self.cache.get(key)
        
        if entry:
            old_state = entry.state
            entry.state = CacheState.INVALID
            self.metrics.record_state_transition(old_state.value, CacheState.INVALID.value)
        
        new_entry = CacheEntry(
            key=key,
            value=value,
            state=CacheState.SHARED,
            version=version,
        )
        
        self.cache.put(key, new_entry)
        
        return Message(
            type=MessageType.CACHE_RESPONSE,
            sender_id=self.node_id,
            term=0,
            payload={"success": True},
        )
    
    async def _handle_cache_invalidate(self, message: Message) -> Optional[Message]:
        key = message.payload["key"]
        version = message.payload.get("version", 0)
        
        entry = self.cache.get(key)
        
        if entry:
            if version > entry.version:
                old_state = entry.state
                new_state = self.protocol.on_bus_write(entry)
                
                if old_state == CacheState.MODIFIED:
                    await self._writeback(entry)
                
                entry.state = new_state
                self.metrics.record_state_transition(old_state.value, new_state.value)
        
        return None
    
    def invalidate(self, key: str) -> bool:
        entry = self.cache.get(key)
        if entry:
            old_state = entry.state
            entry.state = CacheState.INVALID
            self.metrics.record_state_transition(old_state.value, CacheState.INVALID.value)
            return True
        return False
    
    def get_cache_info(self) -> Dict[str, Any]:
        entries = self.cache.get_all()
        
        state_counts = {state.value: 0 for state in CacheState}
        for entry in entries:
            state_counts[entry.state.value] += 1
        
        return {
            "size": self.cache.size(),
            "max_size": self.cache.max_size,
            "state_counts": state_counts,
        }
    
    def get_entry_info(self, key: str) -> Optional[Dict[str, Any]]:
        entry = self.cache.get(key)
        if entry:
            return {
                "key": entry.key,
                "state": entry.state.value,
                "version": entry.version,
                "last_access": entry.last_access,
            }
        return None
