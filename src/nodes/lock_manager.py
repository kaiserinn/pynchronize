import asyncio
import time
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set
from enum import Enum
from collections import defaultdict

from src.nodes.base_node import BaseNode
from src.utils.config import Config
from src.consensus.raft import RaftNode
from src.communication.message_passing import Message, MessageType


class LockType(Enum):
    SHARED = "shared"
    EXCLUSIVE = "exclusive"


class LockStatus(Enum):
    ACQUIRED = "acquired"
    WAITING = "waiting"
    RELEASED = "released"
    DENIED = "denied"
    TIMEOUT = "timeout"
    DEADLOCK = "deadlock"


@dataclass
class LockRequest:
    client_id: str
    resource_id: str
    lock_type: LockType
    timestamp: float = field(default_factory=time.time)


@dataclass
class Lock:
    resource_id: str
    lock_type: LockType
    holders: Set[str] = field(default_factory=set)
    waiting: List[LockRequest] = field(default_factory=list)


class DeadlockDetector:
    def __init__(self):
        self.wait_for_graph: Dict[str, Set[str]] = defaultdict(set)
    
    def add_wait(self, waiter: str, holders: Set[str]):
        for holder in holders:
            if holder != waiter:
                self.wait_for_graph[waiter].add(holder)
    
    def remove_wait(self, waiter: str):
        self.wait_for_graph.pop(waiter, None)
    
    def remove_holder(self, holder: str):
        for waiter in list(self.wait_for_graph.keys()):
            self.wait_for_graph[waiter].discard(holder)
            if not self.wait_for_graph[waiter]:
                del self.wait_for_graph[waiter]
    
    def detect_cycle(self, start: str) -> Optional[List[str]]:
        visited = set()
        path = []
        
        def dfs(node: str) -> Optional[List[str]]:
            if node in visited:
                if node in path:
                    cycle_start = path.index(node)
                    return path[cycle_start:] + [node]
                return None
            
            visited.add(node)
            path.append(node)
            
            for neighbor in self.wait_for_graph.get(node, set()):
                result = dfs(neighbor)
                if result:
                    return result
            
            path.pop()
            return None
        
        return dfs(start)
    
    def has_deadlock(self) -> Optional[List[str]]:
        for node in self.wait_for_graph:
            cycle = self.detect_cycle(node)
            if cycle:
                return cycle
        return None


class LockManager(BaseNode):
    def __init__(self, config: Config):
        super().__init__(config)
        
        self.raft = RaftNode(
            config=config,
            transport=self.transport,
            metrics=self.metrics,
        )
        
        self.locks: Dict[str, Lock] = {}
        self.deadlock_detector = DeadlockDetector()
        self.pending_requests: Dict[str, asyncio.Future] = {}
        
        self.raft.command_handler = self._apply_command
        self.raft.on_become_leader = self._on_become_leader
        self.raft.on_lose_leadership = self._on_lose_leadership
    
    async def _on_start(self):
        await self.raft.start()
        
        self.transport.register_handler(MessageType.LOCK_REQUEST, self._handle_lock_request)
        self.transport.register_handler(MessageType.LOCK_RELEASE, self._handle_lock_release)
    
    async def _on_stop(self):
        await self.raft.stop()
        
        for future in self.pending_requests.values():
            if not future.done():
                future.set_exception(asyncio.CancelledError())
    
    async def _on_become_leader(self):
        self.logger.info("Became lock manager leader")
    
    async def _on_lose_leadership(self):
        self.logger.info("Lost lock manager leadership")
        
        for future in self.pending_requests.values():
            if not future.done():
                future.set_result(LockStatus.DENIED)
        self.pending_requests.clear()
    
    async def _apply_command(self, command: Dict[str, Any]) -> Any:
        action = command.get("action")
        
        if action == "acquire":
            return await self._apply_acquire(command)
        elif action == "release":
            return await self._apply_release(command)
        
        return None
    
    async def _apply_acquire(self, command: Dict[str, Any]) -> LockStatus:
        client_id = command["client_id"]
        resource_id = command["resource_id"]
        lock_type = LockType(command["lock_type"])
        request_id = command.get("request_id")
        
        if resource_id not in self.locks:
            self.locks[resource_id] = Lock(resource_id=resource_id, lock_type=lock_type)
        
        lock = self.locks[resource_id]
        
        can_acquire = False
        
        if not lock.holders:
            can_acquire = True
            lock.lock_type = lock_type
        elif lock.lock_type == LockType.SHARED and lock_type == LockType.SHARED:
            can_acquire = True
        elif client_id in lock.holders:
            can_acquire = True
        
        if can_acquire:
            lock.holders.add(client_id)
            self.deadlock_detector.remove_wait(client_id)
            
            self.metrics.record_lock_operation("acquire", lock_type.value)
            self.metrics.set_active_locks(lock_type.value, len([l for l in self.locks.values() if l.lock_type == lock_type]))
            
            if request_id and request_id in self.pending_requests:
                future = self.pending_requests.pop(request_id)
                if not future.done():
                    future.set_result(LockStatus.ACQUIRED)
            
            return LockStatus.ACQUIRED
        
        self.deadlock_detector.add_wait(client_id, lock.holders)
        
        cycle = self.deadlock_detector.detect_cycle(client_id)
        if cycle:
            self.deadlock_detector.remove_wait(client_id)
            self.metrics.record_deadlock()
            
            if request_id and request_id in self.pending_requests:
                future = self.pending_requests.pop(request_id)
                if not future.done():
                    future.set_result(LockStatus.DEADLOCK)
            
            return LockStatus.DEADLOCK
        
        request = LockRequest(
            client_id=client_id,
            resource_id=resource_id,
            lock_type=lock_type,
        )
        lock.waiting.append(request)
        
        return LockStatus.WAITING
    
    async def _apply_release(self, command: Dict[str, Any]) -> LockStatus:
        client_id = command["client_id"]
        resource_id = command["resource_id"]
        
        if resource_id not in self.locks:
            return LockStatus.RELEASED
        
        lock = self.locks[resource_id]
        
        if client_id not in lock.holders:
            return LockStatus.RELEASED
        
        lock.holders.remove(client_id)
        self.deadlock_detector.remove_holder(client_id)
        
        self.metrics.record_lock_operation("release", lock.lock_type.value)
        
        if not lock.holders and lock.waiting:
            await self._process_waiting(lock)
        
        if not lock.holders and not lock.waiting:
            del self.locks[resource_id]
        
        return LockStatus.RELEASED
    
    async def _process_waiting(self, lock: Lock):
        if not lock.waiting:
            return
        
        next_request = lock.waiting[0]
        
        if next_request.lock_type == LockType.EXCLUSIVE:
            lock.waiting.pop(0)
            lock.lock_type = LockType.EXCLUSIVE
            lock.holders.add(next_request.client_id)
            self.deadlock_detector.remove_wait(next_request.client_id)
        else:
            shared_requests = []
            for req in lock.waiting:
                if req.lock_type == LockType.SHARED:
                    shared_requests.append(req)
                else:
                    break
            
            for req in shared_requests:
                lock.waiting.remove(req)
                lock.holders.add(req.client_id)
                self.deadlock_detector.remove_wait(req.client_id)
            
            lock.lock_type = LockType.SHARED
    
    async def acquire_lock(
        self,
        client_id: str,
        resource_id: str,
        lock_type: LockType,
        timeout: float = 30.0,
    ) -> LockStatus:
        if not self.raft.is_leader():
            leader = self.raft.get_leader()
            if leader:
                return await self._forward_to_leader(leader, "acquire", client_id, resource_id, lock_type)
            return LockStatus.DENIED
        
        request_id = f"{client_id}:{resource_id}:{time.time()}"
        
        future: asyncio.Future = asyncio.Future()
        self.pending_requests[request_id] = future
        
        command = {
            "action": "acquire",
            "client_id": client_id,
            "resource_id": resource_id,
            "lock_type": lock_type.value,
            "request_id": request_id,
        }
        
        self.metrics.start_timer(request_id)
        
        success = await self.raft.propose(command)
        if not success:
            self.pending_requests.pop(request_id, None)
            return LockStatus.DENIED
        
        try:
            result = await asyncio.wait_for(future, timeout=timeout)
            elapsed = self.metrics.stop_timer(request_id)
            if elapsed:
                self.metrics.record_lock_wait(lock_type.value, elapsed)
            return result
        except asyncio.TimeoutError:
            self.pending_requests.pop(request_id, None)
            return LockStatus.TIMEOUT
    
    async def release_lock(self, client_id: str, resource_id: str) -> LockStatus:
        if not self.raft.is_leader():
            leader = self.raft.get_leader()
            if leader:
                return await self._forward_to_leader(leader, "release", client_id, resource_id, None)
            return LockStatus.DENIED
        
        command = {
            "action": "release",
            "client_id": client_id,
            "resource_id": resource_id,
        }
        
        success = await self.raft.propose(command)
        if success:
            return LockStatus.RELEASED
        return LockStatus.DENIED
    
    async def _forward_to_leader(
        self,
        leader_id: str,
        action: str,
        client_id: str,
        resource_id: str,
        lock_type: Optional[LockType],
    ) -> LockStatus:
        leader_node = self.config.cluster.get_node(leader_id)
        if not leader_node:
            return LockStatus.DENIED
        
        msg_type = MessageType.LOCK_REQUEST if action == "acquire" else MessageType.LOCK_RELEASE
        
        message = Message(
            type=msg_type,
            sender_id=self.node_id,
            term=self.raft.get_term(),
            payload={
                "client_id": client_id,
                "resource_id": resource_id,
                "lock_type": lock_type.value if lock_type else None,
            },
        )
        
        response = await self.transport.send(leader_node.host, leader_node.port, message)
        if response and "status" in response.payload:
            return LockStatus(response.payload["status"])
        return LockStatus.DENIED
    
    async def _handle_lock_request(self, message: Message) -> Optional[Message]:
        payload = message.payload
        
        status = await self.acquire_lock(
            client_id=payload["client_id"],
            resource_id=payload["resource_id"],
            lock_type=LockType(payload["lock_type"]),
        )
        
        return Message(
            type=MessageType.LOCK_RESPONSE,
            sender_id=self.node_id,
            term=self.raft.get_term(),
            payload={"status": status.value},
        )
    
    async def _handle_lock_release(self, message: Message) -> Optional[Message]:
        payload = message.payload
        
        status = await self.release_lock(
            client_id=payload["client_id"],
            resource_id=payload["resource_id"],
        )
        
        return Message(
            type=MessageType.LOCK_RESPONSE,
            sender_id=self.node_id,
            term=self.raft.get_term(),
            payload={"status": status.value},
        )
    
    def get_lock_info(self, resource_id: str) -> Optional[Dict[str, Any]]:
        if resource_id not in self.locks:
            return None
        
        lock = self.locks[resource_id]
        return {
            "resource_id": resource_id,
            "lock_type": lock.lock_type.value,
            "holders": list(lock.holders),
            "waiting_count": len(lock.waiting),
        }
    
    def get_all_locks(self) -> List[Dict[str, Any]]:
        return [self.get_lock_info(rid) for rid in self.locks]
