import asyncio
import hashlib
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, Set, Callable, Awaitable
from collections import defaultdict
from bisect import bisect_left, insort

from src.nodes.base_node import BaseNode
from src.utils.config import Config
from src.communication.message_passing import Message, MessageType


class ConsistentHash:
    def __init__(self, nodes: List[str] = None, replicas: int = 100):
        self.replicas = replicas
        self.ring: List[int] = []
        self.node_map: Dict[int, str] = {}
        self.nodes: Set[str] = set()
        
        if nodes:
            for node in nodes:
                self.add_node(node)
    
    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    
    def add_node(self, node: str):
        if node in self.nodes:
            return
        
        self.nodes.add(node)
        for i in range(self.replicas):
            virtual_key = f"{node}:{i}"
            hash_val = self._hash(virtual_key)
            insort(self.ring, hash_val)
            self.node_map[hash_val] = node
    
    def remove_node(self, node: str):
        if node not in self.nodes:
            return
        
        self.nodes.remove(node)
        for i in range(self.replicas):
            virtual_key = f"{node}:{i}"
            hash_val = self._hash(virtual_key)
            self.ring.remove(hash_val)
            del self.node_map[hash_val]
    
    def get_node(self, key: str) -> Optional[str]:
        if not self.ring:
            return None
        
        hash_val = self._hash(key)
        idx = bisect_left(self.ring, hash_val)
        
        if idx == len(self.ring):
            idx = 0
        
        return self.node_map[self.ring[idx]]
    
    def get_nodes_for_key(self, key: str, count: int = 1) -> List[str]:
        if not self.ring:
            return []
        
        result = []
        seen = set()
        
        hash_val = self._hash(key)
        idx = bisect_left(self.ring, hash_val)
        
        while len(result) < count and len(seen) < len(self.nodes):
            if idx >= len(self.ring):
                idx = 0
            
            node = self.node_map[self.ring[idx]]
            if node not in seen:
                seen.add(node)
                result.append(node)
            
            idx += 1
        
        return result


@dataclass
class QueueMessage:
    id: str
    queue_name: str
    payload: Dict[str, Any]
    created_at: float = field(default_factory=time.time)
    attempts: int = 0
    last_attempt: float = 0.0
    acked: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "QueueMessage":
        return cls(**data)


class MessageStore:
    def __init__(self, persistence_path: str):
        self.persistence_path = persistence_path
        self.messages: Dict[str, QueueMessage] = {}
        self.queues: Dict[str, List[str]] = defaultdict(list)
        self._ensure_path()
        self._load()
    
    def _ensure_path(self):
        os.makedirs(self.persistence_path, exist_ok=True)
    
    def _get_file_path(self, queue_name: str) -> str:
        safe_name = hashlib.md5(queue_name.encode()).hexdigest()
        return os.path.join(self.persistence_path, f"{safe_name}.json")
    
    def _load(self):
        if not os.path.exists(self.persistence_path):
            return
        
        for filename in os.listdir(self.persistence_path):
            if not filename.endswith(".json"):
                continue
            
            filepath = os.path.join(self.persistence_path, filename)
            try:
                with open(filepath, "r") as f:
                    data = json.load(f)
                    for msg_data in data.get("messages", []):
                        msg = QueueMessage.from_dict(msg_data)
                        self.messages[msg.id] = msg
                        if not msg.acked:
                            self.queues[msg.queue_name].append(msg.id)
            except Exception:
                pass
    
    def _save_queue(self, queue_name: str):
        filepath = self._get_file_path(queue_name)
        messages = [
            self.messages[mid].to_dict()
            for mid in self.queues[queue_name]
            if mid in self.messages
        ]
        
        try:
            with open(filepath, "w") as f:
                json.dump({"messages": messages}, f)
        except Exception:
            pass
    
    def add(self, message: QueueMessage):
        self.messages[message.id] = message
        self.queues[message.queue_name].append(message.id)
        self._save_queue(message.queue_name)
    
    def get(self, message_id: str) -> Optional[QueueMessage]:
        return self.messages.get(message_id)
    
    def get_pending(self, queue_name: str, limit: int = 10) -> List[QueueMessage]:
        result = []
        for msg_id in self.queues.get(queue_name, []):
            msg = self.messages.get(msg_id)
            if msg and not msg.acked:
                result.append(msg)
                if len(result) >= limit:
                    break
        return result
    
    def ack(self, message_id: str) -> bool:
        msg = self.messages.get(message_id)
        if not msg:
            return False
        
        msg.acked = True
        self._save_queue(msg.queue_name)
        return True
    
    def nack(self, message_id: str):
        msg = self.messages.get(message_id)
        if msg:
            msg.attempts += 1
            msg.last_attempt = time.time()
            self._save_queue(msg.queue_name)
    
    def get_queue_size(self, queue_name: str) -> int:
        return len([
            mid for mid in self.queues.get(queue_name, [])
            if mid in self.messages and not self.messages[mid].acked
        ])


MessageHandler = Callable[[QueueMessage], Awaitable[bool]]


class QueueNode(BaseNode):
    def __init__(self, config: Config, replication_factor: int = 2):
        super().__init__(config)
        
        self.replication_factor = replication_factor
        self.consistent_hash = ConsistentHash()
        self.store = MessageStore(config.queue.persistence_path)
        
        self.consumers: Dict[str, List[MessageHandler]] = defaultdict(list)
        self.in_flight: Dict[str, float] = {}
        self.ack_timeout = 30.0
        
        self._consumer_task: Optional[asyncio.Task] = None
        self._redelivery_task: Optional[asyncio.Task] = None
    
    async def _on_start(self):
        for node in self.config.cluster.nodes:
            self.consistent_hash.add_node(node.node_id)
        
        self.transport.register_handler(MessageType.QUEUE_PUBLISH, self._handle_publish)
        self.transport.register_handler(MessageType.QUEUE_CONSUME, self._handle_consume)
        self.transport.register_handler(MessageType.QUEUE_ACK, self._handle_ack)
        
        self.failure_detector.on_status_change = self._on_node_status_change
        
        self._consumer_task = asyncio.create_task(self._consumer_loop())
        self._redelivery_task = asyncio.create_task(self._redelivery_loop())
    
    async def _on_stop(self):
        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
        
        if self._redelivery_task:
            self._redelivery_task.cancel()
            try:
                await self._redelivery_task
            except asyncio.CancelledError:
                pass
    
    async def _on_node_status_change(self, node_id: str, old_status, new_status):
        from src.communication.failure_detector import NodeStatus
        
        if new_status == NodeStatus.DEAD:
            self.consistent_hash.remove_node(node_id)
            self.logger.warning(f"Node {node_id} removed from consistent hash ring")
        elif new_status == NodeStatus.ALIVE and old_status == NodeStatus.DEAD:
            self.consistent_hash.add_node(node_id)
            self.logger.info(f"Node {node_id} added back to consistent hash ring")
    
    async def publish(
        self,
        queue_name: str,
        payload: Dict[str, Any],
        message_id: Optional[str] = None,
    ) -> str:
        if message_id is None:
            message_id = str(uuid.uuid4())
        
        target_nodes = self.consistent_hash.get_nodes_for_key(
            message_id, 
            min(self.replication_factor, len(self.consistent_hash.nodes))
        )
        
        message = QueueMessage(
            id=message_id,
            queue_name=queue_name,
            payload=payload,
        )
        
        if self.node_id in target_nodes:
            self.store.add(message)
            self.metrics.record_queue_message("publish")
            self.metrics.set_queue_size(queue_name, self.store.get_queue_size(queue_name))
        
        for target_id in target_nodes:
            if target_id == self.node_id:
                continue
            
            target_node = self.config.cluster.get_node(target_id)
            if not target_node:
                continue
            
            msg = Message(
                type=MessageType.QUEUE_PUBLISH,
                sender_id=self.node_id,
                term=0,
                payload={
                    "queue_name": queue_name,
                    "message": message.to_dict(),
                },
            )
            
            await self.transport.send(target_node.host, target_node.port, msg)
        
        return message_id
    
    async def _handle_publish(self, message: Message) -> Optional[Message]:
        payload = message.payload
        queue_msg = QueueMessage.from_dict(payload["message"])
        
        if not self.store.get(queue_msg.id):
            self.store.add(queue_msg)
            self.metrics.record_queue_message("replicate")
            self.metrics.set_queue_size(
                queue_msg.queue_name, 
                self.store.get_queue_size(queue_msg.queue_name)
            )
        
        return None
    
    def subscribe(self, queue_name: str, handler: MessageHandler):
        self.consumers[queue_name].append(handler)
    
    async def _consumer_loop(self):
        while self._running:
            await asyncio.sleep(0.1)
            
            for queue_name, handlers in self.consumers.items():
                if not handlers:
                    continue
                
                messages = self.store.get_pending(queue_name, limit=10)
                
                for msg in messages:
                    if msg.id in self.in_flight:
                        continue
                    
                    self.in_flight[msg.id] = time.time()
                    
                    for handler in handlers:
                        try:
                            start_time = time.time()
                            success = await handler(msg)
                            latency = time.time() - start_time
                            
                            self.metrics.record_message_latency(latency)
                            
                            if success:
                                await self.ack(msg.id)
                                break
                        except Exception as e:
                            self.logger.error(f"Handler error: {e}")
                    else:
                        self.in_flight.pop(msg.id, None)
    
    async def _redelivery_loop(self):
        while self._running:
            await asyncio.sleep(5.0)
            
            current_time = time.time()
            for msg_id, start_time in list(self.in_flight.items()):
                if current_time - start_time > self.ack_timeout:
                    self.logger.warning(f"Message {msg_id} timed out, marking for redelivery")
                    self.store.nack(msg_id)
                    self.in_flight.pop(msg_id, None)
    
    async def consume(
        self,
        queue_name: str,
        limit: int = 1,
    ) -> List[QueueMessage]:
        responsible_node = self.consistent_hash.get_node(queue_name)
        
        if responsible_node == self.node_id:
            messages = self.store.get_pending(queue_name, limit)
            for msg in messages:
                msg.attempts += 1
                msg.last_attempt = time.time()
                self.in_flight[msg.id] = time.time()
            return messages
        
        target_node = self.config.cluster.get_node(responsible_node)
        if not target_node:
            return []
        
        msg = Message(
            type=MessageType.QUEUE_CONSUME,
            sender_id=self.node_id,
            term=0,
            payload={"queue_name": queue_name, "limit": limit},
        )
        
        response = await self.transport.send(target_node.host, target_node.port, msg)
        if response and "messages" in response.payload:
            return [QueueMessage.from_dict(m) for m in response.payload["messages"]]
        
        return []
    
    async def _handle_consume(self, message: Message) -> Optional[Message]:
        payload = message.payload
        queue_name = payload["queue_name"]
        limit = payload.get("limit", 1)
        
        messages = self.store.get_pending(queue_name, limit)
        for msg in messages:
            msg.attempts += 1
            msg.last_attempt = time.time()
            self.in_flight[msg.id] = time.time()
        
        self.metrics.record_queue_message("consume")
        
        return Message(
            type=MessageType.CACHE_RESPONSE,
            sender_id=self.node_id,
            term=0,
            payload={"messages": [m.to_dict() for m in messages]},
        )
    
    async def ack(self, message_id: str) -> bool:
        msg = self.store.get(message_id)
        if not msg:
            return False
        
        responsible_nodes = self.consistent_hash.get_nodes_for_key(
            message_id,
            min(self.replication_factor, len(self.consistent_hash.nodes))
        )
        
        if self.node_id in responsible_nodes:
            self.store.ack(message_id)
            self.in_flight.pop(message_id, None)
            self.metrics.record_queue_message("ack")
            self.metrics.set_queue_size(
                msg.queue_name,
                self.store.get_queue_size(msg.queue_name)
            )
        
        for node_id in responsible_nodes:
            if node_id == self.node_id:
                continue
            
            target_node = self.config.cluster.get_node(node_id)
            if not target_node:
                continue
            
            ack_msg = Message(
                type=MessageType.QUEUE_ACK,
                sender_id=self.node_id,
                term=0,
                payload={"message_id": message_id},
            )
            
            await self.transport.send(target_node.host, target_node.port, ack_msg)
        
        return True
    
    async def _handle_ack(self, message: Message) -> Optional[Message]:
        message_id = message.payload["message_id"]
        self.store.ack(message_id)
        self.in_flight.pop(message_id, None)
        self.metrics.record_queue_message("ack_replicate")
        return None
    
    def get_queue_info(self, queue_name: str) -> Dict[str, Any]:
        return {
            "queue_name": queue_name,
            "size": self.store.get_queue_size(queue_name),
            "in_flight": len([
                mid for mid in self.in_flight
                if self.store.get(mid) and self.store.get(mid).queue_name == queue_name
            ]),
        }
