import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Optional

from src.utils.config import Config
from src.utils.metrics import MetricsCollector
from src.communication.message_passing import MessageTransport, Message, MessageType
from src.communication.failure_detector import FailureDetector


class BaseNode(ABC):
    def __init__(self, config: Config):
        self.config = config
        self.node_id = config.node.node_id
        
        self.transport = MessageTransport(
            node_id=self.node_id,
            host=config.node.host,
            port=config.node.port,
        )
        
        self.failure_detector = FailureDetector(
            node_id=self.node_id,
            heartbeat_interval=config.raft.heartbeat_interval / 1000.0,
        )
        
        self.metrics = MetricsCollector(self.node_id)
        
        self.logger = logging.getLogger(f"node.{self.node_id}")
        self._running = False
        self._heartbeat_task: Optional[asyncio.Task] = None
    
    async def start(self):
        self._running = True
        
        await self.transport.start()
        
        for node in self.config.cluster.get_other_nodes(self.node_id):
            self.failure_detector.register_node(node.node_id)
        
        await self.failure_detector.start()
        
        self._register_handlers()
        
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        
        await self._on_start()
        
        self.logger.info(f"Node {self.node_id} started")
    
    async def stop(self):
        self._running = False
        
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        
        await self._on_stop()
        await self.failure_detector.stop()
        await self.transport.stop()
        
        self.logger.info(f"Node {self.node_id} stopped")
    
    def _register_handlers(self):
        self.transport.register_handler(
            MessageType.HEARTBEAT, 
            self._handle_heartbeat
        )
    
    async def _handle_heartbeat(self, message: Message) -> Optional[Message]:
        self.failure_detector.record_heartbeat(message.sender_id)
        return Message(
            type=MessageType.HEARTBEAT_RESPONSE,
            sender_id=self.node_id,
            term=message.term,
            payload={"status": "alive"},
        )
    
    async def _heartbeat_loop(self):
        interval = self.config.raft.heartbeat_interval / 1000.0
        
        while self._running:
            await asyncio.sleep(interval)
            await self._send_heartbeats()
    
    async def _send_heartbeats(self):
        message = Message(
            type=MessageType.HEARTBEAT,
            sender_id=self.node_id,
            term=0,
            payload={},
        )
        
        other_nodes = self.config.cluster.get_other_nodes(self.node_id)
        await self.transport.broadcast(other_nodes, message)
    
    @abstractmethod
    async def _on_start(self):
        pass
    
    @abstractmethod
    async def _on_stop(self):
        pass
