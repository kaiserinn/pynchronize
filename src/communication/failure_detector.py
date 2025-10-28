import asyncio
import time
import logging
from dataclasses import dataclass, field
from typing import Dict, Set, Callable, Awaitable, Optional
from enum import Enum


class NodeStatus(Enum):
    ALIVE = "alive"
    SUSPECTED = "suspected"
    DEAD = "dead"


@dataclass
class NodeState:
    node_id: str
    status: NodeStatus = NodeStatus.ALIVE
    last_heartbeat: float = field(default_factory=time.time)
    missed_heartbeats: int = 0


class FailureDetector:
    def __init__(
        self,
        node_id: str,
        heartbeat_interval: float = 1.0,
        suspect_threshold: int = 3,
        dead_threshold: int = 5,
    ):
        self.node_id = node_id
        self.heartbeat_interval = heartbeat_interval
        self.suspect_threshold = suspect_threshold
        self.dead_threshold = dead_threshold
        
        self.nodes: Dict[str, NodeState] = {}
        self.on_status_change: Optional[Callable[[str, NodeStatus, NodeStatus], Awaitable[None]]] = None
        
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self.logger = logging.getLogger(f"failure_detector.{node_id}")
    
    def register_node(self, node_id: str):
        if node_id != self.node_id and node_id not in self.nodes:
            self.nodes[node_id] = NodeState(node_id=node_id)
    
    def unregister_node(self, node_id: str):
        self.nodes.pop(node_id, None)
    
    def record_heartbeat(self, node_id: str):
        if node_id in self.nodes:
            node = self.nodes[node_id]
            old_status = node.status
            node.last_heartbeat = time.time()
            node.missed_heartbeats = 0
            
            if node.status != NodeStatus.ALIVE:
                node.status = NodeStatus.ALIVE
                if self.on_status_change:
                    asyncio.create_task(
                        self.on_status_change(node_id, old_status, NodeStatus.ALIVE)
                    )
    
    def get_alive_nodes(self) -> Set[str]:
        return {
            node_id for node_id, state in self.nodes.items()
            if state.status == NodeStatus.ALIVE
        }
    
    def get_node_status(self, node_id: str) -> Optional[NodeStatus]:
        if node_id in self.nodes:
            return self.nodes[node_id].status
        return None
    
    def is_alive(self, node_id: str) -> bool:
        return self.get_node_status(node_id) == NodeStatus.ALIVE
    
    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._check_loop())
        self.logger.info("Failure detector started")
    
    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
    
    async def _check_loop(self):
        while self._running:
            await asyncio.sleep(self.heartbeat_interval)
            await self._check_nodes()
    
    async def _check_nodes(self):
        current_time = time.time()
        
        for node_id, state in self.nodes.items():
            elapsed = current_time - state.last_heartbeat
            missed = int(elapsed / self.heartbeat_interval)
            
            if missed > state.missed_heartbeats:
                state.missed_heartbeats = missed
                old_status = state.status
                
                if missed >= self.dead_threshold:
                    if state.status != NodeStatus.DEAD:
                        state.status = NodeStatus.DEAD
                        self.logger.warning(f"Node {node_id} is DEAD")
                        if self.on_status_change:
                            await self.on_status_change(node_id, old_status, NodeStatus.DEAD)
                
                elif missed >= self.suspect_threshold:
                    if state.status != NodeStatus.SUSPECTED:
                        state.status = NodeStatus.SUSPECTED
                        self.logger.warning(f"Node {node_id} is SUSPECTED")
                        if self.on_status_change:
                            await self.on_status_change(node_id, old_status, NodeStatus.SUSPECTED)


class PhiAccrualFailureDetector:
    def __init__(
        self,
        node_id: str,
        threshold: float = 8.0,
        min_samples: int = 10,
        max_samples: int = 1000,
    ):
        self.node_id = node_id
        self.threshold = threshold
        self.min_samples = min_samples
        self.max_samples = max_samples
        
        self.nodes: Dict[str, NodeState] = {}
        self.heartbeat_history: Dict[str, list] = {}
        self.on_status_change: Optional[Callable[[str, NodeStatus, NodeStatus], Awaitable[None]]] = None
        
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self.logger = logging.getLogger(f"phi_detector.{node_id}")
    
    def register_node(self, node_id: str):
        if node_id != self.node_id and node_id not in self.nodes:
            self.nodes[node_id] = NodeState(node_id=node_id)
            self.heartbeat_history[node_id] = []
    
    def record_heartbeat(self, node_id: str):
        if node_id not in self.nodes:
            return
        
        current_time = time.time()
        node = self.nodes[node_id]
        
        if node.last_heartbeat > 0:
            interval = current_time - node.last_heartbeat
            history = self.heartbeat_history[node_id]
            history.append(interval)
            
            if len(history) > self.max_samples:
                history.pop(0)
        
        node.last_heartbeat = current_time
        
        old_status = node.status
        if node.status != NodeStatus.ALIVE:
            node.status = NodeStatus.ALIVE
            if self.on_status_change:
                asyncio.create_task(
                    self.on_status_change(node_id, old_status, NodeStatus.ALIVE)
                )
    
    def _calculate_phi(self, node_id: str) -> float:
        if node_id not in self.nodes:
            return float("inf")
        
        history = self.heartbeat_history.get(node_id, [])
        if len(history) < self.min_samples:
            return 0.0
        
        import math
        
        mean = sum(history) / len(history)
        variance = sum((x - mean) ** 2 for x in history) / len(history)
        std_dev = math.sqrt(variance) if variance > 0 else 0.1
        
        current_time = time.time()
        last_hb = self.nodes[node_id].last_heartbeat
        elapsed = current_time - last_hb
        
        if elapsed <= mean:
            return 0.0
        
        y = (elapsed - mean) / std_dev
        probability = 0.5 * (1 + math.erf(y / math.sqrt(2)))
        
        if probability >= 1.0:
            return float("inf")
        
        phi = -math.log10(1 - probability)
        return phi
    
    def is_alive(self, node_id: str) -> bool:
        phi = self._calculate_phi(node_id)
        return phi < self.threshold
    
    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._check_loop())
    
    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
    
    async def _check_loop(self):
        while self._running:
            await asyncio.sleep(1.0)
            await self._check_nodes()
    
    async def _check_nodes(self):
        for node_id, state in self.nodes.items():
            phi = self._calculate_phi(node_id)
            old_status = state.status
            
            if phi >= self.threshold:
                if state.status != NodeStatus.DEAD:
                    state.status = NodeStatus.DEAD
                    self.logger.warning(f"Node {node_id} is DEAD (phi={phi:.2f})")
                    if self.on_status_change:
                        await self.on_status_change(node_id, old_status, NodeStatus.DEAD)
            elif phi >= self.threshold * 0.7:
                if state.status != NodeStatus.SUSPECTED:
                    state.status = NodeStatus.SUSPECTED
                    self.logger.warning(f"Node {node_id} is SUSPECTED (phi={phi:.2f})")
                    if self.on_status_change:
                        await self.on_status_change(node_id, old_status, NodeStatus.SUSPECTED)
