import asyncio
import random
import logging
import json
import os
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, Callable, Awaitable
from enum import Enum

from src.utils.config import Config, NodeConfig
from src.utils.metrics import MetricsCollector
from src.communication.message_passing import MessageTransport, Message, MessageType


class RaftState(Enum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


@dataclass
class LogEntry:
    term: int
    index: int
    command: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        return {"term": self.term, "index": self.index, "command": self.command}
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LogEntry":
        return cls(term=data["term"], index=data["index"], command=data["command"])


@dataclass
class PersistentState:
    current_term: int = 0
    voted_for: Optional[str] = None
    log: List[LogEntry] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "log": [e.to_dict() for e in self.log],
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PersistentState":
        return cls(
            current_term=data["current_term"],
            voted_for=data["voted_for"],
            log=[LogEntry.from_dict(e) for e in data["log"]],
        )


@dataclass
class VolatileState:
    commit_index: int = 0
    last_applied: int = 0


@dataclass
class LeaderState:
    next_index: Dict[str, int] = field(default_factory=dict)
    match_index: Dict[str, int] = field(default_factory=dict)


CommandHandler = Callable[[Dict[str, Any]], Awaitable[Any]]


class RaftNode:
    def __init__(
        self,
        config: Config,
        transport: MessageTransport,
        metrics: MetricsCollector,
        state_path: Optional[str] = None,
    ):
        self.config = config
        self.node_id = config.node.node_id
        self.transport = transport
        self.metrics = metrics
        self.state_path = state_path or f"/tmp/raft_{self.node_id}.json"
        
        self.state = RaftState.FOLLOWER
        self.persistent = PersistentState()
        self.volatile = VolatileState()
        self.leader_state: Optional[LeaderState] = None
        
        self.current_leader: Optional[str] = None
        self.votes_received: Dict[str, bool] = {}
        
        self.command_handler: Optional[CommandHandler] = None
        self.on_become_leader: Optional[Callable[[], Awaitable[None]]] = None
        self.on_lose_leadership: Optional[Callable[[], Awaitable[None]]] = None
        
        self._running = False
        self._election_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._last_heartbeat = 0.0
        
        self.logger = logging.getLogger(f"raft.{self.node_id}")
        
        self._load_state()
    
    def _load_state(self):
        if os.path.exists(self.state_path):
            try:
                with open(self.state_path, "r") as f:
                    data = json.load(f)
                    self.persistent = PersistentState.from_dict(data)
            except Exception as e:
                self.logger.error(f"Failed to load state: {e}")
    
    def _save_state(self):
        try:
            with open(self.state_path, "w") as f:
                json.dump(self.persistent.to_dict(), f)
        except Exception as e:
            self.logger.error(f"Failed to save state: {e}")
    
    def _get_election_timeout(self) -> float:
        return random.uniform(
            self.config.raft.election_timeout_min / 1000.0,
            self.config.raft.election_timeout_max / 1000.0,
        )
    
    def _get_other_nodes(self) -> List[NodeConfig]:
        return self.config.cluster.get_other_nodes(self.node_id)
    
    def _get_majority(self) -> int:
        return (len(self.config.cluster.nodes) // 2) + 1
    
    def _get_last_log_index(self) -> int:
        if self.persistent.log:
            return self.persistent.log[-1].index
        return 0
    
    def _get_last_log_term(self) -> int:
        if self.persistent.log:
            return self.persistent.log[-1].term
        return 0
    
    def _get_log_entry(self, index: int) -> Optional[LogEntry]:
        if index <= 0 or index > len(self.persistent.log):
            return None
        return self.persistent.log[index - 1]
    
    async def start(self):
        self._running = True
        self._last_heartbeat = asyncio.get_event_loop().time()
        
        self.transport.register_handler(MessageType.REQUEST_VOTE, self._handle_request_vote)
        self.transport.register_handler(MessageType.APPEND_ENTRIES, self._handle_append_entries)
        
        self._election_task = asyncio.create_task(self._election_loop())
        
        self.metrics.set_state(self.state.value)
        self.metrics.set_term(self.persistent.current_term)
        
        self.logger.info(f"Raft node started as {self.state.name}")
    
    async def stop(self):
        self._running = False
        
        if self._election_task:
            self._election_task.cancel()
            try:
                await self._election_task
            except asyncio.CancelledError:
                pass
        
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        
        self._save_state()
    
    async def _election_loop(self):
        while self._running:
            timeout = self._get_election_timeout()
            await asyncio.sleep(timeout)
            
            if self.state == RaftState.LEADER:
                continue
            
            current_time = asyncio.get_event_loop().time()
            elapsed = current_time - self._last_heartbeat
            
            if elapsed >= timeout:
                await self._start_election()
    
    async def _start_election(self):
        self.state = RaftState.CANDIDATE
        self.persistent.current_term += 1
        self.persistent.voted_for = self.node_id
        self.votes_received = {self.node_id: True}
        self._save_state()
        
        self.metrics.set_state(self.state.value)
        self.metrics.set_term(self.persistent.current_term)
        
        self.logger.info(f"Starting election for term {self.persistent.current_term}")
        
        message = Message(
            type=MessageType.REQUEST_VOTE,
            sender_id=self.node_id,
            term=self.persistent.current_term,
            payload={
                "candidate_id": self.node_id,
                "last_log_index": self._get_last_log_index(),
                "last_log_term": self._get_last_log_term(),
            },
        )
        
        other_nodes = self._get_other_nodes()
        responses = await self.transport.broadcast(other_nodes, message)
        
        for node_id, response in responses.items():
            if response and response.payload.get("vote_granted"):
                self.votes_received[node_id] = True
        
        if len(self.votes_received) >= self._get_majority():
            if self.state == RaftState.CANDIDATE:
                await self._become_leader()
        else:
            self.metrics.record_election("lost")
    
    async def _become_leader(self):
        self.state = RaftState.LEADER
        self.current_leader = self.node_id
        
        self.leader_state = LeaderState()
        last_log_index = self._get_last_log_index()
        
        for node in self._get_other_nodes():
            self.leader_state.next_index[node.node_id] = last_log_index + 1
            self.leader_state.match_index[node.node_id] = 0
        
        self.metrics.set_state(self.state.value)
        self.metrics.record_election("won")
        
        self.logger.info(f"Became leader for term {self.persistent.current_term}")
        
        if self.on_become_leader:
            await self.on_become_leader()
        
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        
        self._heartbeat_task = asyncio.create_task(self._leader_heartbeat_loop())
    
    async def _leader_heartbeat_loop(self):
        interval = self.config.raft.heartbeat_interval / 1000.0
        
        while self._running and self.state == RaftState.LEADER:
            await self._send_append_entries()
            await asyncio.sleep(interval)
    
    async def _send_append_entries(self):
        if self.state != RaftState.LEADER or not self.leader_state:
            return
        
        for node in self._get_other_nodes():
            node_id = node.node_id
            next_idx = self.leader_state.next_index.get(node_id, 1)
            
            prev_log_index = next_idx - 1
            prev_log_term = 0
            if prev_log_index > 0:
                prev_entry = self._get_log_entry(prev_log_index)
                if prev_entry:
                    prev_log_term = prev_entry.term
            
            entries = []
            if next_idx <= len(self.persistent.log):
                entries = [e.to_dict() for e in self.persistent.log[next_idx - 1:]]
            
            message = Message(
                type=MessageType.APPEND_ENTRIES,
                sender_id=self.node_id,
                term=self.persistent.current_term,
                payload={
                    "leader_id": self.node_id,
                    "prev_log_index": prev_log_index,
                    "prev_log_term": prev_log_term,
                    "entries": entries,
                    "leader_commit": self.volatile.commit_index,
                },
            )
            
            response = await self.transport.send(node.host, node.port, message)
            
            if response:
                if response.payload.get("success"):
                    if entries:
                        self.leader_state.match_index[node_id] = prev_log_index + len(entries)
                        self.leader_state.next_index[node_id] = self.leader_state.match_index[node_id] + 1
                else:
                    if response.term > self.persistent.current_term:
                        await self._step_down(response.term)
                    else:
                        self.leader_state.next_index[node_id] = max(1, next_idx - 1)
        
        await self._try_commit()
    
    async def _try_commit(self):
        if self.state != RaftState.LEADER or not self.leader_state:
            return
        
        for n in range(self.volatile.commit_index + 1, len(self.persistent.log) + 1):
            entry = self._get_log_entry(n)
            if not entry or entry.term != self.persistent.current_term:
                continue
            
            match_count = 1
            for node_id, match_idx in self.leader_state.match_index.items():
                if match_idx >= n:
                    match_count += 1
            
            if match_count >= self._get_majority():
                self.volatile.commit_index = n
        
        await self._apply_committed()
    
    async def _apply_committed(self):
        while self.volatile.last_applied < self.volatile.commit_index:
            self.volatile.last_applied += 1
            entry = self._get_log_entry(self.volatile.last_applied)
            
            if entry and self.command_handler:
                await self.command_handler(entry.command)
    
    async def _step_down(self, new_term: int):
        old_state = self.state
        
        self.state = RaftState.FOLLOWER
        self.persistent.current_term = new_term
        self.persistent.voted_for = None
        self.leader_state = None
        self._save_state()
        
        self.metrics.set_state(self.state.value)
        self.metrics.set_term(self.persistent.current_term)
        
        if old_state == RaftState.LEADER and self.on_lose_leadership:
            await self.on_lose_leadership()
    
    async def _handle_request_vote(self, message: Message) -> Optional[Message]:
        payload = message.payload
        candidate_id = payload["candidate_id"]
        last_log_index = payload["last_log_index"]
        last_log_term = payload["last_log_term"]
        
        if message.term > self.persistent.current_term:
            await self._step_down(message.term)
        
        vote_granted = False
        
        if message.term >= self.persistent.current_term:
            if self.persistent.voted_for is None or self.persistent.voted_for == candidate_id:
                my_last_term = self._get_last_log_term()
                my_last_index = self._get_last_log_index()
                
                log_ok = (last_log_term > my_last_term or
                         (last_log_term == my_last_term and last_log_index >= my_last_index))
                
                if log_ok:
                    vote_granted = True
                    self.persistent.voted_for = candidate_id
                    self._save_state()
                    self._last_heartbeat = asyncio.get_event_loop().time()
        
        return Message(
            type=MessageType.REQUEST_VOTE_RESPONSE,
            sender_id=self.node_id,
            term=self.persistent.current_term,
            payload={"vote_granted": vote_granted},
        )
    
    async def _handle_append_entries(self, message: Message) -> Optional[Message]:
        payload = message.payload
        
        if message.term > self.persistent.current_term:
            await self._step_down(message.term)
        
        self._last_heartbeat = asyncio.get_event_loop().time()
        
        if message.term < self.persistent.current_term:
            return Message(
                type=MessageType.APPEND_ENTRIES_RESPONSE,
                sender_id=self.node_id,
                term=self.persistent.current_term,
                payload={"success": False},
            )
        
        self.current_leader = payload["leader_id"]
        
        if self.state == RaftState.CANDIDATE:
            self.state = RaftState.FOLLOWER
            self.metrics.set_state(self.state.value)
        
        prev_log_index = payload["prev_log_index"]
        prev_log_term = payload["prev_log_term"]
        
        if prev_log_index > 0:
            prev_entry = self._get_log_entry(prev_log_index)
            if not prev_entry or prev_entry.term != prev_log_term:
                return Message(
                    type=MessageType.APPEND_ENTRIES_RESPONSE,
                    sender_id=self.node_id,
                    term=self.persistent.current_term,
                    payload={"success": False},
                )
        
        entries = [LogEntry.from_dict(e) for e in payload.get("entries", [])]
        
        for entry in entries:
            if entry.index <= len(self.persistent.log):
                existing = self.persistent.log[entry.index - 1]
                if existing.term != entry.term:
                    self.persistent.log = self.persistent.log[:entry.index - 1]
                    self.persistent.log.append(entry)
            else:
                self.persistent.log.append(entry)
        
        if entries:
            self._save_state()
            self.metrics.set_log_entries(len(self.persistent.log))
        
        leader_commit = payload["leader_commit"]
        if leader_commit > self.volatile.commit_index:
            self.volatile.commit_index = min(leader_commit, self._get_last_log_index())
            await self._apply_committed()
        
        return Message(
            type=MessageType.APPEND_ENTRIES_RESPONSE,
            sender_id=self.node_id,
            term=self.persistent.current_term,
            payload={"success": True},
        )
    
    async def propose(self, command: Dict[str, Any]) -> bool:
        if self.state != RaftState.LEADER:
            return False
        
        entry = LogEntry(
            term=self.persistent.current_term,
            index=len(self.persistent.log) + 1,
            command=command,
        )
        
        self.persistent.log.append(entry)
        self._save_state()
        self.metrics.set_log_entries(len(self.persistent.log))
        
        await self._send_append_entries()
        
        return True
    
    def is_leader(self) -> bool:
        return self.state == RaftState.LEADER
    
    def get_leader(self) -> Optional[str]:
        return self.current_leader
    
    def get_term(self) -> int:
        return self.persistent.current_term
