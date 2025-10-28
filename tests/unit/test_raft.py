import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
import tempfile
import os

from src.consensus.raft import RaftNode, RaftState, LogEntry, PersistentState
from src.utils.config import Config, NodeConfig, ClusterConfig, RaftConfig
from src.communication.message_passing import MessageTransport, Message, MessageType
from src.utils.metrics import MetricsCollector


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


@pytest.fixture
def transport():
    return MagicMock(spec=MessageTransport)


@pytest.fixture
def metrics():
    return MagicMock(spec=MetricsCollector)


@pytest.fixture
def raft_node(config, transport, metrics):
    with tempfile.TemporaryDirectory() as tmpdir:
        state_path = os.path.join(tmpdir, "raft_state.json")
        node = RaftNode(
            config=config,
            transport=transport,
            metrics=metrics,
            state_path=state_path,
        )
        yield node


class TestRaftState:
    def test_initial_state_is_follower(self, raft_node):
        assert raft_node.state == RaftState.FOLLOWER
    
    def test_initial_term_is_zero(self, raft_node):
        assert raft_node.persistent.current_term == 0
    
    def test_initial_voted_for_is_none(self, raft_node):
        assert raft_node.persistent.voted_for is None
    
    def test_initial_log_is_empty(self, raft_node):
        assert len(raft_node.persistent.log) == 0


class TestLogEntry:
    def test_log_entry_to_dict(self):
        entry = LogEntry(term=1, index=1, command={"action": "set", "key": "a"})
        d = entry.to_dict()
        
        assert d["term"] == 1
        assert d["index"] == 1
        assert d["command"] == {"action": "set", "key": "a"}
    
    def test_log_entry_from_dict(self):
        d = {"term": 2, "index": 3, "command": {"action": "delete"}}
        entry = LogEntry.from_dict(d)
        
        assert entry.term == 2
        assert entry.index == 3
        assert entry.command == {"action": "delete"}


class TestPersistentState:
    def test_persistent_state_to_dict(self):
        state = PersistentState(
            current_term=5,
            voted_for="node2",
            log=[LogEntry(term=1, index=1, command={"x": 1})],
        )
        d = state.to_dict()
        
        assert d["current_term"] == 5
        assert d["voted_for"] == "node2"
        assert len(d["log"]) == 1
    
    def test_persistent_state_from_dict(self):
        d = {
            "current_term": 3,
            "voted_for": None,
            "log": [{"term": 1, "index": 1, "command": {}}],
        }
        state = PersistentState.from_dict(d)
        
        assert state.current_term == 3
        assert state.voted_for is None
        assert len(state.log) == 1


class TestRequestVote:
    @pytest.mark.asyncio
    async def test_vote_granted_when_term_higher(self, raft_node):
        message = Message(
            type=MessageType.REQUEST_VOTE,
            sender_id="node2",
            term=5,
            payload={
                "candidate_id": "node2",
                "last_log_index": 0,
                "last_log_term": 0,
            },
        )
        
        response = await raft_node._handle_request_vote(message)
        
        assert response.payload["vote_granted"] is True
        assert raft_node.persistent.voted_for == "node2"
        assert raft_node.persistent.current_term == 5
    
    @pytest.mark.asyncio
    async def test_vote_denied_when_term_lower(self, raft_node):
        raft_node.persistent.current_term = 10
        
        message = Message(
            type=MessageType.REQUEST_VOTE,
            sender_id="node2",
            term=5,
            payload={
                "candidate_id": "node2",
                "last_log_index": 0,
                "last_log_term": 0,
            },
        )
        
        response = await raft_node._handle_request_vote(message)
        
        assert response.payload["vote_granted"] is False
    
    @pytest.mark.asyncio
    async def test_vote_denied_when_already_voted(self, raft_node):
        raft_node.persistent.current_term = 5
        raft_node.persistent.voted_for = "node3"
        
        message = Message(
            type=MessageType.REQUEST_VOTE,
            sender_id="node2",
            term=5,
            payload={
                "candidate_id": "node2",
                "last_log_index": 0,
                "last_log_term": 0,
            },
        )
        
        response = await raft_node._handle_request_vote(message)
        
        assert response.payload["vote_granted"] is False


class TestAppendEntries:
    @pytest.mark.asyncio
    async def test_append_entries_success_empty(self, raft_node):
        message = Message(
            type=MessageType.APPEND_ENTRIES,
            sender_id="node2",
            term=1,
            payload={
                "leader_id": "node2",
                "prev_log_index": 0,
                "prev_log_term": 0,
                "entries": [],
                "leader_commit": 0,
            },
        )
        
        response = await raft_node._handle_append_entries(message)
        
        assert response.payload["success"] is True
        assert raft_node.current_leader == "node2"
    
    @pytest.mark.asyncio
    async def test_append_entries_with_entries(self, raft_node):
        message = Message(
            type=MessageType.APPEND_ENTRIES,
            sender_id="node2",
            term=1,
            payload={
                "leader_id": "node2",
                "prev_log_index": 0,
                "prev_log_term": 0,
                "entries": [
                    {"term": 1, "index": 1, "command": {"x": 1}},
                    {"term": 1, "index": 2, "command": {"x": 2}},
                ],
                "leader_commit": 0,
            },
        )
        
        response = await raft_node._handle_append_entries(message)
        
        assert response.payload["success"] is True
        assert len(raft_node.persistent.log) == 2
    
    @pytest.mark.asyncio
    async def test_append_entries_fail_log_mismatch(self, raft_node):
        message = Message(
            type=MessageType.APPEND_ENTRIES,
            sender_id="node2",
            term=1,
            payload={
                "leader_id": "node2",
                "prev_log_index": 5,
                "prev_log_term": 1,
                "entries": [],
                "leader_commit": 0,
            },
        )
        
        response = await raft_node._handle_append_entries(message)
        
        assert response.payload["success"] is False


class TestMajority:
    def test_majority_three_nodes(self, raft_node):
        assert raft_node._get_majority() == 2
    
    def test_majority_five_nodes(self, config, transport, metrics):
        config.cluster.nodes.extend([
            NodeConfig(node_id="node4", host="localhost", port=8003),
            NodeConfig(node_id="node5", host="localhost", port=8004),
        ])
        
        with tempfile.TemporaryDirectory() as tmpdir:
            node = RaftNode(
                config=config,
                transport=transport,
                metrics=metrics,
                state_path=os.path.join(tmpdir, "state.json"),
            )
            assert node._get_majority() == 3
