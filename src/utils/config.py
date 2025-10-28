from dataclasses import dataclass, field
from typing import List, Optional
import os
from dotenv import load_dotenv


@dataclass
class NodeConfig:
    node_id: str
    host: str
    port: int


@dataclass
class ClusterConfig:
    nodes: List[NodeConfig]
    
    def get_node(self, node_id: str) -> Optional[NodeConfig]:
        for node in self.nodes:
            if node.node_id == node_id:
                return node
        return None
    
    def get_other_nodes(self, node_id: str) -> List[NodeConfig]:
        return [n for n in self.nodes if n.node_id != node_id]


@dataclass
class RaftConfig:
    election_timeout_min: int = 150
    election_timeout_max: int = 300
    heartbeat_interval: int = 50


@dataclass
class CacheConfig:
    max_size: int = 1000


@dataclass
class QueueConfig:
    persistence_path: str = "/data/queue"


@dataclass
class Config:
    node: NodeConfig
    cluster: ClusterConfig
    raft: RaftConfig = field(default_factory=RaftConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    queue: QueueConfig = field(default_factory=QueueConfig)
    redis_host: str = "localhost"
    redis_port: int = 6379
    log_level: str = "INFO"


def parse_cluster_nodes(nodes_str: str) -> List[NodeConfig]:
    nodes = []
    for i, node_spec in enumerate(nodes_str.split(",")):
        parts = node_spec.strip().split(":")
        if len(parts) == 2:
            node_id = parts[0]
            port = int(parts[1])
            nodes.append(NodeConfig(node_id=node_id, host=node_id, port=port))
    return nodes


def load_config(env_path: Optional[str] = None) -> Config:
    if env_path:
        load_dotenv(env_path)
    else:
        load_dotenv()
    
    node_id = os.getenv("NODE_ID", "node1")
    node_host = os.getenv("NODE_HOST", "0.0.0.0")
    node_port = int(os.getenv("NODE_PORT", "8000"))
    
    cluster_nodes_str = os.getenv("CLUSTER_NODES", "node1:8000,node2:8001,node3:8002")
    cluster_nodes = parse_cluster_nodes(cluster_nodes_str)
    
    return Config(
        node=NodeConfig(node_id=node_id, host=node_host, port=node_port),
        cluster=ClusterConfig(nodes=cluster_nodes),
        raft=RaftConfig(
            election_timeout_min=int(os.getenv("ELECTION_TIMEOUT_MIN", "150")),
            election_timeout_max=int(os.getenv("ELECTION_TIMEOUT_MAX", "300")),
            heartbeat_interval=int(os.getenv("HEARTBEAT_INTERVAL", "50")),
        ),
        cache=CacheConfig(
            max_size=int(os.getenv("CACHE_MAX_SIZE", "1000")),
        ),
        queue=QueueConfig(
            persistence_path=os.getenv("QUEUE_PERSISTENCE_PATH", "/data/queue"),
        ),
        redis_host=os.getenv("REDIS_HOST", "localhost"),
        redis_port=int(os.getenv("REDIS_PORT", "6379")),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )
