import asyncio
import json
import logging
from dataclasses import dataclass, asdict
from typing import Any, Callable, Dict, Optional, Awaitable
from enum import Enum
import aiohttp
from aiohttp import web


class MessageType(Enum):
    REQUEST_VOTE = "request_vote"
    REQUEST_VOTE_RESPONSE = "request_vote_response"
    APPEND_ENTRIES = "append_entries"
    APPEND_ENTRIES_RESPONSE = "append_entries_response"
    LOCK_REQUEST = "lock_request"
    LOCK_RESPONSE = "lock_response"
    LOCK_RELEASE = "lock_release"
    QUEUE_PUBLISH = "queue_publish"
    QUEUE_CONSUME = "queue_consume"
    QUEUE_ACK = "queue_ack"
    CACHE_READ = "cache_read"
    CACHE_WRITE = "cache_write"
    CACHE_INVALIDATE = "cache_invalidate"
    CACHE_RESPONSE = "cache_response"
    HEARTBEAT = "heartbeat"
    HEARTBEAT_RESPONSE = "heartbeat_response"


@dataclass
class Message:
    type: MessageType
    sender_id: str
    term: int
    payload: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type.value,
            "sender_id": self.sender_id,
            "term": self.term,
            "payload": self.payload,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Message":
        return cls(
            type=MessageType(data["type"]),
            sender_id=data["sender_id"],
            term=data["term"],
            payload=data["payload"],
        )
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_json(cls, data: str) -> "Message":
        return cls.from_dict(json.loads(data))


MessageHandler = Callable[[Message], Awaitable[Optional[Message]]]


class MessageTransport:
    def __init__(self, node_id: str, host: str, port: int):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.handlers: Dict[MessageType, MessageHandler] = {}
        self.app: Optional[web.Application] = None
        self.runner: Optional[web.AppRunner] = None
        self.session: Optional[aiohttp.ClientSession] = None
        self.logger = logging.getLogger(f"transport.{node_id}")
    
    def register_handler(self, message_type: MessageType, handler: MessageHandler):
        self.handlers[message_type] = handler
    
    async def _handle_message(self, request: web.Request) -> web.Response:
        try:
            data = await request.json()
            message = Message.from_dict(data)
            
            if message.type in self.handlers:
                response = await self.handlers[message.type](message)
                if response:
                    return web.json_response(response.to_dict())
                return web.json_response({"status": "ok"})
            
            return web.json_response({"error": "unknown message type"}, status=400)
        except Exception as e:
            self.logger.error(f"Error handling message: {e}")
            return web.json_response({"error": str(e)}, status=500)
    
    async def start(self):
        self.app = web.Application()
        self.app.router.add_post("/rpc", self._handle_message)
        self.app.router.add_get("/health", self._health_check)
        
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        
        site = web.TCPSite(self.runner, self.host, self.port)
        await site.start()
        
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=5)
        )
        
        self.logger.info(f"Transport started on {self.host}:{self.port}")
    
    async def _health_check(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "healthy", "node_id": self.node_id})
    
    async def stop(self):
        if self.session:
            await self.session.close()
        if self.runner:
            await self.runner.cleanup()
    
    async def send(self, target_host: str, target_port: int, message: Message) -> Optional[Message]:
        if not self.session:
            return None
        
        url = f"http://{target_host}:{target_port}/rpc"
        
        try:
            async with self.session.post(url, json=message.to_dict()) as response:
                if response.status == 200:
                    data = await response.json()
                    if "type" in data:
                        return Message.from_dict(data)
                    return None
                return None
        except asyncio.TimeoutError:
            self.logger.warning(f"Timeout sending to {target_host}:{target_port}")
            return None
        except aiohttp.ClientError as e:
            self.logger.warning(f"Error sending to {target_host}:{target_port}: {e}")
            return None
    
    async def broadcast(
        self, 
        targets: list, 
        message: Message
    ) -> Dict[str, Optional[Message]]:
        tasks = {}
        for target in targets:
            task = asyncio.create_task(
                self.send(target.host, target.port, message)
            )
            tasks[target.node_id] = task
        
        results = {}
        for node_id, task in tasks.items():
            try:
                results[node_id] = await task
            except Exception:
                results[node_id] = None
        
        return results
