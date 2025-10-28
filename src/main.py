import asyncio
import logging
import signal
import sys

from aiohttp import web

from src.utils.config import load_config
from src.nodes.lock_manager import LockManager, LockType, LockStatus
from src.nodes.queue_node import QueueNode
from src.nodes.cache_node import CacheNode


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger("main")


class DistributedNode:
    def __init__(self):
        self.config = load_config()
        
        self.lock_manager = LockManager(self.config)
        self.queue_node = QueueNode(self.config)
        self.cache_node = CacheNode(self.config)
        
        self.app = web.Application()
        self._setup_routes()
    
    def _setup_routes(self):
        self.app.router.add_get("/health", self._health)
        self.app.router.add_get("/metrics", self._metrics)
        
        self.app.router.add_post("/lock/acquire", self._acquire_lock)
        self.app.router.add_post("/lock/release", self._release_lock)
        self.app.router.add_get("/lock/info/{resource_id}", self._lock_info)
        self.app.router.add_get("/locks", self._all_locks)
        
        self.app.router.add_post("/queue/publish", self._queue_publish)
        self.app.router.add_post("/queue/consume", self._queue_consume)
        self.app.router.add_post("/queue/ack", self._queue_ack)
        self.app.router.add_get("/queue/info/{queue_name}", self._queue_info)
        
        self.app.router.add_get("/cache/{key}", self._cache_read)
        self.app.router.add_put("/cache/{key}", self._cache_write)
        self.app.router.add_delete("/cache/{key}", self._cache_invalidate)
        self.app.router.add_get("/cache", self._cache_info)
    
    async def _health(self, request: web.Request) -> web.Response:
        return web.json_response({
            "status": "healthy",
            "node_id": self.config.node.node_id,
            "is_leader": self.lock_manager.raft.is_leader(),
            "current_leader": self.lock_manager.raft.get_leader(),
            "term": self.lock_manager.raft.get_term(),
        })
    
    async def _metrics(self, request: web.Request) -> web.Response:
        return web.Response(
            body=self.lock_manager.metrics.get_metrics(),
            content_type=self.lock_manager.metrics.get_content_type(),
        )
    
    async def _acquire_lock(self, request: web.Request) -> web.Response:
        data = await request.json()
        
        client_id = data.get("client_id")
        resource_id = data.get("resource_id")
        lock_type_str = data.get("lock_type", "exclusive")
        timeout = data.get("timeout", 30.0)
        
        if not client_id or not resource_id:
            return web.json_response({"error": "client_id and resource_id required"}, status=400)
        
        lock_type = LockType.SHARED if lock_type_str == "shared" else LockType.EXCLUSIVE
        
        status = await self.lock_manager.acquire_lock(client_id, resource_id, lock_type, timeout)
        
        return web.json_response({
            "status": status.value,
            "resource_id": resource_id,
            "lock_type": lock_type.value,
        })
    
    async def _release_lock(self, request: web.Request) -> web.Response:
        data = await request.json()
        
        client_id = data.get("client_id")
        resource_id = data.get("resource_id")
        
        if not client_id or not resource_id:
            return web.json_response({"error": "client_id and resource_id required"}, status=400)
        
        status = await self.lock_manager.release_lock(client_id, resource_id)
        
        return web.json_response({"status": status.value})
    
    async def _lock_info(self, request: web.Request) -> web.Response:
        resource_id = request.match_info["resource_id"]
        info = self.lock_manager.get_lock_info(resource_id)
        
        if info:
            return web.json_response(info)
        return web.json_response({"error": "Lock not found"}, status=404)
    
    async def _all_locks(self, request: web.Request) -> web.Response:
        locks = self.lock_manager.get_all_locks()
        return web.json_response({"locks": locks})
    
    async def _queue_publish(self, request: web.Request) -> web.Response:
        data = await request.json()
        
        queue_name = data.get("queue_name")
        payload = data.get("payload")
        message_id = data.get("message_id")
        
        if not queue_name or payload is None:
            return web.json_response({"error": "queue_name and payload required"}, status=400)
        
        msg_id = await self.queue_node.publish(queue_name, payload, message_id)
        
        return web.json_response({"message_id": msg_id, "queue_name": queue_name})
    
    async def _queue_consume(self, request: web.Request) -> web.Response:
        data = await request.json()
        
        queue_name = data.get("queue_name")
        limit = data.get("limit", 1)
        
        if not queue_name:
            return web.json_response({"error": "queue_name required"}, status=400)
        
        messages = await self.queue_node.consume(queue_name, limit)
        
        return web.json_response({
            "messages": [
                {
                    "id": m.id,
                    "payload": m.payload,
                    "attempts": m.attempts,
                }
                for m in messages
            ]
        })
    
    async def _queue_ack(self, request: web.Request) -> web.Response:
        data = await request.json()
        
        message_id = data.get("message_id")
        
        if not message_id:
            return web.json_response({"error": "message_id required"}, status=400)
        
        success = await self.queue_node.ack(message_id)
        
        return web.json_response({"success": success})
    
    async def _queue_info(self, request: web.Request) -> web.Response:
        queue_name = request.match_info["queue_name"]
        info = self.queue_node.get_queue_info(queue_name)
        return web.json_response(info)
    
    async def _cache_read(self, request: web.Request) -> web.Response:
        key = request.match_info["key"]
        value = await self.cache_node.read(key)
        
        if value is not None:
            return web.json_response({"key": key, "value": value})
        return web.json_response({"error": "Key not found"}, status=404)
    
    async def _cache_write(self, request: web.Request) -> web.Response:
        key = request.match_info["key"]
        data = await request.json()
        value = data.get("value")
        
        if value is None:
            return web.json_response({"error": "value required"}, status=400)
        
        success = await self.cache_node.write(key, value)
        
        return web.json_response({"success": success, "key": key})
    
    async def _cache_invalidate(self, request: web.Request) -> web.Response:
        key = request.match_info["key"]
        success = self.cache_node.invalidate(key)
        
        return web.json_response({"success": success, "key": key})
    
    async def _cache_info(self, request: web.Request) -> web.Response:
        info = self.cache_node.get_cache_info()
        return web.json_response(info)
    
    async def start(self):
        await self.lock_manager.start()
        await self.queue_node.start()
        await self.cache_node.start()
        
        logger.info(f"Distributed node {self.config.node.node_id} started")
    
    async def stop(self):
        await self.lock_manager.stop()
        await self.queue_node.stop()
        await self.cache_node.stop()
        
        logger.info(f"Distributed node {self.config.node.node_id} stopped")


async def main():
    node = DistributedNode()
    
    loop = asyncio.get_event_loop()
    
    def signal_handler():
        loop.create_task(shutdown(node))
    
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)
    
    await node.start()
    
    runner = web.AppRunner(node.app)
    await runner.setup()
    
    site = web.TCPSite(
        runner, 
        node.config.node.host, 
        node.config.node.port + 100,
    )
    await site.start()
    
    logger.info(f"API server started on port {node.config.node.port + 100}")
    
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        await node.stop()
        await runner.cleanup()


async def shutdown(node: DistributedNode):
    logger.info("Shutting down...")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
