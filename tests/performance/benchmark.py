import asyncio
import time
import statistics
import argparse
import httpx


class BenchmarkRunner:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.results = []
    
    async def run_lock_benchmark(self, num_requests: int = 100):
        print(f"\n=== Lock Manager Benchmark ({num_requests} requests) ===")
        
        async with httpx.AsyncClient(timeout=30) as client:
            latencies = []
            errors = 0
            
            for i in range(num_requests):
                start = time.time()
                try:
                    response = await client.post(
                        f"{self.base_url}/lock/acquire",
                        json={
                            "client_id": f"client-{i}",
                            "resource_id": f"resource-{i % 10}",
                            "lock_type": "exclusive",
                        },
                    )
                    latency = (time.time() - start) * 1000
                    latencies.append(latency)
                    
                    if response.status_code == 200:
                        await client.post(
                            f"{self.base_url}/lock/release",
                            json={
                                "client_id": f"client-{i}",
                                "resource_id": f"resource-{i % 10}",
                            },
                        )
                except Exception as e:
                    errors += 1
            
            self._print_stats("Lock acquire/release", latencies, errors)
    
    async def run_queue_benchmark(self, num_requests: int = 100):
        print(f"\n=== Queue Benchmark ({num_requests} requests) ===")
        
        async with httpx.AsyncClient(timeout=30) as client:
            publish_latencies = []
            consume_latencies = []
            errors = 0
            
            for i in range(num_requests):
                start = time.time()
                try:
                    response = await client.post(
                        f"{self.base_url}/queue/publish",
                        json={
                            "queue_name": "benchmark-queue",
                            "payload": {"message_num": i, "data": "x" * 100},
                        },
                    )
                    latency = (time.time() - start) * 1000
                    publish_latencies.append(latency)
                except Exception as e:
                    errors += 1
            
            for i in range(num_requests):
                start = time.time()
                try:
                    response = await client.post(
                        f"{self.base_url}/queue/consume",
                        json={"queue_name": "benchmark-queue", "limit": 1},
                    )
                    latency = (time.time() - start) * 1000
                    consume_latencies.append(latency)
                    
                    if response.status_code == 200:
                        data = response.json()
                        for msg in data.get("messages", []):
                            await client.post(
                                f"{self.base_url}/queue/ack",
                                json={"message_id": msg["id"]},
                            )
                except Exception as e:
                    errors += 1
            
            self._print_stats("Queue publish", publish_latencies, 0)
            self._print_stats("Queue consume", consume_latencies, errors)
    
    async def run_cache_benchmark(self, num_requests: int = 100):
        print(f"\n=== Cache Benchmark ({num_requests} requests) ===")
        
        async with httpx.AsyncClient(timeout=30) as client:
            write_latencies = []
            read_latencies = []
            errors = 0
            
            for i in range(num_requests):
                start = time.time()
                try:
                    response = await client.put(
                        f"{self.base_url}/cache/key-{i}",
                        json={"value": {"data": "x" * 100, "num": i}},
                    )
                    latency = (time.time() - start) * 1000
                    write_latencies.append(latency)
                except Exception as e:
                    errors += 1
            
            for i in range(num_requests):
                start = time.time()
                try:
                    response = await client.get(f"{self.base_url}/cache/key-{i}")
                    latency = (time.time() - start) * 1000
                    read_latencies.append(latency)
                except Exception as e:
                    errors += 1
            
            self._print_stats("Cache write", write_latencies, 0)
            self._print_stats("Cache read", read_latencies, errors)
    
    async def run_concurrent_benchmark(self, num_concurrent: int = 10):
        print(f"\n=== Concurrent Lock Benchmark ({num_concurrent} concurrent) ===")
        
        async def acquire_and_release(client_id: int):
            async with httpx.AsyncClient(timeout=30) as client:
                start = time.time()
                try:
                    await client.post(
                        f"{self.base_url}/lock/acquire",
                        json={
                            "client_id": f"concurrent-{client_id}",
                            "resource_id": "shared-resource",
                            "lock_type": "exclusive",
                        },
                    )
                    
                    await asyncio.sleep(0.01)
                    
                    await client.post(
                        f"{self.base_url}/lock/release",
                        json={
                            "client_id": f"concurrent-{client_id}",
                            "resource_id": "shared-resource",
                        },
                    )
                    
                    return (time.time() - start) * 1000
                except Exception:
                    return None
        
        tasks = [acquire_and_release(i) for i in range(num_concurrent)]
        results = await asyncio.gather(*tasks)
        
        latencies = [r for r in results if r is not None]
        errors = len([r for r in results if r is None])
        
        self._print_stats("Concurrent lock acquire/release", latencies, errors)
    
    def _print_stats(self, name: str, latencies: list, errors: int):
        if not latencies:
            print(f"{name}: No successful requests")
            return
        
        print(f"\n{name}:")
        print(f"  Requests:  {len(latencies)}")
        print(f"  Errors:    {errors}")
        print(f"  Min:       {min(latencies):.2f} ms")
        print(f"  Max:       {max(latencies):.2f} ms")
        print(f"  Mean:      {statistics.mean(latencies):.2f} ms")
        print(f"  Median:    {statistics.median(latencies):.2f} ms")
        if len(latencies) > 1:
            print(f"  Std Dev:   {statistics.stdev(latencies):.2f} ms")
        print(f"  Throughput: {len(latencies) / (sum(latencies) / 1000):.2f} req/s")


async def main():
    parser = argparse.ArgumentParser(description="Benchmark distributed system")
    parser.add_argument("--url", default="http://localhost:8100", help="Base URL")
    parser.add_argument("--requests", type=int, default=100, help="Number of requests")
    parser.add_argument("--concurrent", type=int, default=10, help="Concurrent requests")
    args = parser.parse_args()
    
    runner = BenchmarkRunner(args.url)
    
    print(f"Benchmarking {args.url}")
    print("=" * 50)
    
    try:
        await runner.run_lock_benchmark(args.requests)
        await runner.run_queue_benchmark(args.requests)
        await runner.run_cache_benchmark(args.requests)
        await runner.run_concurrent_benchmark(args.concurrent)
    except httpx.ConnectError:
        print(f"Error: Could not connect to {args.url}")
        print("Make sure the distributed system is running.")


if __name__ == "__main__":
    asyncio.run(main())
