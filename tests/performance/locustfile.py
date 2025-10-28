from locust import HttpUser, task, between


class DistributedSystemUser(HttpUser):
    wait_time = between(0.1, 0.5)
    
    @task(3)
    def lock_acquire_release(self):
        client_id = f"locust-{self.environment.runner.user_count}"
        resource_id = f"resource-{hash(client_id) % 10}"
        
        response = self.client.post(
            "/lock/acquire",
            json={
                "client_id": client_id,
                "resource_id": resource_id,
                "lock_type": "exclusive",
                "timeout": 5,
            },
        )
        
        if response.status_code == 200:
            self.client.post(
                "/lock/release",
                json={
                    "client_id": client_id,
                    "resource_id": resource_id,
                },
            )
    
    @task(2)
    def queue_publish(self):
        self.client.post(
            "/queue/publish",
            json={
                "queue_name": "locust-queue",
                "payload": {"test": True, "data": "x" * 50},
            },
        )
    
    @task(2)
    def queue_consume(self):
        response = self.client.post(
            "/queue/consume",
            json={"queue_name": "locust-queue", "limit": 1},
        )
        
        if response.status_code == 200:
            data = response.json()
            for msg in data.get("messages", []):
                self.client.post(
                    "/queue/ack",
                    json={"message_id": msg["id"]},
                )
    
    @task(3)
    def cache_write(self):
        key = f"locust-key-{hash(str(self.environment.runner.user_count)) % 100}"
        self.client.put(
            f"/cache/{key}",
            json={"value": {"test": True}},
        )
    
    @task(5)
    def cache_read(self):
        key = f"locust-key-{hash(str(self.environment.runner.user_count)) % 100}"
        self.client.get(f"/cache/{key}")
    
    @task(1)
    def health_check(self):
        self.client.get("/health")
