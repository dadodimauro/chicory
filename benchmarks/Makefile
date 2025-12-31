.PHONY: taskiq-worker
taskiq-worker:
	taskiq worker --workers 1 --max-async-tasks 32 broker_redis.bench_taskiq:broker --max-prefetch 32

.PHONY: taskiq-run
taskiq-run:
	python broker_redis/bench_taskiq.py

.PHONY: chicory-worker
chicory-worker:
	chicory worker broker_redis.bench_chicory:broker

.PHONY: chicory-run
chicory-run:
	python broker_redis/bench_chicory.py
