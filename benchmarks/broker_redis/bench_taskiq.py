import asyncio
import logging
import time

import redis
from taskiq_redis import RedisAsyncResultBackend, RedisStreamBroker

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("bench_taskiq")

broker = RedisStreamBroker(
    "redis://localhost:6379",
    unacknowledged_batch_size=10,
).with_result_backend(
    RedisAsyncResultBackend(
        "redis://localhost:6379",
        result_ex_time=3600,
        max_connection_pool_size=2**32,
    )
)


@broker.task(name="bench.increment")
async def increment(value: int) -> int:
    return value + 1


async def main(tasks_count: int) -> tuple[float, float]:
    loop = asyncio.get_event_loop()

    logger.info("enqueuing tasks...")
    await broker.startup()
    enqueue_start = loop.time()
    enqueue_results = await asyncio.gather(
        *[increment.kiq(i) for i in range(tasks_count)]
    )
    enqueue_end = loop.time()
    logger.info("enqueuing tasks... done")

    logger.info("retrieving results...")
    dequeue_start = loop.time()
    retrieved_values = await asyncio.gather(
        *[res.wait_result(timeout=200, check_interval=0.1) for res in enqueue_results]
    )
    dequeue_end = loop.time()
    logger.info("retrieving results... done")

    invalid_values_count: int = 0
    for result in retrieved_values:
        if result is None or result.is_err:
            invalid_values_count += 1

    if invalid_values_count > 0:
        logger.warning(f"found {invalid_values_count} invalid results")
    else:
        logger.debug("all results are valid")

    await broker.shutdown()

    return enqueue_end - enqueue_start, dequeue_end - dequeue_start


if __name__ == "__main__":
    tasks = [8, 16, 32, 64, 128, 256, 1024, 2048, 4096, 8192, 16384]
    results: list[tuple[float, float]] = []
    with redis.Redis(host="localhost", port=6379) as client:
        for t in tasks:
            client.flushall()
            # note: the sleep here is needed to give time to taskiq workers to reconnect
            # without affecting the benchmark results.
            time.sleep(3)
            logger.info(f"starting benchmark for {t} tasks...")
            results.append(asyncio.run(main(t)))
            logger.info(f"starting benchmark for {t} tasks... done")
        client.flushall()

    logger.info("=============== TIMINGS ===============")
    for t, (enqueue_time, dequeue_time) in zip(tasks, results):
        logger.info(
            f"tasks: {t:>6}, enqueue: {enqueue_time:>8.3f}s, dequeue: {dequeue_time:>8.3f}s"
        )
    logger.info("=======================================")
