# Benchmarks

## Run

### Run `chicory`

```bash
# terminal 1
make chicory-worker

# terminal 2
make chicory-run
```

### Run `taskiq`

```bash
# terminal 1
make taskiq-worker

# terminal 2
make taskiq-run
```

## Results

```
Debian GNU/Linux 13 (trixie) x86_64
6.12.57+deb13-amd64

Intel Xeon E5-2699 v4 (44) @ 3.600GHz
80348 MiB
```

### Chicory

```
2025-12-28 21:36:28,935 - bench_chicory - INFO - =============== TIMINGS ===============
2025-12-28 21:36:28,935 - bench_chicory - INFO - tasks:      8, enqueue:    0.005s, dequeue:    0.105s
2025-12-28 21:36:28,935 - bench_chicory - INFO - tasks:     16, enqueue:    0.010s, dequeue:    0.112s
2025-12-28 21:36:28,935 - bench_chicory - INFO - tasks:     32, enqueue:    0.022s, dequeue:    0.119s
2025-12-28 21:36:28,935 - bench_chicory - INFO - tasks:     64, enqueue:    0.038s, dequeue:    0.126s
2025-12-28 21:36:28,935 - bench_chicory - INFO - tasks:    128, enqueue:    0.055s, dequeue:    0.238s
2025-12-28 21:36:28,936 - bench_chicory - INFO - tasks:    256, enqueue:    0.092s, dequeue:    0.387s
2025-12-28 21:36:28,936 - bench_chicory - INFO - tasks:   1024, enqueue:    0.291s, dequeue:    1.130s
2025-12-28 21:36:28,936 - bench_chicory - INFO - tasks:   2048, enqueue:    0.544s, dequeue:    2.115s
2025-12-28 21:36:28,936 - bench_chicory - INFO - tasks:   4096, enqueue:    0.887s, dequeue:    3.903s
2025-12-28 21:36:28,936 - bench_chicory - INFO - tasks:   8192, enqueue:    1.984s, dequeue:    8.217s
2025-12-28 21:36:28,936 - bench_chicory - INFO - tasks:  16384, enqueue:    3.789s, dequeue:   16.379s
2025-12-28 21:36:28,936 - bench_chicory - INFO - =======================================
```

> **Note**: Creates few connections to Redis even while inserting 16384 records, but
> the commands per second are high and make Redis CPU skyrocket.

### TaskIQ

```
2025-12-28 21:35:15,333 - bench_taskiq - INFO - =============== TIMINGS ===============
2025-12-28 21:35:15,333 - bench_taskiq - INFO - tasks:      8, enqueue:    0.041s, dequeue:    0.057s
2025-12-28 21:35:15,333 - bench_taskiq - INFO - tasks:     16, enqueue:    0.027s, dequeue:    0.135s
2025-12-28 21:35:15,333 - bench_taskiq - INFO - tasks:     32, enqueue:    0.044s, dequeue:    0.187s
2025-12-28 21:35:15,333 - bench_taskiq - INFO - tasks:     64, enqueue:    0.085s, dequeue:    0.219s
2025-12-28 21:35:15,333 - bench_taskiq - INFO - tasks:    128, enqueue:    0.165s, dequeue:    0.308s
2025-12-28 21:35:15,333 - bench_taskiq - INFO - tasks:    256, enqueue:    0.307s, dequeue:    0.470s
2025-12-28 21:35:15,333 - bench_taskiq - INFO - tasks:   1024, enqueue:    1.219s, dequeue:    1.436s
2025-12-28 21:35:15,334 - bench_taskiq - INFO - tasks:   2048, enqueue:    2.259s, dequeue:    2.825s
2025-12-28 21:35:15,334 - bench_taskiq - INFO - tasks:   4096, enqueue:    4.916s, dequeue:    5.944s
2025-12-28 21:35:15,334 - bench_taskiq - INFO - tasks:   8192, enqueue:   10.951s, dequeue:   13.174s
2025-12-28 21:35:15,334 - bench_taskiq - INFO - tasks:  16384, enqueue:   23.592s, dequeue:   36.781s
2025-12-28 21:35:15,334 - bench_taskiq - INFO - =======================================
```

> **Note**: Creates one connection per result. e.g. inserting 128 tasks will result in 128
> concurrent connections to Redis. While working with 16384 tasks, the connections count
> doubled to 32768: needs investigation as to why.
