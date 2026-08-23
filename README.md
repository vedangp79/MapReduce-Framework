# MapReduce Framework

A fault-tolerant, distributed MapReduce framework implemented from scratch in Python, inspired by Google's original MapReduce paper. Includes a Manager/Worker cluster architecture, automatic failure detection, and task reassignment — no external MapReduce library used.

## How it works

- **Manager** accepts job submissions over TCP, partitions input into map and reduce tasks, and assigns them to available workers.
- **Workers** register with the Manager, execute user-provided map/reduce executables as subprocesses (Hadoop-streaming style), and report task completion over TCP.
- **Fault tolerance**: Workers send heartbeat pings to the Manager over UDP every 2 seconds. If a worker misses heartbeats, the Manager marks it dead and reassigns its in-progress task to another worker — no job restart required.
- **Map phase**: output lines are partitioned across reducers using an MD5 hash of the key, then externally sorted before the reduce phase begins (mirroring real MapReduce shuffle/sort behavior).
- Includes a CLI (`mapreduce-submit`) for submitting jobs and a shell script for starting/stopping a local cluster.

## Tech stack
Python 3.10+, raw TCP/UDP sockets, multithreading, Click (CLI), pytest (test suite covering manager, worker, and integration scenarios).

## Running it locally

```bash
pip install -e .

# Start a local cluster: 1 manager + 2 workers
./bin/mapreduce start

# Submit a word-count job (uses bundled sample mapper/reducer)
mapreduce-submit

# Check cluster status / stop it
./bin/mapreduce status
./bin/mapreduce stop
```

## Testing
```bash
pytest tests/
```
Covers manager task assignment, worker fault-recovery, and full end-to-end job integration.

## Origin
Originally built as a course project for a distributed systems class; the Manager/Worker/fault-tolerance implementation above is my own addition.
