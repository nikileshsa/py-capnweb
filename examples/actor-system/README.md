# Actor System Example

Demonstrates a distributed actor system using Cap'n Web with location-transparent capabilities.

## Features

- Supervisor pattern for managing workers
- Location-transparent capability passing
- Direct communication with spawned workers
- Concurrent message handling

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         Client                               │
│                                                              │
│  1. spawn_worker("Worker-A") ────────────────────────────┐   │
│  2. spawn_worker("Worker-B") ────────────────────────────┤   │
│                                                          │   │
│  3. worker_a.increment() ◀───────────────────────────────┤   │
│  4. worker_b.increment() ◀───────────────────────────────┤   │
│                                                          │   │
└──────────────────────────────────────────────────────────────┘
                                                           │
                                                           │ HTTP
                                                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    Supervisor (Port 8080)                    │
│                                                              │
│  Methods:                                                    │
│  - spawn_worker(name) → Worker capability                    │
│  - list_workers() → [names]                                  │
│                                                              │
│  Workers:                                                    │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │  Worker-A   │  │  Worker-B   │  │  Worker-C   │  ...     │
│  │  count: 0   │  │  count: 0   │  │  count: 0   │          │
│  └─────────────┘  └─────────────┘  └─────────────┘          │
└─────────────────────────────────────────────────────────────┘
```

## Running

### Step 1: Start the Supervisor

```bash
cd capnweb-python
uv run python examples/actor-system/supervisor.py
```

### Step 2: Run the client (new terminal)

```bash
cd capnweb-python
uv run python examples/actor-system/client.py
```

**Expected output:**
```
--- Distributed Actor System Demo ---

1. Spawning two workers...
   ✓ Spawned Worker-A and Worker-B

2. Interacting directly with workers...

3. Sending 'increment' messages to workers...
   ✓ Sent two increments to Worker-A, one to Worker-B

4. Verifying final worker states...
   - Final count for Worker-A: 2
   - Final count for Worker-B: 1

✅ Demo finished successfully!

5. Listing all workers via supervisor...
   Active workers: ['Worker-A', 'Worker-B']
```

## Key Concepts

### Location Transparency

When the client spawns a worker, it receives a **capability** to that worker.
The client can then communicate directly with the worker without going through
the supervisor. This is location transparency - the client doesn't need to know
where the worker is located.

### Supervisor Pattern

The supervisor manages the lifecycle of workers:
- Creates workers on demand
- Tracks all active workers
- Can terminate workers if needed

### Capability-Based Access

Workers are only accessible via their capabilities. You cannot access a worker
without having received its capability from the supervisor.

## Worker Methods

Each worker supports:
- `increment()` - Increment the counter
- `decrement()` - Decrement the counter
- `get_count()` - Get current counter value
- `reset()` - Reset counter to zero
