# Distributed Task Queue with Worker Pools

A cutting-edge application demonstrating advanced Cap'n Web patterns:

## Features Demonstrated

1. **Capability Attenuation**
   - Workers receive limited capabilities based on their role
   - GPU workers get GPU resource access, CPU workers don't
   - Supervisors can create attenuated capabilities for workers

2. **Capability Revocation**
   - Supervisors can revoke worker access at any time
   - Revoked capabilities throw errors on use
   - Clean shutdown of workers when revoked

3. **Nested Capability Passing**
   - Tasks contain resource capabilities (storage, network, GPU)
   - Workers receive task capabilities and use nested resources
   - Multi-level capability chains

4. **Streaming Results**
   - Long-running tasks stream progress updates
   - Workers push incremental results to clients
   - Real-time progress monitoring

5. **Error Propagation**
   - Failures propagate through capability chains
   - Partial results preserved on failure
   - Retry with exponential backoff

6. **Concurrent Capability Usage**
   - Multiple workers using same resource capabilities
   - Race condition handling
   - Capability-based locking

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Supervisor                            │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │ TaskQueue   │  │ WorkerPool  │  │ Resources   │         │
│  │ Capability  │  │ Capability  │  │ Capability  │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────────────────────────────────────────┘
         │                  │                  │
         ▼                  ▼                  ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Client    │    │   Worker    │    │   Worker    │
│ (submits    │    │ (CPU pool)  │    │ (GPU pool)  │
│  tasks)     │    │             │    │             │
└─────────────┘    └─────────────┘    └─────────────┘
```

## Task Types

- **ComputeTask**: CPU-bound computation
- **GPUTask**: GPU-accelerated processing
- **IOTask**: File/network I/O operations
- **PipelineTask**: Chain of tasks with capability passing

## Running

1. Start the supervisor:
   ```bash
   uv run python examples/task-queue/server.py
   ```

2. Run the test suite:
   ```bash
   uv run python examples/task-queue/test_task_queue.py
   ```

3. Run interactive client:
   ```bash
   uv run python examples/task-queue/client.py
   ```
