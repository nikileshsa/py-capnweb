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

### Step 1: Start the server

```bash
cd capnweb-python
uv run python examples/task-queue/server.py
```

**Expected output:**
```
INFO:__main__:Task Queue Server running on ws://127.0.0.1:8080/rpc/ws
INFO:__main__:Run test: uv run python examples/task-queue/test_task_queue.py
```

### Step 2: Run the test suite (new terminal)

```bash
cd capnweb-python
uv run python examples/task-queue/test_task_queue.py
```

**Expected output:**
```
============================================================
  DISTRIBUTED TASK QUEUE - COMPREHENSIVE TEST SUITE
============================================================

============================================================
TEST 1: Basic Task Submission and Execution
============================================================
  ✓ Task queue capability obtained
  ✓ Task submitted and capability returned
  ✓ Task info is correct
  ✓ Worker executed task successfully
  ✓ Task status updated to completed
  ✅ TEST 1 PASSED

============================================================
TEST 2: Nested Capability Passing
============================================================
  ✓ Task has storage capability
  ✓ Task has network capability
  ✓ Worker used storage capability from task
  ✓ Worker used network capability from task
  ✓ Nested capabilities passed correctly
  ✅ TEST 2 PASSED
...
```
