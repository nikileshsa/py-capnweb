# Production Features Examples

This directory contains 10 real-world examples that demonstrate and validate all production-grade features of py-capnweb.

## Features Validated

Each example exercises specific features with assertions to prove correctness:

| Example | Features Validated |
|---------|-------------------|
| 1. `bidirectional_chat/` | BidirectionalSession, bidirectional calls, onBroken callbacks |
| 2. `task_queue/` | drain() for graceful shutdown, pending operation tracking |
| 3. `data_pipeline/` | MapBuilder for batch processing, map instructions |
| 4. `pubsub/` | Release messages, reference counting, memory management |
| 5. `file_transfer/` | Progress callbacks, concurrent streams, large data |
| 6. `game_state/` | getStats() monitoring, real-time state sync |
| 7. `distributed_cache/` | Reference counting validation, export lifecycle |
| 8. `streaming_analytics/` | Concurrent bidirectional calls, high throughput |
| 9. `service_mesh/` | Microservice orchestration, capability passing |
| 10. `resilient_rpc/` | Connection recovery, onBroken handling, reconnection |

## Running Examples

Each example can be run as a standalone test:

```bash
# Run all production feature tests
uv run pytest examples/production_features/ -v

# Run a specific example
uv run pytest examples/production_features/test_01_bidirectional_chat.py -v
```

## Key Assertions

Each test validates specific behaviors:

### BidirectionalSession
- Server can call client methods ✓
- Client can call server methods ✓
- Messages flow in both directions simultaneously ✓

### Memory Management
- Release messages sent when imports resolve ✓
- Reference counts track correctly ✓
- No memory leaks after session close ✓

### Graceful Shutdown
- drain() waits for pending operations ✓
- All callbacks complete before shutdown ✓
- Stats show zero pending after drain ✓

### Connection Handling
- onBroken callbacks fire on disconnect ✓
- Callbacks transfer to resolution ✓
- Clean error propagation ✓
