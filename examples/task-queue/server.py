"""Distributed Task Queue Server.

Demonstrates advanced Cap'n Web patterns:
- Capability attenuation (workers get limited access)
- Capability revocation (supervisors can revoke access)
- Nested capability passing (tasks contain resource capabilities)
- Streaming results (progress updates)
- Error propagation through capability chains
- Concurrent capability usage

Run:
    uv run python examples/task-queue/server.py
"""

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from aiohttp import web

from capnweb import RpcError, RpcTarget, RpcStub, BidirectionalSession, create_stub
from capnweb.ws_transport import WebSocketServerTransport

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =============================================================================
# Enums and Data Models
# =============================================================================

class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class WorkerStatus(Enum):
    IDLE = "idle"
    BUSY = "busy"
    REVOKED = "revoked"


@dataclass
class TaskResult:
    task_id: str
    status: TaskStatus
    result: Any = None
    error: str | None = None
    progress: float = 0.0
    logs: list[str] = field(default_factory=list)


# =============================================================================
# Resource Capabilities (for nested capability passing)
# =============================================================================

@dataclass
class StorageCapability(RpcTarget):
    """Capability to access storage resources.
    
    Demonstrates NESTED CAPABILITY PASSING:
    - Tasks receive this capability
    - Workers use it to read/write data
    """
    
    storage_id: str
    read_only: bool = False
    revoked: bool = False
    data: dict[str, Any] = field(default_factory=dict)
    
    def _check_revoked(self) -> None:
        if self.revoked:
            raise RpcError.permission_denied("Storage capability has been revoked")
    
    async def call(self, method: str, args: list[Any]) -> Any:
        self._check_revoked()
        
        match method:
            case "read":
                return self._read(args[0])
            case "write":
                return self._write(args[0], args[1])
            case "list":
                return self._list()
            case "delete":
                return self._delete(args[0])
            case _:
                raise RpcError.not_found(f"Method '{method}' not found")
    
    async def get_property(self, name: str) -> Any:
        self._check_revoked()
        match name:
            case "id": return self.storage_id
            case "readOnly": return self.read_only
            case _: raise RpcError.not_found(f"Property '{name}' not found")
    
    def _read(self, key: str) -> Any:
        if key not in self.data:
            raise RpcError.not_found(f"Key '{key}' not found")
        return self.data[key]
    
    def _write(self, key: str, value: Any) -> dict[str, Any]:
        if self.read_only:
            raise RpcError.permission_denied("Storage is read-only")
        self.data[key] = value
        return {"success": True, "key": key}
    
    def _list(self) -> list[str]:
        return list(self.data.keys())
    
    def _delete(self, key: str) -> dict[str, Any]:
        if self.read_only:
            raise RpcError.permission_denied("Storage is read-only")
        if key not in self.data:
            raise RpcError.not_found(f"Key '{key}' not found")
        del self.data[key]
        return {"success": True, "key": key}


@dataclass
class GPUCapability(RpcTarget):
    """Capability to access GPU resources.
    
    Demonstrates CAPABILITY ATTENUATION:
    - Only GPU workers receive this capability
    - CPU workers cannot access GPU resources
    """
    
    gpu_id: str
    memory_limit_mb: int = 8192
    revoked: bool = False
    allocated_memory: int = 0
    
    def _check_revoked(self) -> None:
        if self.revoked:
            raise RpcError.permission_denied("GPU capability has been revoked")
    
    async def call(self, method: str, args: list[Any]) -> Any:
        self._check_revoked()
        
        match method:
            case "allocate":
                return self._allocate(args[0])
            case "free":
                return self._free(args[0])
            case "compute":
                return await self._compute(args[0], args[1])
            case "getInfo":
                return self._get_info()
            case _:
                raise RpcError.not_found(f"Method '{method}' not found")
    
    async def get_property(self, name: str) -> Any:
        self._check_revoked()
        match name:
            case "id": return self.gpu_id
            case "memoryLimit": return self.memory_limit_mb
            case "allocatedMemory": return self.allocated_memory
            case _: raise RpcError.not_found(f"Property '{name}' not found")
    
    def _allocate(self, size_mb: int) -> dict[str, Any]:
        if self.allocated_memory + size_mb > self.memory_limit_mb:
            raise RpcError.bad_request(
                f"Not enough GPU memory: {self.memory_limit_mb - self.allocated_memory}MB available"
            )
        self.allocated_memory += size_mb
        return {"success": True, "allocated": size_mb, "total": self.allocated_memory}
    
    def _free(self, size_mb: int) -> dict[str, Any]:
        self.allocated_memory = max(0, self.allocated_memory - size_mb)
        return {"success": True, "freed": size_mb, "total": self.allocated_memory}
    
    async def _compute(self, operation: str, data: Any) -> dict[str, Any]:
        # Simulate GPU computation
        await asyncio.sleep(0.1)
        
        if operation == "matrix_multiply":
            # Simulate matrix multiplication result
            return {"result": f"GPU computed: {operation}", "data": data}
        elif operation == "neural_forward":
            return {"result": f"Neural network forward pass", "output": [0.1, 0.9]}
        else:
            return {"result": f"GPU operation: {operation}", "data": data}
    
    def _get_info(self) -> dict[str, Any]:
        return {
            "id": self.gpu_id,
            "memoryLimit": self.memory_limit_mb,
            "allocatedMemory": self.allocated_memory,
            "available": self.memory_limit_mb - self.allocated_memory,
        }


@dataclass
class NetworkCapability(RpcTarget):
    """Capability to make network requests.
    
    Demonstrates CAPABILITY ATTENUATION:
    - Can be restricted to specific hosts
    - Rate limiting per capability
    """
    
    net_id: str
    allowed_hosts: list[str] = field(default_factory=lambda: ["*"])
    rate_limit: int = 100  # requests per minute
    revoked: bool = False
    request_count: int = 0
    
    def _check_revoked(self) -> None:
        if self.revoked:
            raise RpcError.permission_denied("Network capability has been revoked")
    
    async def call(self, method: str, args: list[Any]) -> Any:
        self._check_revoked()
        
        match method:
            case "fetch":
                return await self._fetch(args[0])
            case "post":
                return await self._post(args[0], args[1])
            case "getStats":
                return self._get_stats()
            case _:
                raise RpcError.not_found(f"Method '{method}' not found")
    
    async def get_property(self, name: str) -> Any:
        self._check_revoked()
        match name:
            case "id": return self.net_id
            case "rateLimit": return self.rate_limit
            case _: raise RpcError.not_found(f"Property '{name}' not found")
    
    def _check_host(self, url: str) -> None:
        if "*" in self.allowed_hosts:
            return
        # Simple host check
        for host in self.allowed_hosts:
            if host in url:
                return
        raise RpcError.permission_denied(f"Host not allowed: {url}")
    
    async def _fetch(self, url: str) -> dict[str, Any]:
        self._check_host(url)
        self.request_count += 1
        # Simulate network request
        await asyncio.sleep(0.05)
        return {"status": 200, "url": url, "body": f"Response from {url}"}
    
    async def _post(self, url: str, data: Any) -> dict[str, Any]:
        self._check_host(url)
        self.request_count += 1
        await asyncio.sleep(0.05)
        return {"status": 201, "url": url, "body": f"Posted to {url}"}
    
    def _get_stats(self) -> dict[str, Any]:
        return {
            "id": self.net_id,
            "requestCount": self.request_count,
            "rateLimit": self.rate_limit,
        }


# =============================================================================
# Task Capability
# =============================================================================

@dataclass
class TaskCapability(RpcTarget):
    """Capability representing a task.
    
    Demonstrates NESTED CAPABILITY PASSING:
    - Contains resource capabilities (storage, GPU, network)
    - Workers receive this and use nested capabilities
    """
    
    task_id: str
    task_type: str
    params: dict[str, Any]
    storage: StorageCapability | None = None
    gpu: GPUCapability | None = None
    network: NetworkCapability | None = None
    status: TaskStatus = TaskStatus.PENDING
    progress: float = 0.0
    result: Any = None
    error: str | None = None
    logs: list[str] = field(default_factory=list)
    progress_callback: RpcStub | None = None
    cancelled: bool = False
    
    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "getInfo":
                return self._get_info()
            case "getStorage":
                return self._get_storage()
            case "getGPU":
                return self._get_gpu()
            case "getNetwork":
                return self._get_network()
            case "setProgress":
                return await self._set_progress(args[0])
            case "addLog":
                return self._add_log(args[0])
            case "complete":
                return await self._complete(args[0])
            case "fail":
                return await self._fail(args[0])
            case "cancel":
                return self._cancel()
            case "setProgressCallback":
                return self._set_progress_callback(args[0])
            case "isCancelled":
                return self.cancelled
            case _:
                raise RpcError.not_found(f"Method '{method}' not found")
    
    async def get_property(self, name: str) -> Any:
        match name:
            case "id": return self.task_id
            case "type": return self.task_type
            case "status": return self.status.value
            case "progress": return self.progress
            case _: raise RpcError.not_found(f"Property '{name}' not found")
    
    def _get_info(self) -> dict[str, Any]:
        return {
            "id": self.task_id,
            "type": self.task_type,
            "params": self.params,
            "status": self.status.value,
            "progress": self.progress,
            "result": self.result,
            "error": self.error,
            "logs": self.logs[-10:],  # Last 10 logs
        }
    
    def _get_storage(self) -> StorageCapability:
        if not self.storage:
            raise RpcError.not_found("Task has no storage capability")
        return self.storage
    
    def _get_gpu(self) -> GPUCapability:
        if not self.gpu:
            raise RpcError.not_found("Task has no GPU capability")
        return self.gpu
    
    def _get_network(self) -> NetworkCapability:
        if not self.network:
            raise RpcError.not_found("Task has no network capability")
        return self.network
    
    async def _set_progress(self, progress: float) -> dict[str, Any]:
        self.progress = min(1.0, max(0.0, progress))
        self.status = TaskStatus.RUNNING
        
        # STREAMING RESULTS: Push progress to callback
        if self.progress_callback:
            try:
                await self.progress_callback.onProgress({
                    "taskId": self.task_id,
                    "progress": self.progress,
                    "status": self.status.value,
                })
            except Exception as e:
                logger.warning(f"Failed to send progress: {e}")
        
        return {"success": True, "progress": self.progress}
    
    def _add_log(self, message: str) -> dict[str, Any]:
        timestamp = datetime.now().isoformat()
        self.logs.append(f"[{timestamp}] {message}")
        return {"success": True}
    
    async def _complete(self, result: Any) -> dict[str, Any]:
        self.status = TaskStatus.COMPLETED
        self.progress = 1.0
        self.result = result
        
        if self.progress_callback:
            try:
                await self.progress_callback.onComplete({
                    "taskId": self.task_id,
                    "result": result,
                })
            except Exception as e:
                logger.warning(f"Failed to send completion: {e}")
        
        return {"success": True}
    
    async def _fail(self, error: str) -> dict[str, Any]:
        self.status = TaskStatus.FAILED
        self.error = error
        
        if self.progress_callback:
            try:
                await self.progress_callback.onError({
                    "taskId": self.task_id,
                    "error": error,
                })
            except Exception as e:
                logger.warning(f"Failed to send error: {e}")
        
        return {"success": True}
    
    def _cancel(self) -> dict[str, Any]:
        self.cancelled = True
        self.status = TaskStatus.CANCELLED
        return {"success": True}
    
    def _set_progress_callback(self, callback: RpcStub) -> dict[str, Any]:
        self.progress_callback = callback
        return {"success": True}


# =============================================================================
# Worker Capability
# =============================================================================

@dataclass
class WorkerCapability(RpcTarget):
    """Capability representing a worker.
    
    Demonstrates CAPABILITY REVOCATION:
    - Supervisors can revoke worker access
    - Revoked workers cannot execute tasks
    """
    
    worker_id: str
    worker_type: str  # "cpu" or "gpu"
    status: WorkerStatus = WorkerStatus.IDLE
    revoked: bool = False
    current_task: TaskCapability | None = None
    tasks_completed: int = 0
    tasks_failed: int = 0
    
    def _check_revoked(self) -> None:
        if self.revoked:
            raise RpcError.permission_denied("Worker capability has been revoked")
    
    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "getInfo":
                return self._get_info()
            case "execute":
                return await self._execute(args[0])
            case "revoke":
                return self._revoke()
            case "isRevoked":
                return self.revoked
            case _:
                raise RpcError.not_found(f"Method '{method}' not found")
    
    async def get_property(self, name: str) -> Any:
        match name:
            case "id": return self.worker_id
            case "type": return self.worker_type
            case "status": return self.status.value
            case _: raise RpcError.not_found(f"Property '{name}' not found")
    
    def _get_info(self) -> dict[str, Any]:
        return {
            "id": self.worker_id,
            "type": self.worker_type,
            "status": self.status.value,
            "revoked": self.revoked,
            "tasksCompleted": self.tasks_completed,
            "tasksFailed": self.tasks_failed,
        }
    
    async def _execute(self, task: RpcStub) -> dict[str, Any]:
        """Execute a task using the task capability.
        
        Demonstrates NESTED CAPABILITY PASSING:
        - Worker receives task capability
        - Uses nested storage/GPU/network capabilities
        """
        self._check_revoked()
        
        if self.status == WorkerStatus.BUSY:
            raise RpcError.bad_request("Worker is busy")
        
        self.status = WorkerStatus.BUSY
        
        try:
            # Get task info
            task_info = await task.getInfo()
            task_type = task_info["type"]
            params = task_info["params"]
            
            await task.addLog(f"Worker {self.worker_id} started execution")
            await task.setProgress(0.1)
            
            # Check for cancellation
            if await task.isCancelled():
                raise RpcError.bad_request("Task was cancelled")
            
            # Execute based on task type
            if task_type == "compute":
                result = await self._execute_compute(task, params)
            elif task_type == "gpu":
                result = await self._execute_gpu(task, params)
            elif task_type == "io":
                result = await self._execute_io(task, params)
            elif task_type == "pipeline":
                result = await self._execute_pipeline(task, params)
            else:
                raise RpcError.bad_request(f"Unknown task type: {task_type}")
            
            await task.complete(result)
            self.tasks_completed += 1
            
            return {"success": True, "result": result}
            
        except RpcError as e:
            self.tasks_failed += 1
            await task.fail(e.message)
            raise
        except Exception as e:
            self.tasks_failed += 1
            await task.fail(str(e))
            raise RpcError.internal(str(e))
        finally:
            self.status = WorkerStatus.IDLE
    
    async def _execute_compute(self, task: RpcStub, params: dict[str, Any]) -> Any:
        """Execute CPU-bound computation."""
        iterations = params.get("iterations", 10)
        
        result = 0
        for i in range(iterations):
            if await task.isCancelled():
                raise RpcError.bad_request("Task cancelled during execution")
            
            await task.setProgress(0.1 + 0.8 * (i + 1) / iterations)
            await task.addLog(f"Iteration {i + 1}/{iterations}")
            
            # Simulate computation
            await asyncio.sleep(0.05)
            result += i * params.get("multiplier", 1)
        
        return {"computed": result, "iterations": iterations}
    
    async def _execute_gpu(self, task: RpcStub, params: dict[str, Any]) -> Any:
        """Execute GPU-accelerated task.
        
        Demonstrates CAPABILITY ATTENUATION:
        - Only GPU workers should have GPU capability
        """
        if self.worker_type != "gpu":
            raise RpcError.permission_denied("CPU worker cannot execute GPU tasks")
        
        # Get GPU capability from task
        gpu = await task.getGPU()
        
        await task.setProgress(0.2)
        await task.addLog("Allocating GPU memory")
        
        # Allocate GPU memory
        memory_needed = params.get("memory_mb", 512)
        await gpu.allocate(memory_needed)
        
        try:
            await task.setProgress(0.5)
            await task.addLog("Running GPU computation")
            
            # Run GPU computation
            operation = params.get("operation", "matrix_multiply")
            result = await gpu.compute(operation, params.get("data", {}))
            
            await task.setProgress(0.9)
            return result
            
        finally:
            # Always free GPU memory
            await gpu.free(memory_needed)
    
    async def _execute_io(self, task: RpcStub, params: dict[str, Any]) -> Any:
        """Execute I/O task using storage and network capabilities."""
        await task.setProgress(0.2)
        
        results = {}
        
        # Use storage if available
        if params.get("use_storage"):
            storage = await task.getStorage()
            await task.addLog("Accessing storage")
            
            key = params.get("storage_key", "test_key")
            
            # If read_only, just read (may fail if key doesn't exist)
            if params.get("read_only"):
                read_value = await storage.read(key)
                results["storage"] = {"key": key, "value": read_value}
            else:
                # Write some data then read it back
                value = params.get("storage_value", "test_value")
                await storage.write(key, value)
                read_value = await storage.read(key)
                results["storage"] = {"key": key, "value": read_value}
            
            await task.setProgress(0.5)
        
        # Use network if available
        if params.get("use_network"):
            network = await task.getNetwork()
            await task.addLog("Making network request")
            
            url = params.get("url", "https://example.com")
            response = await network.fetch(url)
            results["network"] = response
            
            await task.setProgress(0.8)
        
        return results
    
    async def _execute_pipeline(self, task: RpcStub, params: dict[str, Any]) -> Any:
        """Execute a pipeline of operations.
        
        Demonstrates COMPLEX CAPABILITY CHAINS:
        - Multiple nested capabilities used in sequence
        """
        stages = params.get("stages", [])
        results = []
        
        for i, stage in enumerate(stages):
            if await task.isCancelled():
                raise RpcError.bad_request("Pipeline cancelled")
            
            progress = 0.1 + 0.8 * (i + 1) / len(stages)
            await task.setProgress(progress)
            await task.addLog(f"Pipeline stage {i + 1}: {stage['type']}")
            
            if stage["type"] == "storage_read":
                storage = await task.getStorage()
                value = await storage.read(stage["key"])
                results.append({"stage": i, "type": "storage_read", "value": value})
                
            elif stage["type"] == "storage_write":
                storage = await task.getStorage()
                await storage.write(stage["key"], stage["value"])
                results.append({"stage": i, "type": "storage_write", "key": stage["key"]})
                
            elif stage["type"] == "network_fetch":
                network = await task.getNetwork()
                response = await network.fetch(stage["url"])
                results.append({"stage": i, "type": "network_fetch", "response": response})
                
            elif stage["type"] == "gpu_compute":
                if self.worker_type != "gpu":
                    raise RpcError.permission_denied("GPU stage requires GPU worker")
                gpu = await task.getGPU()
                result = await gpu.compute(stage["operation"], stage.get("data", {}))
                results.append({"stage": i, "type": "gpu_compute", "result": result})
            
            await asyncio.sleep(0.05)
        
        return {"pipeline_results": results, "stages_completed": len(stages)}
    
    def _revoke(self) -> dict[str, Any]:
        """Revoke this worker's capability."""
        self.revoked = True
        self.status = WorkerStatus.REVOKED
        return {"success": True, "workerId": self.worker_id}


# =============================================================================
# Task Queue Capability
# =============================================================================

@dataclass
class TaskQueueCapability(RpcTarget):
    """Capability to submit and manage tasks."""
    
    queue_id: str
    tasks: dict[str, TaskCapability] = field(default_factory=dict)
    shared_storage: StorageCapability = field(default_factory=lambda: StorageCapability("shared"))
    shared_gpu: GPUCapability = field(default_factory=lambda: GPUCapability("gpu0"))
    shared_network: NetworkCapability = field(default_factory=lambda: NetworkCapability("net0"))
    
    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "submit":
                if len(args) < 2:
                    raise RpcError.bad_request(f"submit requires 2 args, got {len(args)}: {args}")
                return self._submit(args[0], args[1])
            case "getTask":
                return self._get_task(args[0])
            case "listTasks":
                return self._list_tasks()
            case "cancelTask":
                return self._cancel_task(args[0])
            case "getSharedStorage":
                return self.shared_storage
            case "getSharedGPU":
                return self.shared_gpu
            case "getSharedNetwork":
                return self.shared_network
            case _:
                raise RpcError.not_found(f"Method '{method}' not found")
    
    async def get_property(self, name: str) -> Any:
        match name:
            case "id": return self.queue_id
            case "taskCount": return len(self.tasks)
            case _: raise RpcError.not_found(f"Property '{name}' not found")
    
    def _submit(self, task_type: str, params: dict[str, Any]) -> TaskCapability:
        """Submit a new task and return its capability."""
        task_id = f"task_{uuid.uuid4().hex[:8]}"
        
        # Create task with appropriate resource capabilities
        task = TaskCapability(
            task_id=task_id,
            task_type=task_type,
            params=params,
            storage=self.shared_storage if params.get("needs_storage") else None,
            gpu=self.shared_gpu if params.get("needs_gpu") else None,
            network=self.shared_network if params.get("needs_network") else None,
        )
        
        self.tasks[task_id] = task
        return task
    
    def _get_task(self, task_id: str) -> TaskCapability:
        if task_id not in self.tasks:
            raise RpcError.not_found(f"Task '{task_id}' not found")
        return self.tasks[task_id]
    
    def _list_tasks(self) -> list[dict[str, Any]]:
        return [
            {
                "id": t.task_id,
                "type": t.task_type,
                "status": t.status.value,
                "progress": t.progress,
            }
            for t in self.tasks.values()
        ]
    
    def _cancel_task(self, task_id: str) -> dict[str, Any]:
        if task_id not in self.tasks:
            raise RpcError.not_found(f"Task '{task_id}' not found")
        self.tasks[task_id].cancelled = True
        self.tasks[task_id].status = TaskStatus.CANCELLED
        return {"success": True, "taskId": task_id}


# =============================================================================
# Worker Pool Capability
# =============================================================================

@dataclass
class WorkerPoolCapability(RpcTarget):
    """Capability to manage worker pools."""
    
    pool_id: str
    workers: dict[str, WorkerCapability] = field(default_factory=dict)
    
    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "createWorker":
                return self._create_worker(args[0])
            case "getWorker":
                return self._get_worker(args[0])
            case "listWorkers":
                return self._list_workers()
            case "revokeWorker":
                return self._revoke_worker(args[0])
            case "getIdleWorker":
                return self._get_idle_worker(args[0] if args else None)
            case _:
                raise RpcError.not_found(f"Method '{method}' not found")
    
    async def get_property(self, name: str) -> Any:
        match name:
            case "id": return self.pool_id
            case "workerCount": return len(self.workers)
            case _: raise RpcError.not_found(f"Property '{name}' not found")
    
    def _create_worker(self, worker_type: str) -> WorkerCapability:
        """Create a new worker with attenuated capabilities."""
        worker_id = f"worker_{uuid.uuid4().hex[:8]}"
        
        worker = WorkerCapability(
            worker_id=worker_id,
            worker_type=worker_type,
        )
        
        self.workers[worker_id] = worker
        return worker
    
    def _get_worker(self, worker_id: str) -> WorkerCapability:
        if worker_id not in self.workers:
            raise RpcError.not_found(f"Worker '{worker_id}' not found")
        return self.workers[worker_id]
    
    def _list_workers(self) -> list[dict[str, Any]]:
        return [
            {
                "id": w.worker_id,
                "type": w.worker_type,
                "status": w.status.value,
                "revoked": w.revoked,
            }
            for w in self.workers.values()
        ]
    
    def _revoke_worker(self, worker_id: str) -> dict[str, Any]:
        """Revoke a worker's capability.
        
        Demonstrates CAPABILITY REVOCATION:
        - Worker can no longer execute tasks
        - All future calls will fail
        """
        if worker_id not in self.workers:
            raise RpcError.not_found(f"Worker '{worker_id}' not found")
        
        worker = self.workers[worker_id]
        worker.revoked = True
        worker.status = WorkerStatus.REVOKED
        
        return {"success": True, "workerId": worker_id}
    
    def _get_idle_worker(self, worker_type: str | None) -> WorkerCapability:
        """Get an idle worker, optionally of a specific type."""
        for worker in self.workers.values():
            if worker.status == WorkerStatus.IDLE and not worker.revoked:
                if worker_type is None or worker.worker_type == worker_type:
                    return worker
        raise RpcError.not_found("No idle workers available")


# =============================================================================
# Supervisor Gateway
# =============================================================================

class SupervisorGateway(RpcTarget):
    """Main gateway for the task queue system."""
    
    def __init__(self):
        self.task_queue = TaskQueueCapability(queue_id="main_queue")
        self.worker_pool = WorkerPoolCapability(pool_id="main_pool")
        
        # Pre-create some workers
        self.worker_pool._create_worker("cpu")
        self.worker_pool._create_worker("cpu")
        self.worker_pool._create_worker("gpu")
    
    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "getTaskQueue":
                return self.task_queue
            case "getWorkerPool":
                return self.worker_pool
            case "submitAndExecute":
                return await self._submit_and_execute(args[0], args[1])
            case _:
                raise RpcError.not_found(f"Method '{method}' not found")
    
    async def get_property(self, name: str) -> Any:
        raise RpcError.not_found(f"Property '{name}' not found")
    
    async def _submit_and_execute(self, task_type: str, params: dict[str, Any]) -> dict[str, Any]:
        """Convenience method to submit a task and execute it immediately."""
        # Submit task
        task = self.task_queue._submit(task_type, params)
        
        # Find appropriate worker
        worker_type = "gpu" if task_type == "gpu" else "cpu"
        try:
            worker = self.worker_pool._get_idle_worker(worker_type)
        except RpcError:
            # Fall back to any idle worker for non-GPU tasks
            if worker_type != "gpu":
                worker = self.worker_pool._get_idle_worker(None)
            else:
                raise
        
        # Execute
        task_stub = create_stub(task)
        result = await worker._execute(task_stub)
        
        return {
            "taskId": task.task_id,
            "workerId": worker.worker_id,
            "result": result,
        }


# =============================================================================
# WebSocket Handler
# =============================================================================

gateway = SupervisorGateway()


async def handle_websocket(request: web.Request) -> web.WebSocketResponse:
    """Handle WebSocket connections."""
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    
    transport = WebSocketServerTransport(ws)
    session = BidirectionalSession(transport, gateway)
    session.start()
    
    logger.info("Client connected")
    
    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                transport.feed_message(msg.data)
            elif msg.type == web.WSMsgType.BINARY:
                transport.feed_message(msg.data.decode("utf-8"))
            elif msg.type == web.WSMsgType.ERROR:
                transport.set_error(ws.exception() or Exception("WebSocket error"))
                break
    except Exception as e:
        logger.error("WebSocket error: %s", e)
        transport.set_error(e)
    finally:
        transport.set_closed()
        logger.info("Client disconnected")
    
    return ws


async def main() -> None:
    """Run the task queue server."""
    app = web.Application()
    app.router.add_get("/rpc/ws", handle_websocket)
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    site = web.TCPSite(runner, "127.0.0.1", 8080)
    await site.start()
    
    print("🚀 Task Queue Server running on http://127.0.0.1:8080")
    print("   WebSocket endpoint: ws://127.0.0.1:8080/rpc/ws")
    print()
    print("Pre-created workers:")
    for w in gateway.worker_pool.workers.values():
        print(f"  - {w.worker_id} ({w.worker_type})")
    print()
    print("Run tests with: uv run python examples/task-queue/test_task_queue.py")
    print("Press Ctrl+C to stop")
    
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
