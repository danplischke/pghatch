"""
Core benchmarking utilities and infrastructure for pghatch API performance testing.
"""

import asyncio
import time
import statistics
import psutil
import resource
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable, Any
from contextlib import asynccontextmanager
import httpx


@dataclass
class BenchmarkMetrics:
    """Container for benchmark performance metrics."""
    
    # Response Time metrics (in seconds)
    response_times: List[float] = field(default_factory=list)
    avg_response_time: float = 0.0
    min_response_time: float = 0.0
    max_response_time: float = 0.0
    p95_response_time: float = 0.0
    p99_response_time: float = 0.0
    
    # Throughput metrics
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    requests_per_second: float = 0.0
    
    # Error metrics
    error_rate: float = 0.0
    success_rate: float = 0.0
    error_counts: Dict[int, int] = field(default_factory=dict)
    
    # Latency metrics (TTFB - Time To First Byte)
    ttfb_times: List[float] = field(default_factory=list)
    avg_ttfb: float = 0.0
    min_ttfb: float = 0.0
    max_ttfb: float = 0.0
    
    # Resource utilization
    cpu_usage_samples: List[float] = field(default_factory=list)
    memory_usage_samples: List[float] = field(default_factory=list)  # in MB
    avg_cpu_usage: float = 0.0
    max_cpu_usage: float = 0.0
    avg_memory_usage: float = 0.0
    max_memory_usage: float = 0.0
    
    # Concurrency metrics
    concurrent_requests: int = 0
    concurrent_success_rate: float = 0.0
    
    # Data transfer metrics
    total_bytes_sent: int = 0
    total_bytes_received: int = 0
    avg_transfer_speed_mbps: float = 0.0
    
    # Test duration
    test_duration: float = 0.0
    
    def calculate_derived_metrics(self):
        """Calculate derived metrics from collected data."""
        if self.response_times:
            self.avg_response_time = statistics.mean(self.response_times)
            self.min_response_time = min(self.response_times)
            self.max_response_time = max(self.response_times)
            
            sorted_times = sorted(self.response_times)
            n = len(sorted_times)
            self.p95_response_time = sorted_times[int(0.95 * n)] if n > 0 else 0.0
            self.p99_response_time = sorted_times[int(0.99 * n)] if n > 0 else 0.0
        
        if self.ttfb_times:
            self.avg_ttfb = statistics.mean(self.ttfb_times)
            self.min_ttfb = min(self.ttfb_times)
            self.max_ttfb = max(self.ttfb_times)
        
        if self.total_requests > 0:
            self.success_rate = (self.successful_requests / self.total_requests) * 100
            self.error_rate = (self.failed_requests / self.total_requests) * 100
            
            if self.test_duration > 0:
                self.requests_per_second = self.total_requests / self.test_duration
        
        if self.cpu_usage_samples:
            self.avg_cpu_usage = statistics.mean(self.cpu_usage_samples)
            self.max_cpu_usage = max(self.cpu_usage_samples)
        
        if self.memory_usage_samples:
            self.avg_memory_usage = statistics.mean(self.memory_usage_samples)
            self.max_memory_usage = max(self.memory_usage_samples)
        
        if self.test_duration > 0 and (self.total_bytes_sent + self.total_bytes_received) > 0:
            total_bytes = self.total_bytes_sent + self.total_bytes_received
            self.avg_transfer_speed_mbps = (total_bytes * 8) / (self.test_duration * 1_000_000)


class ResourceMonitor:
    """Monitor system resource usage during benchmarks."""
    
    def __init__(self, sample_interval: float = 0.5):
        self.sample_interval = sample_interval
        self._monitoring = False
        self._cpu_samples = []
        self._memory_samples = []
        self._monitor_task = None
    
    async def start_monitoring(self):
        """Start resource monitoring."""
        self._monitoring = True
        self._cpu_samples.clear()
        self._memory_samples.clear()
        self._monitor_task = asyncio.create_task(self._monitor_resources())
    
    async def stop_monitoring(self):
        """Stop resource monitoring."""
        self._monitoring = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
    
    async def _monitor_resources(self):
        """Monitor resources at regular intervals."""
        while self._monitoring:
            try:
                # CPU usage (percentage)
                cpu_percent = psutil.cpu_percent(interval=None)
                self._cpu_samples.append(cpu_percent)
                
                # Memory usage (in MB)
                memory_info = psutil.virtual_memory()
                memory_mb = memory_info.used / (1024 * 1024)
                self._memory_samples.append(memory_mb)
                
                await asyncio.sleep(self.sample_interval)
            except Exception:
                # Continue monitoring even if we can't get resource info
                await asyncio.sleep(self.sample_interval)
    
    def get_samples(self) -> tuple[List[float], List[float]]:
        """Get collected CPU and memory samples."""
        return self._cpu_samples.copy(), self._memory_samples.copy()


class BenchmarkRunner:
    """Main benchmark runner for executing performance tests."""
    
    def __init__(self, base_url: str, timeout: float = 30.0):
        self.base_url = base_url
        self.timeout = timeout
        self.resource_monitor = ResourceMonitor()
    
    @asynccontextmanager
    async def client_session(self):
        """Create an HTTP client session for benchmarking."""
        async with httpx.AsyncClient(
            base_url=self.base_url,
            timeout=httpx.Timeout(self.timeout),
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20)
        ) as client:
            yield client
    
    async def single_request_benchmark(
        self,
        endpoint: str,
        method: str = "POST",
        json_data: Optional[Dict] = None,
        headers: Optional[Dict] = None
    ) -> tuple[float, float, int, int, int]:
        """
        Benchmark a single request.
        
        Returns:
            tuple: (response_time, ttfb, status_code, bytes_sent, bytes_received)
        """
        async with self.client_session() as client:
            request_data = json_data or {}
            request_headers = headers or {}
            
            start_time = time.perf_counter()
            ttfb_recorded = False
            ttfb = 0.0
            
            try:
                if method.upper() == "POST":
                    response = await client.post(endpoint, json=request_data, headers=request_headers)
                elif method.upper() == "GET":
                    response = await client.get(endpoint, headers=request_headers)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
                
                # Record TTFB (approximate - when response starts)
                if not ttfb_recorded:
                    ttfb = time.perf_counter() - start_time
                    ttfb_recorded = True
                
                # Read response content to measure full response time
                content = response.content
                end_time = time.perf_counter()
                
                response_time = end_time - start_time
                bytes_sent = len(response.request.content) if response.request.content else 0
                bytes_received = len(content)
                
                return response_time, ttfb, response.status_code, bytes_sent, bytes_received
                
            except Exception as e:
                end_time = time.perf_counter()
                response_time = end_time - start_time
                # Return error with status code 0 to indicate failure
                return response_time, ttfb, 0, 0, 0
    
    async def throughput_benchmark(
        self,
        endpoint: str,
        duration_seconds: float,
        method: str = "POST",
        json_data: Optional[Dict] = None,
        headers: Optional[Dict] = None
    ) -> BenchmarkMetrics:
        """
        Run throughput benchmark for specified duration.
        
        Args:
            endpoint: API endpoint to test
            duration_seconds: How long to run the test
            method: HTTP method
            json_data: Request payload
            headers: Request headers
            
        Returns:
            BenchmarkMetrics with collected performance data
        """
        metrics = BenchmarkMetrics()
        start_time = time.perf_counter()
        end_time = start_time + duration_seconds
        
        # Start resource monitoring
        await self.resource_monitor.start_monitoring()
        
        try:
            while time.perf_counter() < end_time:
                response_time, ttfb, status_code, bytes_sent, bytes_received = await self.single_request_benchmark(
                    endpoint, method, json_data, headers
                )
                
                metrics.total_requests += 1
                metrics.response_times.append(response_time)
                metrics.ttfb_times.append(ttfb)
                metrics.total_bytes_sent += bytes_sent
                metrics.total_bytes_received += bytes_received
                
                if status_code == 0:  # Error case
                    metrics.failed_requests += 1
                elif 200 <= status_code < 300:  # Success
                    metrics.successful_requests += 1
                else:  # Other status codes
                    metrics.failed_requests += 1
                    metrics.error_counts[status_code] = metrics.error_counts.get(status_code, 0) + 1
        
        finally:
            # Stop resource monitoring
            await self.resource_monitor.stop_monitoring()
            cpu_samples, memory_samples = self.resource_monitor.get_samples()
            metrics.cpu_usage_samples = cpu_samples
            metrics.memory_usage_samples = memory_samples
        
        metrics.test_duration = time.perf_counter() - start_time
        metrics.calculate_derived_metrics()
        
        return metrics
    
    async def concurrency_benchmark(
        self,
        endpoint: str,
        concurrent_requests: int,
        method: str = "POST",
        json_data: Optional[Dict] = None,
        headers: Optional[Dict] = None
    ) -> BenchmarkMetrics:
        """
        Run concurrency benchmark with multiple simultaneous requests.
        
        Args:
            endpoint: API endpoint to test
            concurrent_requests: Number of concurrent requests
            method: HTTP method
            json_data: Request payload
            headers: Request headers
            
        Returns:
            BenchmarkMetrics with collected performance data
        """
        metrics = BenchmarkMetrics()
        metrics.concurrent_requests = concurrent_requests
        
        start_time = time.perf_counter()
        
        # Start resource monitoring
        await self.resource_monitor.start_monitoring()
        
        try:
            # Create tasks for concurrent requests
            tasks = []
            for _ in range(concurrent_requests):
                task = asyncio.create_task(
                    self.single_request_benchmark(endpoint, method, json_data, headers)
                )
                tasks.append(task)
            
            # Wait for all requests to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for result in results:
                if isinstance(result, Exception):
                    metrics.failed_requests += 1
                    metrics.response_times.append(0.0)
                    metrics.ttfb_times.append(0.0)
                else:
                    response_time, ttfb, status_code, bytes_sent, bytes_received = result
                    metrics.total_requests += 1
                    metrics.response_times.append(response_time)
                    metrics.ttfb_times.append(ttfb)
                    metrics.total_bytes_sent += bytes_sent
                    metrics.total_bytes_received += bytes_received
                    
                    if status_code == 0:  # Error case
                        metrics.failed_requests += 1
                    elif 200 <= status_code < 300:  # Success
                        metrics.successful_requests += 1
                    else:  # Other status codes
                        metrics.failed_requests += 1
                        metrics.error_counts[status_code] = metrics.error_counts.get(status_code, 0) + 1
        
        finally:
            # Stop resource monitoring
            await self.resource_monitor.stop_monitoring()
            cpu_samples, memory_samples = self.resource_monitor.get_samples()
            metrics.cpu_usage_samples = cpu_samples
            metrics.memory_usage_samples = memory_samples
        
        metrics.test_duration = time.perf_counter() - start_time
        metrics.concurrent_success_rate = (metrics.successful_requests / concurrent_requests) * 100 if concurrent_requests > 0 else 0.0
        metrics.calculate_derived_metrics()
        
        return metrics


def format_benchmark_results(metrics: BenchmarkMetrics) -> str:
    """Format benchmark results for display."""
    return f"""
Benchmark Results:
==================

Response Time Metrics:
- Average: {metrics.avg_response_time:.4f}s
- Min: {metrics.min_response_time:.4f}s
- Max: {metrics.max_response_time:.4f}s
- 95th percentile: {metrics.p95_response_time:.4f}s
- 99th percentile: {metrics.p99_response_time:.4f}s

Throughput Metrics:
- Total requests: {metrics.total_requests}
- Successful requests: {metrics.successful_requests}
- Failed requests: {metrics.failed_requests}
- Requests per second: {metrics.requests_per_second:.2f}

Reliability Metrics:
- Success rate: {metrics.success_rate:.2f}%
- Error rate: {metrics.error_rate:.2f}%

Latency Metrics (TTFB):
- Average TTFB: {metrics.avg_ttfb:.4f}s
- Min TTFB: {metrics.min_ttfb:.4f}s
- Max TTFB: {metrics.max_ttfb:.4f}s

Resource Utilization:
- Average CPU usage: {metrics.avg_cpu_usage:.2f}%
- Max CPU usage: {metrics.max_cpu_usage:.2f}%
- Average memory usage: {metrics.avg_memory_usage:.2f} MB
- Max memory usage: {metrics.max_memory_usage:.2f} MB

Data Transfer:
- Total bytes sent: {metrics.total_bytes_sent:,}
- Total bytes received: {metrics.total_bytes_received:,}
- Average transfer speed: {metrics.avg_transfer_speed_mbps:.2f} Mbps

Test Configuration:
- Test duration: {metrics.test_duration:.2f}s
- Concurrent requests: {metrics.concurrent_requests}
"""