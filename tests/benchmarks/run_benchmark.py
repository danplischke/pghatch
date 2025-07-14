"""
Simple benchmark runner for pghatch performance testing.

This script allows running benchmarks independently from pytest for quick testing
and development purposes.
"""

import asyncio
import sys
import os

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from tests.benchmarks.core import BenchmarkRunner, format_benchmark_results


async def run_simple_benchmark():
    """Run a simple benchmark test for development purposes."""
    
    print("Starting simple benchmark test...")
    
    # Configuration
    base_url = "http://127.0.0.1:8000"  # Adjust as needed
    endpoint = "/test_schema/users"
    request_data = {"limit": 10, "offset": 0}
    
    # Create benchmark runner
    runner = BenchmarkRunner(base_url=base_url, timeout=30.0)
    
    try:
        print(f"\n1. Testing single request to {endpoint}")
        response_time, ttfb, status_code, bytes_sent, bytes_received = await runner.single_request_benchmark(
            endpoint=endpoint,
            method="POST",
            json_data=request_data
        )
        
        print(f"   Response time: {response_time:.4f}s")
        print(f"   TTFB: {ttfb:.4f}s")
        print(f"   Status code: {status_code}")
        print(f"   Bytes sent: {bytes_sent}")
        print(f"   Bytes received: {bytes_received}")
        
        if status_code not in [200, 201]:
            print(f"   Warning: Unexpected status code {status_code}")
            return
        
        print(f"\n2. Running throughput test for 5 seconds...")
        metrics = await runner.throughput_benchmark(
            endpoint=endpoint,
            duration_seconds=5.0,
            method="POST",
            json_data=request_data
        )
        
        print(format_benchmark_results(metrics))
        
        print(f"\n3. Running concurrency test with 5 concurrent requests...")
        concurrency_metrics = await runner.concurrency_benchmark(
            endpoint=endpoint,
            concurrent_requests=5,
            method="POST",
            json_data=request_data
        )
        
        print(f"   Concurrent requests: {concurrency_metrics.concurrent_requests}")
        print(f"   Concurrent success rate: {concurrency_metrics.concurrent_success_rate:.2f}%")
        print(f"   Average response time: {concurrency_metrics.avg_response_time:.4f}s")
        
        print("\nBenchmark test completed successfully!")
        
    except Exception as e:
        print(f"Benchmark test failed: {e}")
        print("Make sure the pghatch server is running and accessible.")


if __name__ == "__main__":
    asyncio.run(run_simple_benchmark())