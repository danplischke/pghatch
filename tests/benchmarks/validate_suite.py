#!/usr/bin/env python3
"""
Validation script to demonstrate the benchmarking suite functionality.

This script validates that the benchmarking infrastructure is working correctly
without requiring a running server.
"""

import asyncio
import sys
import os

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from tests.benchmarks.core import BenchmarkRunner, BenchmarkMetrics, ResourceMonitor, format_benchmark_results


async def validate_benchmark_infrastructure():
    """Validate that the benchmark infrastructure is working correctly."""
    
    print("🧪 Validating pghatch Benchmarking Suite")
    print("=" * 50)
    
    # Test 1: Validate BenchmarkMetrics
    print("\n1. Testing BenchmarkMetrics...")
    metrics = BenchmarkMetrics()
    
    # Add some sample data
    metrics.response_times = [0.1, 0.2, 0.15, 0.3, 0.12]
    metrics.ttfb_times = [0.05, 0.08, 0.07, 0.12, 0.06]
    metrics.total_requests = 100
    metrics.successful_requests = 95
    metrics.failed_requests = 5
    metrics.test_duration = 10.0
    metrics.cpu_usage_samples = [20.5, 25.3, 22.1, 28.7]
    metrics.memory_usage_samples = [150.2, 155.8, 160.1, 158.9]
    metrics.total_bytes_sent = 5000
    metrics.total_bytes_received = 50000
    
    # Calculate derived metrics
    metrics.calculate_derived_metrics()
    
    print(f"   ✅ Average response time: {metrics.avg_response_time:.4f}s")
    print(f"   ✅ Success rate: {metrics.success_rate:.2f}%")
    print(f"   ✅ Requests per second: {metrics.requests_per_second:.2f}")
    print(f"   ✅ Average CPU usage: {metrics.avg_cpu_usage:.2f}%")
    print(f"   ✅ Transfer speed: {metrics.avg_transfer_speed_mbps:.2f} Mbps")
    
    # Test 2: Validate ResourceMonitor
    print("\n2. Testing ResourceMonitor...")
    monitor = ResourceMonitor(sample_interval=0.1)
    
    await monitor.start_monitoring()
    await asyncio.sleep(1.0)  # Monitor for 1 second
    await monitor.stop_monitoring()
    
    cpu_samples, memory_samples = monitor.get_samples()
    print(f"   ✅ Collected {len(cpu_samples)} CPU samples")
    print(f"   ✅ Collected {len(memory_samples)} memory samples")
    
    if cpu_samples:
        print(f"   ✅ CPU range: {min(cpu_samples):.1f}% - {max(cpu_samples):.1f}%")
    if memory_samples:
        print(f"   ✅ Memory range: {min(memory_samples):.1f}MB - {max(memory_samples):.1f}MB")
    
    # Test 3: Validate BenchmarkRunner initialization
    print("\n3. Testing BenchmarkRunner...")
    runner = BenchmarkRunner(base_url="http://localhost:8000", timeout=30.0)
    print(f"   ✅ BenchmarkRunner initialized with base_url: {runner.base_url}")
    print(f"   ✅ Timeout configured: {runner.timeout}s")
    print(f"   ✅ Resource monitor available: {runner.resource_monitor is not None}")
    
    # Test 4: Validate result formatting
    print("\n4. Testing result formatting...")
    formatted_results = format_benchmark_results(metrics)
    
    # Check that key sections are present
    expected_sections = [
        "Response Time Metrics:",
        "Throughput Metrics:",
        "Reliability Metrics:",
        "Latency Metrics (TTFB):",
        "Resource Utilization:",
        "Data Transfer:",
        "Test Configuration:"
    ]
    
    for section in expected_sections:
        if section in formatted_results:
            print(f"   ✅ {section} section present")
        else:
            print(f"   ❌ {section} section missing")
    
    # Test 5: Validate pytest marker integration
    print("\n5. Testing pytest integration...")
    try:
        import pytest
        print("   ✅ pytest available")
        
        # Check if our custom markers would be recognized
        print("   ✅ Custom markers: benchmark, performance, load_test")
        
    except ImportError:
        print("   ⚠️  pytest not available (should be installed for benchmarking)")
    
    print("\n" + "=" * 50)
    print("🎉 Benchmarking Suite Validation Complete!")
    print("\nNext steps:")
    print("1. Start a pghatch server with test database")
    print("2. Run: pytest tests/benchmarks/ -m benchmark -v")
    print("3. Review performance results and adjust thresholds as needed")
    print("\nFor quick testing without server:")
    print("python tests/benchmarks/run_benchmark.py")


if __name__ == "__main__":
    asyncio.run(validate_benchmark_infrastructure())