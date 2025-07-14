"""
Main benchmark tests for pghatch API performance evaluation.

This module contains comprehensive benchmark tests covering all key performance metrics
specified in the issue requirements.
"""

import pytest
import asyncio
import statistics
from tests.benchmarks.core import BenchmarkMetrics, format_benchmark_results


@pytest.mark.benchmark
@pytest.mark.asyncio
class TestResponseTimeBenchmarks:
    """Test response time performance metrics."""
    
    async def test_single_request_response_time(self, benchmark_runner, sample_table_endpoints, sample_requests):
        """Test response time for single requests."""
        endpoint = sample_table_endpoints[0]  # /test_schema/users
        request_data = sample_requests["table_request"]
        
        # Run single request benchmark
        response_time, ttfb, status_code, bytes_sent, bytes_received = await benchmark_runner.single_request_benchmark(
            endpoint=endpoint,
            method="POST",
            json_data=request_data
        )
        
        # Assertions for response time quality
        assert response_time > 0, "Response time should be positive"
        assert response_time < 5.0, f"Response time {response_time}s should be under 5 seconds"
        assert ttfb > 0, "Time to first byte should be positive"
        assert ttfb <= response_time, "TTFB should not exceed total response time"
        assert status_code in [200, 201], f"Expected success status code, got {status_code}"
        
        print(f"\nSingle Request Response Time: {response_time:.4f}s")
        print(f"Time to First Byte: {ttfb:.4f}s")
        print(f"Data Transfer: {bytes_sent + bytes_received} bytes")
    
    async def test_average_response_time_under_load(self, benchmark_runner, benchmark_config, sample_table_endpoints, sample_requests):
        """Test average response time under sustained load."""
        endpoint = sample_table_endpoints[0]
        request_data = sample_requests["table_request"]
        
        # Run throughput benchmark for medium duration
        metrics = await benchmark_runner.throughput_benchmark(
            endpoint=endpoint,
            duration_seconds=benchmark_config.MEDIUM_DURATION,
            method="POST",
            json_data=request_data
        )
        
        # Response time quality assertions
        assert metrics.avg_response_time > 0, "Average response time should be positive"
        assert metrics.avg_response_time < 2.0, f"Average response time {metrics.avg_response_time}s should be under 2 seconds"
        assert metrics.p95_response_time < 5.0, f"95th percentile response time {metrics.p95_response_time}s should be under 5 seconds"
        assert metrics.max_response_time < 10.0, f"Max response time {metrics.max_response_time}s should be under 10 seconds"
        
        print(format_benchmark_results(metrics))


@pytest.mark.benchmark
@pytest.mark.asyncio
class TestThroughputBenchmarks:
    """Test throughput and requests per second metrics."""
    
    async def test_sustained_throughput(self, benchmark_runner, benchmark_config, sample_table_endpoints, sample_requests):
        """Test sustained throughput over time."""
        endpoint = sample_table_endpoints[0]
        request_data = sample_requests["table_request"]
        
        # Run throughput benchmark
        metrics = await benchmark_runner.throughput_benchmark(
            endpoint=endpoint,
            duration_seconds=benchmark_config.MEDIUM_DURATION,
            method="POST",
            json_data=request_data
        )
        
        # Throughput quality assertions
        assert metrics.requests_per_second > 0, "Should process at least some requests per second"
        assert metrics.requests_per_second > 1.0, f"Throughput {metrics.requests_per_second} RPS should be at least 1 RPS"
        assert metrics.total_requests > 0, "Should have processed some requests"
        
        print(f"\nThroughput Test Results:")
        print(f"Requests per second: {metrics.requests_per_second:.2f}")
        print(f"Total requests: {metrics.total_requests}")
        print(f"Test duration: {metrics.test_duration:.2f}s")
    
    async def test_peak_throughput(self, benchmark_runner, sample_table_endpoints, sample_requests):
        """Test peak throughput with burst requests."""
        endpoint = sample_table_endpoints[0]
        request_data = sample_requests["table_request"]
        
        # Run short burst test
        metrics = await benchmark_runner.throughput_benchmark(
            endpoint=endpoint,
            duration_seconds=3.0,  # Short burst
            method="POST",
            json_data=request_data
        )
        
        # Peak throughput should be higher due to burst nature
        assert metrics.requests_per_second > 0, "Peak throughput should be positive"
        
        print(f"\nPeak Throughput: {metrics.requests_per_second:.2f} RPS")


@pytest.mark.benchmark
@pytest.mark.asyncio
class TestErrorRateAndReliabilityBenchmarks:
    """Test error rate and success rate metrics."""
    
    async def test_error_rate_under_normal_load(self, benchmark_runner, benchmark_config, sample_table_endpoints, sample_requests):
        """Test error rate under normal operating conditions."""
        endpoint = sample_table_endpoints[0]
        request_data = sample_requests["table_request"]
        
        metrics = await benchmark_runner.throughput_benchmark(
            endpoint=endpoint,
            duration_seconds=benchmark_config.SHORT_DURATION,
            method="POST",
            json_data=request_data
        )
        
        # Error rate quality assertions
        assert metrics.error_rate < 5.0, f"Error rate {metrics.error_rate}% should be under 5%"
        assert metrics.success_rate > 95.0, f"Success rate {metrics.success_rate}% should be above 95%"
        assert metrics.successful_requests > 0, "Should have some successful requests"
        
        print(f"\nReliability Metrics:")
        print(f"Error rate: {metrics.error_rate:.2f}%")
        print(f"Success rate: {metrics.success_rate:.2f}%")
        print(f"Failed requests: {metrics.failed_requests}")
    
    async def test_error_rate_with_invalid_requests(self, benchmark_runner, sample_table_endpoints):
        """Test error handling with invalid requests."""
        endpoint = sample_table_endpoints[0]
        invalid_data = {"invalid_field": "invalid_value"}
        
        # Test single invalid request
        response_time, ttfb, status_code, _, _ = await benchmark_runner.single_request_benchmark(
            endpoint=endpoint,
            method="POST",
            json_data=invalid_data
        )
        
        # Should handle invalid requests gracefully
        assert response_time > 0, "Should still measure response time for invalid requests"
        # Status code might be 4xx or 5xx depending on validation
        assert status_code != 0, "Should return a valid HTTP status code"


@pytest.mark.benchmark
@pytest.mark.asyncio
class TestLatencyBenchmarks:
    """Test latency metrics including TTFB (Time To First Byte)."""
    
    async def test_time_to_first_byte(self, benchmark_runner, sample_table_endpoints, sample_requests):
        """Test Time To First Byte (TTFB) latency."""
        endpoint = sample_table_endpoints[0]
        request_data = sample_requests["table_request"]
        
        # Collect multiple TTFB measurements
        ttfb_measurements = []
        for _ in range(10):
            _, ttfb, status_code, _, _ = await benchmark_runner.single_request_benchmark(
                endpoint=endpoint,
                method="POST",
                json_data=request_data
            )
            if status_code in [200, 201]:
                ttfb_measurements.append(ttfb)
        
        assert len(ttfb_measurements) > 0, "Should have collected TTFB measurements"
        
        avg_ttfb = statistics.mean(ttfb_measurements)
        min_ttfb = min(ttfb_measurements)
        max_ttfb = max(ttfb_measurements)
        
        # TTFB quality assertions
        assert avg_ttfb > 0, "Average TTFB should be positive"
        assert avg_ttfb < 1.0, f"Average TTFB {avg_ttfb:.4f}s should be under 1 second"
        assert max_ttfb < 2.0, f"Max TTFB {max_ttfb:.4f}s should be under 2 seconds"
        
        print(f"\nTTFB Metrics:")
        print(f"Average TTFB: {avg_ttfb:.4f}s")
        print(f"Min TTFB: {min_ttfb:.4f}s")
        print(f"Max TTFB: {max_ttfb:.4f}s")


@pytest.mark.benchmark
@pytest.mark.asyncio
class TestConcurrencyBenchmarks:
    """Test concurrency and simultaneous request handling."""
    
    async def test_low_concurrency_performance(self, benchmark_runner, benchmark_config, sample_table_endpoints, sample_requests):
        """Test performance with low concurrency load."""
        endpoint = sample_table_endpoints[0]
        request_data = sample_requests["table_request"]
        
        metrics = await benchmark_runner.concurrency_benchmark(
            endpoint=endpoint,
            concurrent_requests=benchmark_config.LOW_CONCURRENCY,
            method="POST",
            json_data=request_data
        )
        
        # Concurrency quality assertions
        assert metrics.concurrent_success_rate > 80.0, f"Concurrent success rate {metrics.concurrent_success_rate}% should be above 80%"
        assert metrics.successful_requests > 0, "Should have successful concurrent requests"
        assert metrics.avg_response_time < 5.0, f"Average response time {metrics.avg_response_time}s should be under 5 seconds under low concurrency"
        
        print(f"\nLow Concurrency Results:")
        print(f"Concurrent requests: {metrics.concurrent_requests}")
        print(f"Concurrent success rate: {metrics.concurrent_success_rate:.2f}%")
        print(f"Average response time: {metrics.avg_response_time:.4f}s")
    
    async def test_medium_concurrency_performance(self, benchmark_runner, benchmark_config, sample_table_endpoints, sample_requests):
        """Test performance with medium concurrency load."""
        endpoint = sample_table_endpoints[0]
        request_data = sample_requests["table_request"]
        
        metrics = await benchmark_runner.concurrency_benchmark(
            endpoint=endpoint,
            concurrent_requests=benchmark_config.MEDIUM_CONCURRENCY,
            method="POST",
            json_data=request_data
        )
        
        # Should still perform reasonably well under medium load
        assert metrics.concurrent_success_rate > 70.0, f"Concurrent success rate {metrics.concurrent_success_rate}% should be above 70%"
        assert metrics.avg_response_time < 10.0, f"Average response time {metrics.avg_response_time}s should be under 10 seconds under medium concurrency"
        
        print(f"\nMedium Concurrency Results:")
        print(f"Concurrent requests: {metrics.concurrent_requests}")
        print(f"Concurrent success rate: {metrics.concurrent_success_rate:.2f}%")
    
    async def test_high_concurrency_stress(self, benchmark_runner, benchmark_config, sample_table_endpoints, sample_requests):
        """Test system behavior under high concurrency stress."""
        endpoint = sample_table_endpoints[0]
        request_data = sample_requests["table_request"]
        
        metrics = await benchmark_runner.concurrency_benchmark(
            endpoint=endpoint,
            concurrent_requests=benchmark_config.HIGH_CONCURRENCY,
            method="POST",
            json_data=request_data
        )
        
        # Under high stress, we expect some degradation but not complete failure
        assert metrics.concurrent_success_rate > 50.0, f"Even under high stress, success rate {metrics.concurrent_success_rate}% should be above 50%"
        assert metrics.total_requests > 0, "Should process some requests even under stress"
        
        print(f"\nHigh Concurrency Stress Results:")
        print(f"Concurrent requests: {metrics.concurrent_requests}")
        print(f"Concurrent success rate: {metrics.concurrent_success_rate:.2f}%")


@pytest.mark.benchmark
@pytest.mark.asyncio  
class TestResourceUtilizationBenchmarks:
    """Test resource utilization metrics (CPU, memory)."""
    
    async def test_cpu_usage_under_load(self, benchmark_runner, benchmark_config, sample_table_endpoints, sample_requests):
        """Test CPU usage under sustained load."""
        endpoint = sample_table_endpoints[0]
        request_data = sample_requests["table_request"]
        
        metrics = await benchmark_runner.throughput_benchmark(
            endpoint=endpoint,
            duration_seconds=benchmark_config.MEDIUM_DURATION,
            method="POST",
            json_data=request_data
        )
        
        # CPU usage assertions
        assert len(metrics.cpu_usage_samples) > 0, "Should have collected CPU usage samples"
        assert metrics.avg_cpu_usage >= 0, "Average CPU usage should be non-negative"
        assert metrics.max_cpu_usage >= 0, "Max CPU usage should be non-negative"
        assert metrics.max_cpu_usage <= 100, "Max CPU usage should not exceed 100%"
        
        print(f"\nCPU Usage Metrics:")
        print(f"Average CPU usage: {metrics.avg_cpu_usage:.2f}%")
        print(f"Max CPU usage: {metrics.max_cpu_usage:.2f}%")
        print(f"CPU samples collected: {len(metrics.cpu_usage_samples)}")
    
    async def test_memory_usage_under_load(self, benchmark_runner, benchmark_config, sample_table_endpoints, sample_requests):
        """Test memory usage under sustained load."""
        endpoint = sample_table_endpoints[0]
        request_data = sample_requests["table_request"]
        
        metrics = await benchmark_runner.throughput_benchmark(
            endpoint=endpoint,
            duration_seconds=benchmark_config.MEDIUM_DURATION,
            method="POST",
            json_data=request_data
        )
        
        # Memory usage assertions
        assert len(metrics.memory_usage_samples) > 0, "Should have collected memory usage samples"
        assert metrics.avg_memory_usage > 0, "Average memory usage should be positive"
        assert metrics.max_memory_usage > 0, "Max memory usage should be positive"
        
        print(f"\nMemory Usage Metrics:")
        print(f"Average memory usage: {metrics.avg_memory_usage:.2f} MB")
        print(f"Max memory usage: {metrics.max_memory_usage:.2f} MB")
        print(f"Memory samples collected: {len(metrics.memory_usage_samples)}")


@pytest.mark.benchmark
@pytest.mark.asyncio
class TestDataTransferBenchmarks:
    """Test data transfer speed and payload handling."""
    
    async def test_small_payload_transfer(self, benchmark_runner, sample_table_endpoints, sample_requests):
        """Test data transfer with small payloads."""
        endpoint = sample_table_endpoints[0]
        request_data = sample_requests["table_request"]
        
        response_time, ttfb, status_code, bytes_sent, bytes_received = await benchmark_runner.single_request_benchmark(
            endpoint=endpoint,
            method="POST",
            json_data=request_data
        )
        
        assert bytes_sent > 0 or bytes_received > 0, "Should transfer some data"
        total_bytes = bytes_sent + bytes_received
        
        if response_time > 0:
            transfer_speed_bps = (total_bytes * 8) / response_time  # bits per second
            transfer_speed_mbps = transfer_speed_bps / 1_000_000  # Mbps
            
            print(f"\nSmall Payload Transfer:")
            print(f"Bytes sent: {bytes_sent}")
            print(f"Bytes received: {bytes_received}")
            print(f"Transfer speed: {transfer_speed_mbps:.2f} Mbps")
    
    async def test_large_payload_transfer(self, benchmark_runner, sample_table_endpoints, sample_requests):
        """Test data transfer with larger payloads."""
        endpoint = sample_table_endpoints[0]
        request_data = sample_requests["large_request"]
        
        response_time, ttfb, status_code, bytes_sent, bytes_received = await benchmark_runner.single_request_benchmark(
            endpoint=endpoint,
            method="POST",
            json_data=request_data
        )
        
        total_bytes = bytes_sent + bytes_received
        
        # Should handle larger payloads
        assert total_bytes > 0, "Should transfer data with large payload"
        
        if response_time > 0:
            transfer_speed_bps = (total_bytes * 8) / response_time
            transfer_speed_mbps = transfer_speed_bps / 1_000_000
            
            print(f"\nLarge Payload Transfer:")
            print(f"Bytes sent: {bytes_sent}")
            print(f"Bytes received: {bytes_received}")
            print(f"Transfer speed: {transfer_speed_mbps:.2f} Mbps")
    
    async def test_sustained_data_transfer_rate(self, benchmark_runner, benchmark_config, sample_table_endpoints, sample_requests):
        """Test sustained data transfer rate over time."""
        endpoint = sample_table_endpoints[0]
        request_data = sample_requests["table_request"]
        
        metrics = await benchmark_runner.throughput_benchmark(
            endpoint=endpoint,
            duration_seconds=benchmark_config.SHORT_DURATION,
            method="POST",
            json_data=request_data
        )
        
        # Data transfer quality assertions
        assert metrics.total_bytes_sent + metrics.total_bytes_received > 0, "Should transfer data over sustained period"
        assert metrics.avg_transfer_speed_mbps >= 0, "Transfer speed should be non-negative"
        
        print(f"\nSustained Data Transfer:")
        print(f"Total bytes transferred: {metrics.total_bytes_sent + metrics.total_bytes_received:,}")
        print(f"Average transfer speed: {metrics.avg_transfer_speed_mbps:.2f} Mbps")


@pytest.mark.benchmark
@pytest.mark.asyncio
class TestAPIConsumptionBenchmarks:
    """Test API consumption patterns and usage metrics."""
    
    async def test_requests_per_minute_rate(self, benchmark_runner, sample_table_endpoints, sample_requests):
        """Test requests per minute consumption rate."""
        endpoint = sample_table_endpoints[0]
        request_data = sample_requests["table_request"]
        
        # Run for 1 minute to get accurate RPM
        metrics = await benchmark_runner.throughput_benchmark(
            endpoint=endpoint,
            duration_seconds=60.0,  # 1 minute
            method="POST",
            json_data=request_data
        )
        
        requests_per_minute = metrics.requests_per_second * 60
        
        assert requests_per_minute > 0, "Should have positive requests per minute"
        
        print(f"\nAPI Consumption Metrics:")
        print(f"Requests per second: {metrics.requests_per_second:.2f}")
        print(f"Requests per minute: {requests_per_minute:.2f}")
        print(f"Total requests in test: {metrics.total_requests}")
    
    async def test_api_endpoint_usage_patterns(self, benchmark_runner, sample_table_endpoints, sample_requests):
        """Test usage patterns across different API endpoints."""
        request_data = sample_requests["table_request"]
        results = {}
        
        # Test multiple endpoints
        for endpoint in sample_table_endpoints[:2]:  # Test first 2 endpoints
            try:
                response_time, ttfb, status_code, bytes_sent, bytes_received = await benchmark_runner.single_request_benchmark(
                    endpoint=endpoint,
                    method="POST",
                    json_data=request_data
                )
                
                results[endpoint] = {
                    "response_time": response_time,
                    "status_code": status_code,
                    "bytes_transferred": bytes_sent + bytes_received
                }
            except Exception as e:
                results[endpoint] = {"error": str(e)}
        
        # At least one endpoint should work
        successful_endpoints = [ep for ep, result in results.items() if "error" not in result]
        assert len(successful_endpoints) > 0, "At least one endpoint should be accessible"
        
        print(f"\nEndpoint Usage Patterns:")
        for endpoint, result in results.items():
            if "error" not in result:
                print(f"{endpoint}: {result['response_time']:.4f}s, {result['bytes_transferred']} bytes")
            else:
                print(f"{endpoint}: Error - {result['error']}")


@pytest.mark.benchmark
@pytest.mark.performance
@pytest.mark.asyncio
class TestComprehensivePerformanceSuite:
    """Comprehensive performance test suite combining all metrics."""
    
    async def test_comprehensive_performance_baseline(self, benchmark_runner, benchmark_config, sample_table_endpoints, sample_requests):
        """Run a comprehensive performance baseline test."""
        endpoint = sample_table_endpoints[0]
        request_data = sample_requests["table_request"]
        
        print(f"\n{'='*60}")
        print("COMPREHENSIVE PERFORMANCE BASELINE TEST")
        print(f"{'='*60}")
        
        # 1. Single request performance
        print("\n1. Single Request Performance:")
        response_time, ttfb, status_code, bytes_sent, bytes_received = await benchmark_runner.single_request_benchmark(
            endpoint=endpoint,
            method="POST",
            json_data=request_data
        )
        print(f"   Response time: {response_time:.4f}s")
        print(f"   TTFB: {ttfb:.4f}s")
        print(f"   Status: {status_code}")
        
        # 2. Sustained throughput
        print("\n2. Sustained Throughput Test:")
        throughput_metrics = await benchmark_runner.throughput_benchmark(
            endpoint=endpoint,
            duration_seconds=benchmark_config.MEDIUM_DURATION,
            method="POST",
            json_data=request_data
        )
        print(f"   Requests/sec: {throughput_metrics.requests_per_second:.2f}")
        print(f"   Success rate: {throughput_metrics.success_rate:.2f}%")
        print(f"   Avg response time: {throughput_metrics.avg_response_time:.4f}s")
        
        # 3. Concurrency test
        print("\n3. Concurrency Test:")
        concurrency_metrics = await benchmark_runner.concurrency_benchmark(
            endpoint=endpoint,
            concurrent_requests=benchmark_config.MEDIUM_CONCURRENCY,
            method="POST",
            json_data=request_data
        )
        print(f"   Concurrent requests: {concurrency_metrics.concurrent_requests}")
        print(f"   Concurrent success rate: {concurrency_metrics.concurrent_success_rate:.2f}%")
        print(f"   Avg response time: {concurrency_metrics.avg_response_time:.4f}s")
        
        # Overall performance assertions
        assert response_time < 5.0, "Single request should complete within 5 seconds"
        assert throughput_metrics.requests_per_second > 0.5, "Should achieve at least 0.5 RPS"
        assert throughput_metrics.success_rate > 90.0, "Should maintain >90% success rate"
        assert concurrency_metrics.concurrent_success_rate > 70.0, "Should handle concurrency with >70% success rate"
        
        print(f"\n{'='*60}")
        print("PERFORMANCE BASELINE COMPLETE")
        print(f"{'='*60}")