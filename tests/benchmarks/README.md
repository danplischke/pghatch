# pghatch Benchmarking Suite

A comprehensive performance testing suite for the pghatch FastAPI application, designed to evaluate key performance metrics and ensure optimal API performance.

## Overview

The benchmarking suite provides comprehensive performance testing covering the following metrics:

### Core Performance Metrics

1. **Response Time**: Time for API to receive request and send response
2. **Throughput**: Number of requests handled per time unit (RPS)
3. **Error Rate**: Percentage of requests resulting in errors
4. **Latency**: Delay between request and first byte of response (TTFB)
5. **Resource Utilization**: CPU and memory usage during testing
6. **Success Rate**: Percentage of successful responses
7. **API Consumption**: Requests per minute/second metrics
8. **Concurrency**: Ability to handle multiple simultaneous requests
9. **Data Transfer Speed**: Speed of data transfer between client and server
10. **SDK Performance**: Client-side library performance metrics

## Installation

Install the benchmark dependencies:

```bash
pip install psutil httpx pytest pytest-asyncio uvicorn
```

Or install the benchmark dependency group:

```bash
pip install -e ".[benchmark]"
```

## Usage

### Running All Benchmarks

Run the complete benchmark suite:

```bash
# Run all benchmark tests
pytest tests/benchmarks/ -m benchmark -v

# Run only performance tests
pytest tests/benchmarks/ -m performance -v

# Run specific benchmark categories
pytest tests/benchmarks/test_benchmarks.py::TestResponseTimeBenchmarks -v
pytest tests/benchmarks/test_benchmarks.py::TestThroughputBenchmarks -v
pytest tests/benchmarks/test_benchmarks.py::TestConcurrencyBenchmarks -v
```

### Running Individual Benchmark Categories

```bash
# Response time benchmarks
pytest tests/benchmarks/test_benchmarks.py::TestResponseTimeBenchmarks -v

# Throughput benchmarks  
pytest tests/benchmarks/test_benchmarks.py::TestThroughputBenchmarks -v

# Error rate and reliability benchmarks
pytest tests/benchmarks/test_benchmarks.py::TestErrorRateAndReliabilityBenchmarks -v

# Latency benchmarks
pytest tests/benchmarks/test_benchmarks.py::TestLatencyBenchmarks -v

# Concurrency benchmarks
pytest tests/benchmarks/test_benchmarks.py::TestConcurrencyBenchmarks -v

# Resource utilization benchmarks
pytest tests/benchmarks/test_benchmarks.py::TestResourceUtilizationBenchmarks -v

# Data transfer benchmarks
pytest tests/benchmarks/test_benchmarks.py::TestDataTransferBenchmarks -v

# API consumption benchmarks
pytest tests/benchmarks/test_benchmarks.py::TestAPIConsumptionBenchmarks -v

# Comprehensive performance suite
pytest tests/benchmarks/test_benchmarks.py::TestComprehensivePerformanceSuite -v
```

### Quick Development Testing

For quick testing during development:

```bash
# Run the simple benchmark script
python tests/benchmarks/run_benchmark.py
```

## Configuration

### Benchmark Configuration

The benchmarking suite uses configurable parameters defined in `tests/benchmarks/conftest.py`:

```python
class BenchmarkConfig:
    # Test durations (in seconds)
    SHORT_DURATION = 5.0
    MEDIUM_DURATION = 10.0
    LONG_DURATION = 30.0
    
    # Concurrency levels
    LOW_CONCURRENCY = 5
    MEDIUM_CONCURRENCY = 10
    HIGH_CONCURRENCY = 20
    
    # Test server configuration
    TEST_HOST = "127.0.0.1"
    TEST_PORT = 8899
    BASE_URL = f"http://{TEST_HOST}:{TEST_PORT}"
    
    # Request timeouts
    REQUEST_TIMEOUT = 30.0
```

### Test Data Requirements

The benchmarks require a test database with the following schema setup:

- `test_schema.users` table
- `test_schema.posts` table  
- `test_schema.user_profiles` table
- Various stored procedures and functions

The test schema is automatically set up using the existing `conftest.py` fixtures.

## Benchmark Results

### Sample Output

```
Benchmark Results:
==================

Response Time Metrics:
- Average: 0.0234s
- Min: 0.0187s
- Max: 0.0456s
- 95th percentile: 0.0387s
- 99th percentile: 0.0423s

Throughput Metrics:
- Total requests: 847
- Successful requests: 847
- Failed requests: 0
- Requests per second: 84.70

Reliability Metrics:
- Success rate: 100.00%
- Error rate: 0.00%

Latency Metrics (TTFB):
- Average TTFB: 0.0156s
- Min TTFB: 0.0123s
- Max TTFB: 0.0298s

Resource Utilization:
- Average CPU usage: 23.45%
- Max CPU usage: 67.89%
- Average memory usage: 245.67 MB
- Max memory usage: 289.34 MB

Data Transfer:
- Total bytes sent: 42,350
- Total bytes received: 1,234,567
- Average transfer speed: 12.34 Mbps

Test Configuration:
- Test duration: 10.00s
- Concurrent requests: 10
```

## Architecture

### Core Components

- **`core.py`**: Core benchmarking infrastructure including `BenchmarkRunner`, `BenchmarkMetrics`, and `ResourceMonitor`
- **`conftest.py`**: Pytest fixtures and configuration for benchmark testing
- **`test_benchmarks.py`**: Main benchmark test suite with comprehensive performance tests
- **`run_benchmark.py`**: Simple standalone benchmark runner for development

### Key Classes

#### BenchmarkRunner

Main class for executing performance tests:

```python
runner = BenchmarkRunner(base_url="http://localhost:8000", timeout=30.0)

# Single request benchmark
response_time, ttfb, status_code, bytes_sent, bytes_received = await runner.single_request_benchmark(
    endpoint="/api/endpoint",
    method="POST",
    json_data={"key": "value"}
)

# Throughput benchmark
metrics = await runner.throughput_benchmark(
    endpoint="/api/endpoint",
    duration_seconds=10.0,
    method="POST",
    json_data={"key": "value"}
)

# Concurrency benchmark
metrics = await runner.concurrency_benchmark(
    endpoint="/api/endpoint",
    concurrent_requests=10,
    method="POST",
    json_data={"key": "value"}
)
```

#### BenchmarkMetrics

Container for collected performance metrics with automatic calculation of derived metrics.

#### ResourceMonitor

Monitors system resources (CPU, memory) during benchmark execution.

## Extending the Benchmarks

### Adding New Benchmark Tests

1. Create a new test class in `test_benchmarks.py`:

```python
@pytest.mark.benchmark
@pytest.mark.asyncio
class TestCustomBenchmarks:
    """Test custom performance metrics."""
    
    async def test_custom_metric(self, benchmark_runner, benchmark_config):
        """Test custom performance metric."""
        # Your benchmark implementation
        pass
```

2. Use the existing `benchmark_runner` fixture for consistent testing.

3. Follow the existing patterns for assertions and result formatting.

### Adding New Metrics

1. Extend the `BenchmarkMetrics` dataclass in `core.py`:

```python
@dataclass
class BenchmarkMetrics:
    # Add new metric fields
    custom_metric: float = 0.0
    custom_samples: List[float] = field(default_factory=list)
```

2. Update the `calculate_derived_metrics()` method to compute your custom metrics.

3. Update the `format_benchmark_results()` function to display your new metrics.

## Best Practices

### Performance Testing Guidelines

1. **Baseline Establishment**: Always establish baseline performance before making changes
2. **Consistent Environment**: Run benchmarks in consistent environments
3. **Multiple Runs**: Run benchmarks multiple times to account for variance
4. **Resource Monitoring**: Monitor system resources during testing
5. **Realistic Data**: Use realistic test data and request patterns

### Assertion Guidelines

1. **Reasonable Thresholds**: Set reasonable performance thresholds based on requirements
2. **Environment Awareness**: Adjust thresholds based on test environment capabilities
3. **Degradation Detection**: Focus on detecting performance degradation rather than absolute values
4. **Success Rate Focus**: Prioritize success rate and error rate over pure speed

### CI/CD Integration

The benchmarks can be integrated into CI/CD pipelines:

```bash
# Quick performance check (short duration tests)
pytest tests/benchmarks/ -m "benchmark and not load_test" --tb=short

# Full performance suite (longer running)
pytest tests/benchmarks/ -m benchmark --tb=short
```

## Troubleshooting

### Common Issues

1. **Server Not Running**: Ensure the pghatch server is running before executing benchmarks
2. **Database Connection**: Verify database connection and test schema setup
3. **Port Conflicts**: Check that the test port (8899) is available
4. **Resource Limitations**: Consider system resources when running high concurrency tests

### Debug Mode

Enable debug output for troubleshooting:

```bash
pytest tests/benchmarks/ -v -s --log-cli-level=DEBUG
```

## Performance Targets

### Recommended Performance Targets

Based on typical web API performance standards:

- **Response Time**: < 200ms for simple queries, < 2s for complex operations
- **Throughput**: > 100 RPS for lightweight operations
- **Success Rate**: > 99.9% under normal load
- **Error Rate**: < 0.1% under normal conditions
- **TTFB**: < 100ms for most operations
- **Concurrency**: Handle 10+ concurrent requests with < 10% performance degradation

These targets should be adjusted based on your specific requirements and infrastructure.