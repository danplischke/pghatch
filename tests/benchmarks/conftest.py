"""
Benchmark fixtures and configuration for pghatch API performance testing.
"""

import pytest
import asyncio
import uvicorn
import multiprocessing
import time
import signal
import os
from contextlib import asynccontextmanager
from fastapi.testclient import TestClient
from fastapi import FastAPI

from pghatch.api import app as pghatch_app
from tests.benchmarks.core import BenchmarkRunner


class BenchmarkConfig:
    """Configuration for benchmark tests."""
    
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
    TEST_PORT = 8899  # Use a different port to avoid conflicts
    BASE_URL = f"http://{TEST_HOST}:{TEST_PORT}"
    
    # Request timeouts
    REQUEST_TIMEOUT = 30.0
    
    # Sample test data
    SAMPLE_TABLE_REQUEST = {"limit": 10, "offset": 0}
    SAMPLE_PROC_REQUEST = {"arg_1": "test_value"}


@pytest.fixture(scope="session")
def benchmark_config():
    """Provide benchmark configuration."""
    return BenchmarkConfig()


@pytest.fixture(scope="session")
async def benchmark_server(benchmark_config):
    """Start a test server for benchmarking."""
    
    # Create a separate process for the server to avoid interference
    server_process = None
    
    def start_server():
        """Start the FastAPI server in a separate process."""
        # Configure uvicorn to run the app
        config = uvicorn.Config(
            app=pghatch_app,
            host=benchmark_config.TEST_HOST,
            port=benchmark_config.TEST_PORT,
            log_level="error",  # Minimize logging for cleaner benchmark output
            access_log=False
        )
        server = uvicorn.Server(config)
        server.run()
    
    try:
        # Start the server in a separate process
        server_process = multiprocessing.Process(target=start_server)
        server_process.start()
        
        # Wait for server to start up
        await asyncio.sleep(3)
        
        # Verify server is running by making a test request
        import httpx
        max_retries = 10
        for i in range(max_retries):
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.get(f"{benchmark_config.BASE_URL}/docs")
                    if response.status_code == 200:
                        break
            except Exception:
                if i < max_retries - 1:
                    await asyncio.sleep(1)
                else:
                    raise RuntimeError("Failed to start benchmark server")
        
        yield benchmark_config.BASE_URL
        
    finally:
        # Clean up server process
        if server_process and server_process.is_alive():
            server_process.terminate()
            server_process.join(timeout=5)
            if server_process.is_alive():
                server_process.kill()
                server_process.join()


@pytest.fixture
async def benchmark_runner(benchmark_server, benchmark_config):
    """Provide a configured benchmark runner."""
    return BenchmarkRunner(
        base_url=benchmark_server,
        timeout=benchmark_config.REQUEST_TIMEOUT
    )


@pytest.fixture
def sample_table_endpoints():
    """Provide sample table endpoints for testing."""
    return [
        "/test_schema/users",
        "/test_schema/posts",
        "/test_schema/user_profiles"
    ]


@pytest.fixture
def sample_proc_endpoints():
    """Provide sample procedure endpoints for testing."""
    return [
        "/test_schema/get_user_count",
        "/test_schema/get_active_users",
        "/test_schema/get_users_paginated"
    ]


@pytest.fixture
def sample_requests():
    """Provide sample request payloads for different endpoint types."""
    return {
        "table_request": BenchmarkConfig.SAMPLE_TABLE_REQUEST,
        "proc_request": BenchmarkConfig.SAMPLE_PROC_REQUEST,
        "empty_request": {},
        "large_request": {
            "limit": 1000,
            "offset": 0,
            "filter_data": "x" * 1000  # Large payload for data transfer testing
        }
    }


@pytest.fixture(scope="session")
async def test_database_setup(setup_test_schema):
    """Ensure test database is set up for benchmarking."""
    # This fixture depends on the existing setup_test_schema fixture
    # from conftest.py to ensure test data is available
    yield


# Benchmark-specific markers
pytest.benchmark = pytest.mark.benchmark("Benchmark tests")
pytest.performance = pytest.mark.performance("Performance tests")
pytest.load_test = pytest.mark.load_test("Load testing")