#!/usr/bin/env python3
"""
Simple test script to verify the logging configuration works correctly.
Run this script to test different log levels and features.
"""

import os
import asyncio
from pghatch.logging_config import get_logger, log_performance

# Test basic logging
logger = get_logger(__name__)

def test_basic_logging():
    """Test basic logging functionality."""
    print("=== Testing Basic Logging ===")

    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")

    print()

def test_contextual_logging():
    """Test logging with context."""
    print("=== Testing Contextual Logging ===")

    context = {"user_id": "test_user", "session_id": "abc123"}
    context_logger = get_logger("test.context", context=context)

    context_logger.info("User performed action")
    context_logger.warning("User attempted unauthorized action")

    print()

def test_exception_logging():
    """Test exception logging."""
    print("=== Testing Exception Logging ===")

    try:
        # Simulate an error
        result = 1 / 0
    except Exception as e:
        logger.error("Division by zero error occurred: %s", str(e), exc_info=True)

    print()

@log_performance(logger, "test operation")
async def test_async_operation():
    """Test async operation with performance logging."""
    await asyncio.sleep(0.1)  # Simulate some work
    return "operation completed"

@log_performance(logger, "sync operation")
def test_sync_operation():
    """Test sync operation with performance logging."""
    import time
    time.sleep(0.05)  # Simulate some work
    return "sync operation completed"

async def test_performance_logging():
    """Test performance logging decorator."""
    print("=== Testing Performance Logging ===")

    # Test async performance logging
    result = await test_async_operation()
    logger.info("Async result: %s", result)

    # Test sync performance logging
    result = test_sync_operation()
    logger.info("Sync result: %s", result)

    print()

def test_structured_logging():
    """Test structured logging patterns."""
    print("=== Testing Structured Logging ===")

    # Good practices
    schema = "public"
    table = "users"
    row_count = 42

    logger.info("Query executed successfully for %s.%s, returned %d rows",
               schema, table, row_count)

    # Database operation simulation
    logger.debug("Connecting to database host: %s, port: %d", "localhost", 5432)
    logger.debug("Executing SQL: %s", "SELECT * FROM users LIMIT 10")
    logger.info("Database operation completed successfully")

    print()

def test_different_log_levels():
    """Test behavior with different log levels."""
    print("=== Testing Different Log Levels ===")

    current_level = os.getenv("PGHATCH_LOG_LEVEL", "INFO")
    print(f"Current log level: {current_level}")

    logger.debug("This debug message may not appear depending on log level")
    logger.info("This info message should appear at INFO level and above")
    logger.warning("This warning should appear at WARNING level and above")
    logger.error("This error should appear at ERROR level and above")

    print()

async def main():
    """Run all logging tests."""
    print("PGHatch Logging System Test")
    print("=" * 40)
    print()

    # Show current configuration
    log_level = os.getenv("PGHATCH_LOG_LEVEL", "INFO")
    log_env = os.getenv("PGHATCH_ENV", "development")
    log_file = os.getenv("PGHATCH_LOG_FILE", "None")

    print(f"Configuration:")
    print(f"  Log Level: {log_level}")
    print(f"  Environment: {log_env}")
    print(f"  Log File: {log_file}")
    print()

    # Run tests
    test_basic_logging()
    test_contextual_logging()
    test_exception_logging()
    await test_performance_logging()
    test_structured_logging()
    test_different_log_levels()

    print("=== Test Complete ===")
    print("Check the output above to verify logging is working correctly.")
    print()
    print("To test different configurations, try:")
    print("  export PGHATCH_LOG_LEVEL=DEBUG")
    print("  export PGHATCH_LOG_LEVEL=WARNING")
    print("  export PGHATCH_LOG_FILE=/tmp/pghatch_test.log")
    print("  python test_logging.py")

if __name__ == "__main__":
    asyncio.run(main())
