# Logging Configuration for PGHatch

This document describes the logging setup and best practices implemented across the PGHatch project.

## Overview

PGHatch uses a centralized logging configuration that provides:
- Structured logging with consistent formatting
- Environment-based configuration
- Performance monitoring capabilities
- Proper error tracking with stack traces
- Configurable log levels and output destinations

## Configuration

### Environment Variables

The logging system can be configured using the following environment variables:

- `PGHATCH_LOG_LEVEL`: Set the log level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Default: INFO
- `PGHATCH_ENV`: Set the environment (development, production). Default: development
- `PGHATCH_LOG_FILE`: Optional file path for log output. If not set, logs only to console

### Log Levels

- **DEBUG**: Detailed information for debugging, including SQL queries, connection details
- **INFO**: General operational information, successful operations, startup/shutdown
- **WARNING**: Non-critical issues, schema changes, deprecated usage
- **ERROR**: Failed operations, database connection issues, invalid configurations
- **CRITICAL**: System failures, startup failures

## Usage Examples

### Basic Logger Usage

```python
from pghatch.logging_config import get_logger

logger = get_logger(__name__)

# Log different levels
logger.debug("Detailed debug information")
logger.info("Operation completed successfully")
logger.warning("Non-critical issue occurred")
logger.error("Operation failed", exc_info=True)  # Include stack trace
```

### Performance Logging

```python
from pghatch.logging_config import get_logger, log_performance

logger = get_logger(__name__)

@log_performance(logger, "database operation")
async def my_database_operation():
    # Your code here
    pass
```

### Contextual Logging

```python
from pghatch.logging_config import get_logger

# Add context to all log messages from this logger
context = {"user_id": "123", "request_id": "abc-def"}
logger = get_logger(__name__, context=context)
```

## Log Format

### Development Environment
```
2024-01-15 10:30:45 | pghatch.api        | INFO     | FastAPI application created successfully
```

### Production Environment
```
2024-01-15 10:30:45 | pghatch.api | INFO | FastAPI application created successfully | /path/to/file.py:123
```

## Logging Locations

### API Layer (`pghatch/api.py`)
- HTTP request/response logging
- Application startup/shutdown
- Middleware operations
- Error handling

### Router Layer (`pghatch/router/router.py`)
- Schema router initialization
- Database connection pool management
- Schema change detection
- Route setup and teardown

### Introspection Layer (`pghatch/introspection/`)
- Database introspection operations
- Query execution timing
- Metadata parsing
- Constraint validation warnings

### Resolver Layer (`pghatch/router/resolver/`)
- Table/view resolver creation
- Endpoint mounting
- Query generation and execution
- Result processing

## Best Practices

### 1. Use Appropriate Log Levels
```python
# Good
logger.debug("Executing SQL: %s", sql_query)
logger.info("Created %d resolvers for schema: %s", count, schema)
logger.warning("Schema change detected, restarting router")
logger.error("Database connection failed: %s", error, exc_info=True)

# Avoid
logger.info("Executing SQL: %s", sql_query)  # Too verbose for INFO
logger.error("Schema change detected")  # Not an error
```

### 2. Include Relevant Context
```python
# Good
logger.info("Query executed successfully for %s.%s, returned %d rows",
           schema, table, row_count)

# Less helpful
logger.info("Query executed successfully")
```

### 3. Use Structured Logging
```python
# Good
logger.error("Query execution failed for %s.%s: %s",
            schema, table, str(error), exc_info=True)

# Avoid string formatting in log messages
logger.error(f"Query execution failed for {schema}.{table}: {error}")
```

### 4. Performance Logging
```python
# Use the decorator for timing operations
@log_performance(logger, "database introspection")
async def introspect_database():
    # Implementation
    pass
```

### 5. Exception Logging
```python
try:
    # Some operation
    pass
except Exception as e:
    logger.error("Operation failed: %s", str(e), exc_info=True)
    raise  # Re-raise the exception
```

## Configuration Examples

### Development Setup
```bash
export PGHATCH_LOG_LEVEL=DEBUG
export PGHATCH_ENV=development
```

### Production Setup
```bash
export PGHATCH_LOG_LEVEL=INFO
export PGHATCH_ENV=production
export PGHATCH_LOG_FILE=/var/log/pghatch/app.log
```

### Testing Setup
```bash
export PGHATCH_LOG_LEVEL=WARNING
export PGHATCH_ENV=development
```

## File Logging

When `PGHATCH_LOG_FILE` is set, logs are written to both console and file:
- File logs use detailed formatting with file paths and line numbers
- Log files are rotated when they reach 10MB
- Up to 5 backup files are kept
- File logging includes all log levels configured

## Third-Party Library Logging

The logging configuration also manages third-party library log levels:
- **uvicorn**: INFO level
- **fastapi**: INFO level
- **asyncpg**: WARNING level (to reduce connection noise)

## Troubleshooting

### No Logs Appearing
1. Check `PGHATCH_LOG_LEVEL` environment variable
2. Ensure logger is created with `get_logger(__name__)`
3. Verify log level is appropriate for your messages

### Too Verbose Logging
1. Increase log level (DEBUG → INFO → WARNING → ERROR)
2. Check third-party library log levels
3. Review log statements in your code

### Performance Issues
1. Avoid expensive operations in log message formatting
2. Use lazy evaluation: `logger.debug("Message: %s", expensive_operation())`
3. Consider reducing log level in production

## Integration with Monitoring

The structured logging format makes it easy to integrate with log aggregation systems:
- **ELK Stack**: Parse structured logs with Logstash
- **Prometheus**: Extract metrics from log patterns
- **Grafana**: Visualize log-based metrics
- **Sentry**: Automatic error tracking with stack traces
