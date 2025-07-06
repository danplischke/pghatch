"""
Centralized logging configuration for pghatch.

This module provides a consistent logging setup across the entire application
with support for different environments and structured logging.
"""

import logging
import logging.config
import os
import sys
from typing import Dict, Any, Optional


class ContextFilter(logging.Filter):
    """Add contextual information to log records."""

    def __init__(self, context: Optional[Dict[str, Any]] = None):
        super().__init__()
        self.context = context or {}

    def filter(self, record: logging.LogRecord) -> bool:
        # Add context to the log record
        for key, value in self.context.items():
            setattr(record, key, value)
        return True


def get_log_level() -> str:
    """Get log level from environment variable or default to INFO."""
    return os.getenv("PGHATCH_LOG_LEVEL", "INFO").upper()


def get_log_format() -> str:
    """Get log format based on environment."""
    env = os.getenv("PGHATCH_ENV", "development").lower()

    if env == "production":
        # Structured format for production
        return "%(asctime)s | %(name)s | %(levelname)s | %(message)s | %(pathname)s:%(lineno)d"
    else:
        # Human-readable format for development
        return "%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s"


def get_logging_config() -> Dict[str, Any]:
    """Get the logging configuration dictionary."""
    log_level = get_log_level()
    log_format = get_log_format()

    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": log_format,
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "detailed": {
                "format": "%(asctime)s | %(name)s | %(levelname)s | %(funcName)s:%(lineno)d | %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": log_level,
                "formatter": "standard",
                "stream": sys.stdout,
            },
        },
        "loggers": {
            "pghatch": {
                "level": log_level,
                "handlers": ["console"],
                "propagate": False,
            },
            # Third-party loggers
            "uvicorn": {
                "level": "INFO",
                "handlers": ["console"],
                "propagate": False,
            },
            "fastapi": {
                "level": "INFO",
                "handlers": ["console"],
                "propagate": False,
            },
            "asyncpg": {
                "level": "WARNING",
                "handlers": ["console"],
                "propagate": False,
            },
        },
        "root": {
            "level": "WARNING",
            "handlers": ["console"],
        },
    }

    # Add file handler if log file path is specified
    log_file = os.getenv("PGHATCH_LOG_FILE")
    if log_file:
        config["handlers"]["file"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "level": log_level,
            "formatter": "detailed",
            "filename": log_file,
            "maxBytes": 10 * 1024 * 1024,  # 10MB
            "backupCount": 5,
        }
        # Add file handler to pghatch logger
        config["loggers"]["pghatch"]["handlers"].append("file")

    return config


def setup_logging() -> None:
    """Setup logging configuration for the application."""
    config = get_logging_config()
    logging.config.dictConfig(config)

    # Log the logging setup
    logger = logging.getLogger("pghatch.logging")
    logger.info("Logging configured with level: %s", get_log_level())

    if os.getenv("PGHATCH_LOG_FILE"):
        logger.info("File logging enabled: %s", os.getenv("PGHATCH_LOG_FILE"))


def get_logger(name: str, context: Optional[Dict[str, Any]] = None) -> logging.Logger:
    """
    Get a logger with the specified name and optional context.

    Args:
        name: Logger name (typically __name__ of the module)
        context: Optional context dictionary to add to all log records

    Returns:
        Configured logger instance
    """
    # Ensure the name starts with 'pghatch' for proper hierarchy
    if not name.startswith("pghatch"):
        if name == "__main__":
            name = "pghatch.main"
        else:
            name = f"pghatch.{name}"

    logger = logging.getLogger(name)

    # Add context filter if provided
    if context:
        context_filter = ContextFilter(context)
        logger.addFilter(context_filter)

    return logger


def log_performance(logger: logging.Logger, operation: str):
    """
    Decorator to log performance timing of operations.

    Args:
        logger: Logger instance to use
        operation: Description of the operation being timed
    """
    import functools
    import time

    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                duration = time.time() - start_time
                logger.debug("Operation '%s' completed in %.3fs", operation, duration)
                return result
            except Exception as e:
                duration = time.time() - start_time
                logger.error("Operation '%s' failed after %.3fs: %s", operation, duration, str(e))
                raise

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                logger.debug("Operation '%s' completed in %.3fs", operation, duration)
                return result
            except Exception as e:
                duration = time.time() - start_time
                logger.error("Operation '%s' failed after %.3fs: %s", operation, duration, str(e))
                raise

        # Return appropriate wrapper based on whether function is async
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


# Initialize logging when module is imported
if not logging.getLogger().handlers:
    setup_logging()
