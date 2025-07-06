from fastapi import FastAPI, Request, Response
from fastapi.middleware.base import BaseHTTPMiddleware
import time
from typing import Callable

from pghatch.logging_config import get_logger
from pghatch.router.router import SchemaRouter

logger = get_logger(__name__)


class LoggingMiddleware(BaseHTTPMiddleware):
    """Middleware to log HTTP requests and responses."""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start_time = time.time()

        # Log incoming request
        logger.info(
            "Incoming request: %s %s from %s",
            request.method,
            request.url.path,
            request.client.host if request.client else "unknown"
        )

        try:
            response = await call_next(request)
            duration = time.time() - start_time

            # Log successful response
            logger.info(
                "Request completed: %s %s -> %d in %.3fs",
                request.method,
                request.url.path,
                response.status_code,
                duration
            )

            return response

        except Exception as e:
            duration = time.time() - start_time
            logger.error(
                "Request failed: %s %s -> %s in %.3fs",
                request.method,
                request.url.path,
                str(e),
                duration,
                exc_info=True
            )
            raise


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    logger.info("Creating FastAPI application")

    app = FastAPI(
        title="PGHatch",
        description="Create a REST API for your PostgreSQL database",
        version="0.0.1"
    )

    # Add logging middleware
    app.add_middleware(LoggingMiddleware)

    # Create and include router
    logger.info("Setting up schema router for 'public' schema")
    router = SchemaRouter(schema="public")
    app.include_router(router)

    logger.info("FastAPI application created successfully")
    return app


app = create_app()
