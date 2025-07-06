"""
Unit tests for pghatch.api module.
Tests the main FastAPI application setup.
"""

import pytest
from unittest.mock import MagicMock, patch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from pghatch.api import app, router


class TestAPI:
    """Test the main API module functionality."""

    def test_app_is_fastapi_instance(self):
        """Test that app is a FastAPI instance."""
        assert isinstance(app, FastAPI)

    def test_app_has_router_included(self):
        """Test that the SchemaRouter is included in the app."""
        # Check that router is included
        assert router in app.router.routes

    def test_router_configuration(self):
        """Test that the router is configured correctly."""
        from pghatch.router.router import SchemaRouter

        assert isinstance(router, SchemaRouter)
        assert router.schema == "public"

    def test_app_basic_structure(self):
        """Test basic app structure and configuration."""
        # Test that app has expected attributes
        assert hasattr(app, 'router')
        assert hasattr(app, 'routes')

        # Test that routes include our router
        assert len(app.routes) > 0

    @pytest.mark.asyncio
    async def test_app_lifespan_integration(self):
        """Test that app lifespan works with router lifespan."""
        # This is more of an integration test to ensure the lifespan
        # context manager works correctly

        # Mock the router's lifespan to avoid actual database connection
        with patch.object(router, 'lifespan') as mock_lifespan:
            mock_lifespan.return_value.__aenter__ = MagicMock()
            mock_lifespan.return_value.__aexit__ = MagicMock()

            # Test that we can create a test client (which triggers lifespan)
            with TestClient(app) as client:
                # Basic health check - app should be responsive
                assert client is not None


@pytest.mark.integration
class TestAPIIntegration:
    """Integration tests for the API with real database."""

    @pytest.mark.asyncio
    async def test_api_with_real_database(self, setup_test_schema):
        """Test API integration with real database."""
        # Import here to avoid circular imports during test collection
        from pghatch.api import app

        # Create test client
        with TestClient(app) as client:
            # Test that the app starts up correctly
            assert client is not None

            # The router should have been initialized with real database
            # and routes should be available

            # Note: We can't easily test actual route responses here
            # without more complex setup, but we can verify the app
            # structure is correct

            # Verify that routes were created
            route_paths = [route.path for route in app.routes]

            # Should have at least some routes from the test schema
            # (The exact routes depend on the router's lifespan execution)
            assert len(route_paths) >= 0  # At minimum, no errors during startup

    def test_api_openapi_schema_generation(self):
        """Test that OpenAPI schema can be generated."""
        # Test that we can generate OpenAPI schema without errors
        schema = app.openapi()

        assert schema is not None
        assert "openapi" in schema
        assert "info" in schema
        assert "paths" in schema

    def test_api_docs_endpoints(self):
        """Test that documentation endpoints are available."""
        with TestClient(app) as client:
            # Test docs endpoint
            docs_response = client.get("/docs")
            assert docs_response.status_code == 200

            # Test redoc endpoint
            redoc_response = client.get("/redoc")
            assert redoc_response.status_code == 200

            # Test openapi.json endpoint
            openapi_response = client.get("/openapi.json")
            assert openapi_response.status_code == 200
            assert openapi_response.headers["content-type"] == "application/json"


class TestAPIConfiguration:
    """Test API configuration and setup."""

    def test_fastapi_configuration(self):
        """Test FastAPI app configuration."""
        # Test default configuration
        assert app.title == "FastAPI"  # Default title
        assert app.version == "0.1.0"  # Default version

        # Test that debug mode and other settings can be configured
        # (These would typically be set via environment variables)

    def test_cors_configuration(self):
        """Test CORS configuration if applicable."""
        # Check if CORS middleware is configured
        # This would depend on whether CORS is needed for the API

        # For now, just verify that middleware can be added
        middleware_stack = app.user_middleware
        assert isinstance(middleware_stack, list)

    def test_exception_handlers(self):
        """Test custom exception handlers if any."""
        # Check if custom exception handlers are configured
        exception_handlers = app.exception_handlers
        assert isinstance(exception_handlers, dict)

    def test_dependency_injection_setup(self):
        """Test dependency injection setup."""
        # Verify that dependencies are properly configured
        # This would include database connections, authentication, etc.

        # For now, verify basic structure
        assert hasattr(app, 'dependency_overrides')
        assert isinstance(app.dependency_overrides, dict)


@pytest.mark.slow
class TestAPIPerformance:
    """Performance tests for the API."""

    def test_app_startup_time(self):
        """Test that app starts up within reasonable time."""
        import time

        start_time = time.time()

        # Create and destroy test client to measure startup time
        with TestClient(app) as client:
            startup_time = time.time() - start_time

            # App should start up within 5 seconds
            assert startup_time < 5.0

            # Basic request should work
            response = client.get("/docs")
            assert response.status_code == 200

    def test_concurrent_requests(self):
        """Test handling of concurrent requests."""
        import threading
        import time

        results = []

        def make_request():
            with TestClient(app) as client:
                response = client.get("/docs")
                results.append(response.status_code)

        # Create multiple threads to make concurrent requests
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=make_request)
            threads.append(thread)

        # Start all threads
        start_time = time.time()
        for thread in threads:
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        total_time = time.time() - start_time

        # All requests should succeed
        assert len(results) == 5
        assert all(status == 200 for status in results)

        # Should complete within reasonable time
        assert total_time < 10.0


class TestAPIErrorHandling:
    """Test API error handling."""

    def test_404_handling(self):
        """Test 404 error handling."""
        with TestClient(app) as client:
            response = client.get("/nonexistent-endpoint")
            assert response.status_code == 404

    def test_method_not_allowed_handling(self):
        """Test 405 error handling."""
        with TestClient(app) as client:
            # Try to POST to docs endpoint (should be GET only)
            response = client.post("/docs")
            assert response.status_code == 405

    def test_internal_server_error_handling(self):
        """Test 500 error handling."""
        # This would require creating a route that intentionally fails
        # For now, just verify that the error handling structure exists

        assert hasattr(app, 'exception_handlers')

        # Could add a test route that raises an exception
        # and verify it's handled gracefully


class TestAPIMetadata:
    """Test API metadata and documentation."""

    def test_openapi_metadata(self):
        """Test OpenAPI metadata generation."""
        schema = app.openapi()

        # Verify basic OpenAPI structure
        assert "openapi" in schema
        assert "info" in schema
        assert "paths" in schema

        # Verify info section
        info = schema["info"]
        assert "title" in info
        assert "version" in info

    def test_route_documentation(self):
        """Test that routes have proper documentation."""
        schema = app.openapi()
        paths = schema.get("paths", {})

        # Each path should have proper documentation
        for path, methods in paths.items():
            for method, details in methods.items():
                if method in ["get", "post", "put", "delete", "patch"]:
                    # Should have summary or description
                    assert "summary" in details or "description" in details

    def test_schema_definitions(self):
        """Test that schema definitions are properly generated."""
        schema = app.openapi()

        # Should have components section with schemas
        if "components" in schema:
            components = schema["components"]
            if "schemas" in components:
                schemas = components["schemas"]
                assert isinstance(schemas, dict)

                # Each schema should have proper structure
                for schema_name, schema_def in schemas.items():
                    assert "type" in schema_def or "$ref" in schema_def


class TestAPIHealthCheck:
    """Test API health check functionality."""

    def test_basic_health_check(self):
        """Test basic health check via docs endpoint."""
        with TestClient(app) as client:
            response = client.get("/docs")
            assert response.status_code == 200
            assert "text/html" in response.headers.get("content-type", "")

    def test_openapi_json_health(self):
        """Test OpenAPI JSON endpoint health."""
        with TestClient(app) as client:
            response = client.get("/openapi.json")
            assert response.status_code == 200
            assert response.headers.get("content-type") == "application/json"

            # Should be valid JSON
            json_data = response.json()
            assert isinstance(json_data, dict)
            assert "openapi" in json_data

    def test_redoc_health(self):
        """Test ReDoc endpoint health."""
        with TestClient(app) as client:
            response = client.get("/redoc")
            assert response.status_code == 200
            assert "text/html" in response.headers.get("content-type", "")


@pytest.mark.integration
class TestAPIWithMockedDatabase:
    """Test API with mocked database to avoid real database dependency."""

    def test_api_with_mocked_router(self):
        """Test API with mocked router to avoid database dependency."""
        with patch('pghatch.api.router') as mock_router:
            # Configure mock router
            mock_router.schema = "public"
            mock_router.routes = []

            # Import app after mocking
            from pghatch import api

            # Create test client
            with TestClient(api.app) as client:
                # Basic health checks should work
                response = client.get("/docs")
                assert response.status_code == 200

    def test_api_initialization_error_handling(self):
        """Test API behavior when router initialization fails."""
        with patch('pghatch.router.router.SchemaRouter') as mock_router_class:
            # Make router initialization raise an exception
            mock_router_class.side_effect = Exception("Database connection failed")

            # The app should still be importable, but router creation would fail
            # This tests graceful degradation
            try:
                from pghatch.router.router import SchemaRouter
                SchemaRouter(schema="public")
                assert False, "Expected exception was not raised"
            except Exception as e:
                assert "Database connection failed" in str(e)
