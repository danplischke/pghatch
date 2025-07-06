"""
Unit tests for pghatch.router.router module.
Tests the SchemaRouter class functionality.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, call
from fastapi import FastAPI

from pghatch.router.router import SchemaRouter, WATCH_SQL
from pghatch.introspection.introspection import Introspection


class TestSchemaRouter:
    """Test the SchemaRouter class functionality."""

    def test_schema_router_initialization(self):
        """Test SchemaRouter initialization."""
        router = SchemaRouter(schema="test_schema")

        assert router.schema == "test_schema"
        assert router.initialized is False
        assert router.check_connection_interval == 5
        assert router._pool is None
        assert router._app is None
        assert router._watcher is None

    def test_schema_router_initialization_with_kwargs(self):
        """Test SchemaRouter initialization with additional kwargs."""
        router = SchemaRouter(
            schema="test_schema",
            prefix="/api/v1",
            tags=["test"]
        )

        assert router.schema == "test_schema"
        assert router.prefix == "/api/v1"
        assert router.tags == ["test"]

    @pytest.mark.asyncio
    async def test_lifespan_context_manager(self):
        """Test the lifespan context manager."""
        router = SchemaRouter(schema="test_schema")
        app = FastAPI()

        # Mock the pool creation and start method
        mock_pool = AsyncMock()
        mock_pool.close = AsyncMock()

        with patch('asyncpg.create_pool', return_value=mock_pool) as mock_create_pool:
            with patch.object(router, 'start', new_callable=AsyncMock) as mock_start:
                async with router.lifespan(app):
                    assert router._app == app
                    assert router._pool == mock_pool
                    assert router.initialized is True
                    mock_start.assert_called_once()

                # After exiting context, pool should be closed
                mock_pool.close.assert_called_once()
                mock_create_pool.assert_called_once_with(
                    dsn="postgres://postgres:postgres@localhost:5432/postgres"
                )

    @pytest.mark.asyncio
    async def test_watch_schema_success(self):
        """Test successful schema watching."""
        router = SchemaRouter(schema="test_schema")

        # Mock connection and pool
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        router._pool = mock_pool

        # Mock the restart method
        router.restart = AsyncMock()

        # Mock asyncio.sleep to prevent infinite loop in test
        with patch('asyncio.sleep', side_effect=[None, asyncio.CancelledError()]):
            with pytest.raises(asyncio.CancelledError):
                await router.watch_schema()

        # Verify SQL execution and listener setup
        mock_conn.execute.assert_has_calls([
            call(WATCH_SQL),
            call("SELECT 1")
        ])
        mock_conn.add_listener.assert_called_once_with('pghatch_watch', router.restart)

    @pytest.mark.asyncio
    async def test_watch_schema_connection_error(self):
        """Test schema watching with connection error."""
        router = SchemaRouter(schema="test_schema")

        # Mock connection that raises an exception
        mock_conn = AsyncMock()
        mock_conn.execute.side_effect = Exception("Connection lost")
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        router._pool = mock_pool

        # Mock the watch method to prevent infinite recursion
        router.watch = AsyncMock()

        await router.watch_schema()

        # Should call watch() when exception occurs
        router.watch.assert_called_once()

    @pytest.mark.asyncio
    async def test_watch_method(self):
        """Test the watch method."""
        router = SchemaRouter(schema="test_schema")

        # Mock existing watcher
        mock_existing_watcher = AsyncMock()
        router._watcher = mock_existing_watcher

        with patch('asyncio.create_task') as mock_create_task:
            mock_new_task = AsyncMock()
            mock_create_task.return_value = mock_new_task

            await router.watch()

            # Should cancel existing watcher and create new one
            mock_existing_watcher.cancel.assert_called_once()
            mock_create_task.assert_called_once()
            assert router._watcher == mock_new_task

    @pytest.mark.asyncio
    async def test_watch_method_no_existing_watcher(self):
        """Test the watch method with no existing watcher."""
        router = SchemaRouter(schema="test_schema")

        with patch('asyncio.create_task') as mock_create_task:
            mock_new_task = AsyncMock()
            mock_create_task.return_value = mock_new_task

            await router.watch()

            # Should create new watcher without canceling
            mock_create_task.assert_called_once()
            assert router._watcher == mock_new_task

    @pytest.mark.asyncio
    async def test_restart_method(self):
        """Test the restart method."""
        router = SchemaRouter(schema="test_schema")

        with patch.object(router, 'start', new_callable=AsyncMock) as mock_start:
            await router.restart("a", "b", "c", "d")
            mock_start.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_method(self):
        """Test the start method."""
        router = SchemaRouter(schema="test_schema")

        # Mock app and pool
        mock_app = MagicMock()
        mock_app.routes = []
        mock_app.include_router = MagicMock()
        router._app = mock_app

        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        router._pool = mock_pool

        # Mock introspection data
        mock_introspection = MagicMock()

        # Mock classes (tables/views)
        mock_class = MagicMock()
        mock_class.oid = "12345"
        mock_class.relnamespace = "test_ns_oid"
        mock_class.relkind = "r"  # table

        mock_namespace = MagicMock()
        mock_namespace.nspname = "test_schema"

        mock_introspection.classes = [mock_class]
        mock_introspection.procs = []
        mock_introspection.get_namespace.return_value = mock_namespace

        # Mock procedures
        mock_proc = MagicMock()
        mock_proc.oid = "67890"
        mock_proc.pronamespace = "test_ns_oid"
        mock_proc.prokind = "f"  # function

        mock_introspection.procs = [mock_proc]

        with patch('pghatch.router.router.make_introspection_query', return_value=mock_introspection):
            with patch('pghatch.router.router.TableViewResolver') as mock_table_resolver:
                with patch('pghatch.router.router.ProcResolver') as mock_proc_resolver:
                    with patch.object(router, 'watch', new_callable=AsyncMock) as mock_watch:
                        mock_table_instance = MagicMock()
                        mock_table_resolver.return_value = mock_table_instance

                        mock_proc_instance = MagicMock()
                        mock_proc_resolver.return_value = mock_proc_instance

                        await router.start()

                        # Verify table resolver was created and mounted
                        mock_table_resolver.assert_called_once_with(
                            oid="12345",
                            introspection=mock_introspection
                        )
                        mock_table_instance.mount.assert_called_once_with(router)

                        # Verify proc resolver was created and mounted
                        mock_proc_resolver.assert_called_once_with(
                            oid="67890",
                            introspection=mock_introspection
                        )
                        mock_proc_instance.mount.assert_called_once_with(router)

                        # Verify watch was called
                        mock_watch.assert_called_once()

                        # Verify app router inclusion
                        mock_app.include_router.assert_called_once_with(router)

                        # Verify OpenAPI schema reset
                        assert mock_app.openapi_schema is None

    @pytest.mark.asyncio
    async def test_start_method_with_existing_routes(self):
        """Test the start method with existing routes that need cleanup."""
        router = SchemaRouter(schema="test_schema")

        # Mock app with existing routes
        mock_app = MagicMock()
        existing_route = MagicMock()
        mock_app.routes = [existing_route]
        mock_app.include_router = MagicMock()
        router._app = mock_app

        # Add existing route to router
        router.routes = [existing_route]

        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        router._pool = mock_pool

        # Mock introspection with no objects
        mock_introspection = MagicMock()
        mock_introspection.classes = []
        mock_introspection.procs = []

        with patch('pghatch.router.router.make_introspection_query', return_value=mock_introspection):
            with patch.object(router, 'watch', new_callable=AsyncMock):
                await router.start()

                # Verify route cleanup
                assert len(router.routes) == 0
                assert len(mock_app.routes) == 0

    @pytest.mark.asyncio
    async def test_start_method_filters_schema(self):
        """Test that start method only processes objects from the specified schema."""
        router = SchemaRouter(schema="target_schema")

        mock_app = MagicMock()
        mock_app.routes = []
        mock_app.include_router = MagicMock()
        router._app = mock_app

        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        router._pool = mock_pool

        # Mock introspection with objects from different schemas
        mock_introspection = MagicMock()

        # Class in target schema
        mock_class_target = MagicMock()
        mock_class_target.oid = "12345"
        mock_class_target.relnamespace = "target_ns_oid"
        mock_class_target.relkind = "r"

        # Class in different schema
        mock_class_other = MagicMock()
        mock_class_other.oid = "54321"
        mock_class_other.relnamespace = "other_ns_oid"
        mock_class_other.relkind = "r"

        mock_introspection.classes = [mock_class_target, mock_class_other]
        mock_introspection.procs = []

        # Mock namespace lookup
        def mock_get_namespace(oid):
            if oid == "target_ns_oid":
                ns = MagicMock()
                ns.nspname = "target_schema"
                return ns
            elif oid == "other_ns_oid":
                ns = MagicMock()
                ns.nspname = "other_schema"
                return ns
            return None

        mock_introspection.get_namespace.side_effect = mock_get_namespace

        with patch('pghatch.router.router.make_introspection_query', return_value=mock_introspection):
            with patch('pghatch.router.router.TableViewResolver') as mock_table_resolver:
                with patch.object(router, 'watch', new_callable=AsyncMock):
                    await router.start()

                    # Should only create resolver for target schema object
                    mock_table_resolver.assert_called_once_with(
                        oid="12345",
                        introspection=mock_introspection
                    )

    @pytest.mark.asyncio
    async def test_start_method_filters_relation_kinds(self):
        """Test that start method only processes supported relation kinds."""
        router = SchemaRouter(schema="test_schema")

        mock_app = MagicMock()
        mock_app.routes = []
        mock_app.include_router = MagicMock()
        router._app = mock_app

        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        router._pool = mock_pool

        mock_introspection = MagicMock()

        # Create classes with different relkinds
        supported_kinds = ["r", "v", "m", "f", "p"]  # table, view, materialized view, foreign table, partitioned table
        unsupported_kinds = ["i", "S", "c"]  # index, sequence, composite type

        mock_classes = []
        for i, kind in enumerate(supported_kinds + unsupported_kinds):
            mock_class = MagicMock()
            mock_class.oid = f"oid_{i}"
            mock_class.relnamespace = "test_ns_oid"
            mock_class.relkind = kind
            mock_classes.append(mock_class)

        mock_introspection.classes = mock_classes
        mock_introspection.procs = []

        mock_namespace = MagicMock()
        mock_namespace.nspname = "test_schema"
        mock_introspection.get_namespace.return_value = mock_namespace

        with patch('pghatch.router.router.make_introspection_query', return_value=mock_introspection):
            with patch('pghatch.router.router.TableViewResolver') as mock_table_resolver:
                with patch.object(router, 'watch', new_callable=AsyncMock):
                    await router.start()

                    # Should only create resolvers for supported kinds
                    assert mock_table_resolver.call_count == len(supported_kinds)

                    # Verify the OIDs of called resolvers
                    called_oids = [call[1]['oid'] for call in mock_table_resolver.call_args_list]
                    expected_oids = [f"oid_{i}" for i in range(len(supported_kinds))]
                    assert called_oids == expected_oids

    @pytest.mark.asyncio
    async def test_start_method_filters_procedure_kinds(self):
        """Test that start method only processes supported procedure kinds."""
        router = SchemaRouter(schema="test_schema")

        mock_app = MagicMock()
        mock_app.routes = []
        mock_app.include_router = MagicMock()
        router._app = mock_app

        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        router._pool = mock_pool

        mock_introspection = MagicMock()
        mock_introspection.classes = []

        # Create procedures with different prokind values
        supported_kinds = ["f", "p"]  # function, procedure
        unsupported_kinds = ["a", "w"]  # aggregate, window function

        mock_procs = []
        for i, kind in enumerate(supported_kinds + unsupported_kinds):
            mock_proc = MagicMock()
            mock_proc.oid = f"proc_oid_{i}"
            mock_proc.pronamespace = "test_ns_oid"
            mock_proc.prokind = kind
            mock_procs.append(mock_proc)

        mock_introspection.procs = mock_procs

        mock_namespace = MagicMock()
        mock_namespace.nspname = "test_schema"
        mock_introspection.get_namespace.return_value = mock_namespace

        with patch('pghatch.router.router.make_introspection_query', return_value=mock_introspection):
            with patch('pghatch.router.router.ProcResolver') as mock_proc_resolver:
                with patch.object(router, 'watch', new_callable=AsyncMock):
                    await router.start()

                    # Should only create resolvers for supported kinds
                    assert mock_proc_resolver.call_count == len(supported_kinds)

                    # Verify the OIDs of called resolvers
                    called_oids = [call[1]['oid'] for call in mock_proc_resolver.call_args_list]
                    expected_oids = [f"proc_oid_{i}" for i in range(len(supported_kinds))]
                    assert called_oids == expected_oids


class TestWatchSQL:
    """Test the WATCH_SQL constant."""

    def test_watch_sql_structure(self):
        """Test that WATCH_SQL contains expected components."""
        expected_components = [
            "drop schema if exists pghatch_watch cascade",
            "create schema pghatch_watch",
            "create function pghatch_watch.notify_watchers_ddl",
            "create function pghatch_watch.notify_watchers_drop",
            "create event trigger pghatch_watch_ddl",
            "create event trigger pghatch_watch_drop",
            "pg_notify",
            "pghatch_watch",
            "ddl_command_end",
            "sql_drop"
        ]

        sql_lower = WATCH_SQL.lower()
        for component in expected_components:
            assert component.lower() in sql_lower, f"Expected '{component}' in WATCH_SQL"

    def test_watch_sql_ddl_events(self):
        """Test that WATCH_SQL includes expected DDL events."""
        expected_events = [
            "ALTER TABLE",
            "CREATE TABLE",
            "DROP TABLE",
            "CREATE FUNCTION",
            "DROP FUNCTION",
            "CREATE VIEW",
            "DROP VIEW",
            "CREATE SCHEMA",
            "DROP SCHEMA"
        ]

        for event in expected_events:
            assert event in WATCH_SQL, f"Expected DDL event '{event}' in WATCH_SQL"


@pytest.mark.integration
class TestSchemaRouterIntegration:
    """Integration tests for SchemaRouter with real database."""

    @pytest.mark.asyncio
    async def test_schema_router_with_real_database(self, setup_test_schema):
        """Test SchemaRouter with real database connection."""
        router = SchemaRouter(schema="test_schema")
        app = FastAPI()

        # Test the lifespan context manager
        async with router.lifespan(app):
            assert router.initialized is True
            assert router._pool is not None
            assert router._app == app

            # Verify that routes were created
            assert len(router.routes) > 0

            # Check for expected table routes
            route_paths = [route.path for route in router.routes]
            expected_paths = [
                "/test_schema/users",
                "/test_schema/posts",
                "/test_schema/user_profiles"
            ]

            for expected_path in expected_paths:
                assert expected_path in route_paths

    @pytest.mark.asyncio
    async def test_schema_router_restart_functionality(self, setup_test_schema):
        """Test that restart functionality works correctly."""
        router = SchemaRouter(schema="test_schema")
        app = FastAPI()

        async with router.lifespan(app):
            initial_route_count = len(router.routes)

            # Trigger a restart
            await router.restart("mock", "notification", "payload", "data")

            # Routes should be regenerated
            assert len(router.routes) >= initial_route_count
            assert router.initialized is True

    @pytest.mark.asyncio
    async def test_schema_router_watch_setup(self, setup_test_schema):
        """Test that database watching is set up correctly."""
        router = SchemaRouter(schema="test_schema")
        app = FastAPI()

        async with router.lifespan(app):
            # Verify watcher task is created
            assert router._watcher is not None
            assert not router._watcher.done()

            # Test that watch schema creates the necessary database objects
            async with router._pool.acquire() as conn:
                # Check if pghatch_watch schema exists
                result = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'pghatch_watch')"
                )
                assert result is True

                # Check if event triggers exist
                triggers = await conn.fetch(
                    "SELECT evtname FROM pg_event_trigger WHERE evtname LIKE 'pghatch_watch%'"
                )
                trigger_names = [trigger['evtname'] for trigger in triggers]
                assert 'pghatch_watch_ddl' in trigger_names
                assert 'pghatch_watch_drop' in trigger_names

    @pytest.mark.asyncio
    async def test_schema_router_nonexistent_schema(self):
        """Test SchemaRouter behavior with non-existent schema."""
        router = SchemaRouter(schema="nonexistent_schema")
        app = FastAPI()

        async with router.lifespan(app):
            # Should not create any routes for non-existent schema
            assert len(router.routes) == 0

    @pytest.mark.asyncio
    async def test_schema_router_empty_schema(self, clean_db_connection):
        """Test SchemaRouter behavior with empty schema."""
        # Create an empty schema
        await clean_db_connection.execute("CREATE SCHEMA IF NOT EXISTS empty_schema")

        try:
            router = SchemaRouter(schema="empty_schema")
            app = FastAPI()

            async with router.lifespan(app):
                # Should not create any routes for empty schema
                assert len(router.routes) == 0
        finally:
            # Clean up
            await clean_db_connection.execute("DROP SCHEMA IF EXISTS empty_schema CASCADE")
