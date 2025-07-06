"""
Unit tests for pghatch.router.resolver.table_resolver module.
Tests the TableViewResolver class functionality.
"""

import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from typing import List
from fastapi import APIRouter
from pydantic import BaseModel

from pghatch.router.resolver.table_resolver import TableViewResolver, TableViewLimit
from pghatch.introspection.introspection import Introspection
from pghatch.introspection.tables import PgClass, PgAttribute, PgNamespace, PgType


class TestTableViewLimit:
    """Test the TableViewLimit model."""

    def test_table_view_limit_default_values(self):
        """Test TableViewLimit with default values."""
        limit = TableViewLimit()

        assert limit.limit is None
        assert limit.offset is None

    def test_table_view_limit_with_values(self):
        """Test TableViewLimit with specified values."""
        limit = TableViewLimit(limit=10, offset=5)

        assert limit.limit == 10
        assert limit.offset == 5

    def test_table_view_limit_validation(self):
        """Test TableViewLimit field validation."""
        # Test with valid values
        limit = TableViewLimit(limit=100, offset=0)
        assert limit.limit == 100
        assert limit.offset == 0

        # Test with None values (should be allowed)
        limit = TableViewLimit(limit=None, offset=None)
        assert limit.limit is None
        assert limit.offset is None


class TestTableViewResolver:
    """Test the TableViewResolver class functionality."""

    def test_table_view_resolver_initialization(self):
        """Test TableViewResolver initialization."""
        # Mock introspection and related objects
        mock_introspection = MagicMock()

        # Mock PgClass
        mock_class = MagicMock()
        mock_class.relname = "test_table"
        mock_class.relkind = "r"

        # Mock PgNamespace
        mock_namespace = MagicMock()
        mock_namespace.nspname = "test_schema"

        mock_introspection.get_class.return_value = mock_class
        mock_introspection.get_namespace.return_value = mock_namespace

        # Mock attributes
        mock_attr1 = MagicMock()
        mock_attr1.attname = "id"
        mock_attr1.attisdropped = False
        mock_attr1.get_py_type.return_value = int
        mock_attr1.get_description.return_value = "ID field"

        mock_attr2 = MagicMock()
        mock_attr2.attname = "name"
        mock_attr2.attisdropped = False
        mock_attr2.get_py_type.return_value = str
        mock_attr2.get_description.return_value = "Name field"

        mock_introspection.get_attributes.return_value = [mock_attr1, mock_attr2]

        with patch('pghatch.router.resolver.table_resolver.create_model') as mock_create_model:
            mock_model = MagicMock()
            mock_create_model.return_value = mock_model

            resolver = TableViewResolver(oid="12345", introspection=mock_introspection)

            assert resolver.cls == mock_class
            assert resolver.name == "test_table"
            assert resolver.oid == "12345"
            assert resolver.schema == "test_schema"
            assert resolver.type == "r"
            assert resolver.fields == ["id", "name"]
            assert resolver.return_type == mock_model

    def test_table_view_resolver_initialization_missing_class(self):
        """Test TableViewResolver initialization with missing class."""
        mock_introspection = MagicMock()
        mock_introspection.get_class.return_value = None

        with pytest.raises(ValueError, match="Class with OID 99999 not found"):
            TableViewResolver(oid="99999", introspection=mock_introspection)

    def test_create_return_type(self):
        """Test the _create_return_type method."""
        mock_introspection = MagicMock()

        # Mock PgClass
        mock_class = MagicMock()
        mock_class.relname = "users"
        mock_class.relkind = "r"

        # Mock attributes with different types
        mock_attr1 = MagicMock()
        mock_attr1.attname = "id"
        mock_attr1.attisdropped = False
        mock_attr1.get_py_type.return_value = int

        mock_type1 = MagicMock()
        mock_type1.oid = "23"  # int4
        mock_attr1.get_type.return_value = mock_type1

        mock_attr2 = MagicMock()
        mock_attr2.attname = "email"
        mock_attr2.attisdropped = False
        mock_attr2.get_py_type.return_value = str

        mock_type2 = MagicMock()
        mock_type2.oid = "25"  # text
        mock_attr2.get_type.return_value = mock_type2

        # Mock dropped attribute (should be ignored)
        mock_attr3 = MagicMock()
        mock_attr3.attname = "dropped_field"
        mock_attr3.attisdropped = True

        mock_introspection.get_class.return_value = mock_class
        mock_introspection.get_attributes.return_value = [mock_attr1, mock_attr2, mock_attr3]
        mock_introspection.get_description.return_value = "Test description"
        mock_introspection.PG_CLASS = "1259"

        with patch('pghatch.router.resolver.table_resolver.create_model') as mock_create_model:
            with patch('pghatch.router.resolver.table_resolver.Field') as mock_field:
                mock_model = MagicMock()
                mock_create_model.return_value = mock_model
                mock_field.return_value = "mock_field"

                resolver = TableViewResolver(oid="12345", introspection=mock_introspection)

                # Verify create_model was called with correct parameters
                mock_create_model.assert_called_once()
                call_args = mock_create_model.call_args

                # Check model name (should be camelCase)
                assert call_args[0][0] == "Users"  # to_camel("users")

                # Check field definitions
                field_definitions = call_args[1]
                assert "id" in field_definitions
                assert "email" in field_definitions
                assert "dropped_field" not in field_definitions

                # Verify fields list
                assert resolver.fields == ["id", "email"]

    def test_mount_method(self):
        """Test the mount method."""
        # Setup resolver
        mock_introspection = MagicMock()
        mock_class = MagicMock()
        mock_class.relname = "test_table"
        mock_class.relkind = "r"
        mock_namespace = MagicMock()
        mock_namespace.nspname = "test_schema"

        mock_introspection.get_class.return_value = mock_class
        mock_introspection.get_namespace.return_value = mock_namespace
        mock_introspection.get_attributes.return_value = []

        with patch('pghatch.router.resolver.table_resolver.create_model') as mock_create_model:
            mock_model = MagicMock()
            mock_create_model.return_value = mock_model

            resolver = TableViewResolver(oid="12345", introspection=mock_introspection)

            # Mock router
            mock_router = MagicMock()

            resolver.mount(mock_router)

            # Verify add_api_route was called
            mock_router.add_api_route.assert_called_once()
            call_args = mock_router.add_api_route.call_args

            # Check route parameters
            assert call_args[0][0] == "/test_schema/test_table"  # path
            assert call_args[0][1] == resolver.resolve  # endpoint
            assert call_args[1]["methods"] == ["POST"]
            assert call_args[1]["summary"] == "Get data from test_schema.test_table"
            assert call_args[1]["description"] == "Fetches data from the table or view test_schema.test_table."

    @pytest.mark.asyncio
    async def test_resolve_method_without_limit(self):
        """Test the resolve method without limit parameters."""
        # Setup resolver
        mock_introspection = MagicMock()
        mock_class = MagicMock()
        mock_class.relname = "users"
        mock_class.relkind = "r"
        mock_namespace = MagicMock()
        mock_namespace.nspname = "public"

        mock_attr = MagicMock()
        mock_attr.attname = "id"
        mock_attr.attisdropped = False
        mock_attr.get_py_type.return_value = int

        mock_introspection.get_class.return_value = mock_class
        mock_introspection.get_namespace.return_value = mock_namespace
        mock_introspection.get_attributes.return_value = [mock_attr]

        with patch('pghatch.router.resolver.table_resolver.create_model') as mock_create_model:
            mock_model = MagicMock()
            mock_create_model.return_value = mock_model

            resolver = TableViewResolver(oid="12345", introspection=mock_introspection)

            # Mock database connection and results
            mock_conn = AsyncMock()
            mock_row = {"id": 1}
            mock_conn.fetch.return_value = [mock_row]

            with patch('asyncpg.connect', return_value=mock_conn):
                with patch('pghatch.router.resolver.table_resolver.RawStream') as mock_raw_stream:
                    mock_stream_instance = MagicMock()
                    mock_stream_instance.return_value = "SELECT id FROM public.users"
                    mock_raw_stream.return_value = mock_stream_instance

                    result = await resolver.resolve()

                    # Verify database connection
                    mock_conn.fetch.assert_called_once_with("SELECT id FROM public.users")
                    mock_conn.close.assert_called_once()

                    # Verify result
                    assert len(result) == 1

    @pytest.mark.asyncio
    async def test_resolve_method_with_limit(self):
        """Test the resolve method with limit parameters."""
        # Setup resolver
        mock_introspection = MagicMock()
        mock_class = MagicMock()
        mock_class.relname = "users"
        mock_class.relkind = "r"
        mock_namespace = MagicMock()
        mock_namespace.nspname = "public"

        mock_attr = MagicMock()
        mock_attr.attname = "id"
        mock_attr.attisdropped = False
        mock_attr.get_py_type.return_value = int

        mock_introspection.get_class.return_value = mock_class
        mock_introspection.get_namespace.return_value = mock_namespace
        mock_introspection.get_attributes.return_value = [mock_attr]

        with patch('pghatch.router.resolver.table_resolver.create_model') as mock_create_model:
            mock_model = MagicMock()
            mock_create_model.return_value = mock_model

            resolver = TableViewResolver(oid="12345", introspection=mock_introspection)

            # Mock database connection and results
            mock_conn = AsyncMock()
            mock_row = {"id": 1}
            mock_conn.fetch.return_value = [mock_row]

            # Create limit object
            limit = TableViewLimit(limit=10, offset=5)

            with patch('asyncpg.connect', return_value=mock_conn):
                with patch('pghatch.router.resolver.table_resolver.RawStream') as mock_raw_stream:
                    mock_stream_instance = MagicMock()
                    mock_stream_instance.return_value = "SELECT id FROM public.users LIMIT 10 OFFSET 5"
                    mock_raw_stream.return_value = mock_stream_instance

                    result = await resolver.resolve(limit=limit)

                    # Verify database connection
                    mock_conn.fetch.assert_called_once()
                    mock_conn.close.assert_called_once()

                    # Verify result
                    assert len(result) == 1

    @pytest.mark.asyncio
    async def test_resolve_method_with_partial_limit(self):
        """Test the resolve method with only limit (no offset)."""
        # Setup resolver
        mock_introspection = MagicMock()
        mock_class = MagicMock()
        mock_class.relname = "users"
        mock_class.relkind = "r"
        mock_namespace = MagicMock()
        mock_namespace.nspname = "public"

        mock_attr = MagicMock()
        mock_attr.attname = "id"
        mock_attr.attisdropped = False
        mock_attr.get_py_type.return_value = int

        mock_introspection.get_class.return_value = mock_class
        mock_introspection.get_namespace.return_value = mock_namespace
        mock_introspection.get_attributes.return_value = [mock_attr]

        with patch('pghatch.router.resolver.table_resolver.create_model') as mock_create_model:
            mock_model = MagicMock()
            mock_create_model.return_value = mock_model

            resolver = TableViewResolver(oid="12345", introspection=mock_introspection)

            # Mock database connection and results
            mock_conn = AsyncMock()
            mock_row = {"id": 1}
            mock_conn.fetch.return_value = [mock_row]

            # Create limit object with only limit
            limit = TableViewLimit(limit=5, offset=None)

            with patch('asyncpg.connect', return_value=mock_conn):
                with patch('pghatch.router.resolver.table_resolver.RawStream') as mock_raw_stream:
                    mock_stream_instance = MagicMock()
                    mock_stream_instance.return_value = "SELECT id FROM public.users LIMIT 5"
                    mock_raw_stream.return_value = mock_stream_instance

                    result = await resolver.resolve(limit=limit)

                    # Verify result
                    assert len(result) == 1

    def test_sql_generation_structure(self):
        """Test that SQL generation creates proper structure."""
        # Setup resolver
        mock_introspection = MagicMock()
        mock_class = MagicMock()
        mock_class.relname = "test_table"
        mock_class.relkind = "r"
        mock_namespace = MagicMock()
        mock_namespace.nspname = "test_schema"

        mock_attr1 = MagicMock()
        mock_attr1.attname = "id"
        mock_attr1.attisdropped = False
        mock_attr1.get_py_type.return_value = int

        mock_attr2 = MagicMock()
        mock_attr2.attname = "name"
        mock_attr2.attisdropped = False
        mock_attr2.get_py_type.return_value = str

        mock_introspection.get_class.return_value = mock_class
        mock_introspection.get_namespace.return_value = mock_namespace
        mock_introspection.get_attributes.return_value = [mock_attr1, mock_attr2]

        with patch('pghatch.router.resolver.table_resolver.create_model') as mock_create_model:
            mock_model = MagicMock()
            mock_create_model.return_value = mock_model

            resolver = TableViewResolver(oid="12345", introspection=mock_introspection)

            # Verify that fields are correctly set
            assert resolver.fields == ["id", "name"]
            assert resolver.schema == "test_schema"
            assert resolver.name == "test_table"
            assert resolver.type == "r"

    def test_different_relation_kinds(self):
        """Test TableViewResolver with different relation kinds."""
        mock_introspection = MagicMock()
        mock_namespace = MagicMock()
        mock_namespace.nspname = "test_schema"
        mock_introspection.get_namespace.return_value = mock_namespace
        mock_introspection.get_attributes.return_value = []

        relation_kinds = [
            ("r", "table"),
            ("v", "view"),
            ("m", "materialized_view"),
            ("f", "foreign_table"),
            ("p", "partitioned_table")
        ]

        for relkind, description in relation_kinds:
            mock_class = MagicMock()
            mock_class.relname = f"test_{description}"
            mock_class.relkind = relkind
            mock_introspection.get_class.return_value = mock_class

            with patch('pghatch.router.resolver.table_resolver.create_model') as mock_create_model:
                mock_model = MagicMock()
                mock_create_model.return_value = mock_model

                resolver = TableViewResolver(oid="12345", introspection=mock_introspection)

                assert resolver.type == relkind
                assert resolver.name == f"test_{description}"

    def test_attribute_ordering(self):
        """Test that attributes are processed in the correct order."""
        mock_introspection = MagicMock()
        mock_class = MagicMock()
        mock_class.relname = "test_table"
        mock_class.relkind = "r"
        mock_namespace = MagicMock()
        mock_namespace.nspname = "test_schema"

        # Create attributes with different attnum values
        mock_attr1 = MagicMock()
        mock_attr1.attname = "third_field"
        mock_attr1.attisdropped = False
        mock_attr1.get_py_type.return_value = str

        mock_attr2 = MagicMock()
        mock_attr2.attname = "first_field"
        mock_attr2.attisdropped = False
        mock_attr2.get_py_type.return_value = int

        mock_attr3 = MagicMock()
        mock_attr3.attname = "second_field"
        mock_attr3.attisdropped = False
        mock_attr3.get_py_type.return_value = str

        # Return attributes in order (get_attributes should return them ordered by attnum)
        mock_introspection.get_class.return_value = mock_class
        mock_introspection.get_namespace.return_value = mock_namespace
        mock_introspection.get_attributes.return_value = [mock_attr2, mock_attr3, mock_attr1]

        with patch('pghatch.router.resolver.table_resolver.create_model') as mock_create_model:
            mock_model = MagicMock()
            mock_create_model.return_value = mock_model

            resolver = TableViewResolver(oid="12345", introspection=mock_introspection)

            # Fields should be in the order returned by get_attributes
            assert resolver.fields == ["first_field", "second_field", "third_field"]


@pytest.mark.integration
class TestTableViewResolverIntegration:
    """Integration tests for TableViewResolver with real database."""

    @pytest.mark.asyncio
    async def test_table_resolver_with_real_data(self, introspection, setup_test_schema):
        """Test TableViewResolver with real database data."""
        # Find the users table
        users_class = None
        for cls in introspection.classes:
            if cls.relname == "users" and introspection.get_namespace(cls.relnamespace).nspname == "test_schema":
                users_class = cls
                break

        assert users_class is not None, "Users table not found in test schema"

        # Create resolver
        resolver = TableViewResolver(oid=users_class.oid, introspection=introspection)

        # Verify resolver properties
        assert resolver.name == "users"
        assert resolver.schema == "test_schema"
        assert resolver.type == "r"  # table

        # Verify fields
        expected_fields = ["id", "name", "email", "age", "salary", "is_active", "created_at", "metadata", "tags", "profile_picture"]
        for field in expected_fields:
            assert field in resolver.fields

        # Verify return type is a Pydantic model
        assert hasattr(resolver.return_type, 'model_fields')

        # Test mounting on router
        mock_router = MagicMock()
        resolver.mount(mock_router)

        # Verify route was added
        mock_router.add_api_route.assert_called_once()
        call_args = mock_router.add_api_route.call_args
        assert call_args[0][0] == "/test_schema/users"

    @pytest.mark.asyncio
    async def test_view_resolver_with_real_data(self, introspection, setup_test_schema):
        """Test TableViewResolver with a real view."""
        # Find the active_users view
        view_class = None
        for cls in introspection.classes:
            if cls.relname == "active_users" and introspection.get_namespace(cls.relnamespace).nspname == "test_schema":
                view_class = cls
                break

        assert view_class is not None, "Active users view not found in test schema"

        # Create resolver
        resolver = TableViewResolver(oid=view_class.oid, introspection=introspection)

        # Verify resolver properties
        assert resolver.name == "active_users"
        assert resolver.schema == "test_schema"
        assert resolver.type == "v"  # view

        # Verify fields (view should have subset of user fields)
        expected_fields = ["id", "name", "email", "created_at"]
        for field in expected_fields:
            assert field in resolver.fields

    @pytest.mark.asyncio
    async def test_materialized_view_resolver(self, introspection, setup_test_schema):
        """Test TableViewResolver with a materialized view."""
        # Find the user_stats materialized view
        mv_class = None
        for cls in introspection.classes:
            if cls.relname == "user_stats" and introspection.get_namespace(cls.relnamespace).nspname == "test_schema":
                mv_class = cls
                break

        assert mv_class is not None, "User stats materialized view not found in test schema"

        # Create resolver
        resolver = TableViewResolver(oid=mv_class.oid, introspection=introspection)

        # Verify resolver properties
        assert resolver.name == "user_stats"
        assert resolver.schema == "test_schema"
        assert resolver.type == "m"  # materialized view

        # Verify fields
        expected_fields = ["total_users", "active_users", "avg_age"]
        for field in expected_fields:
            assert field in resolver.fields

    @pytest.mark.asyncio
    async def test_resolver_with_complex_types(self, introspection, setup_test_schema):
        """Test TableViewResolver with complex PostgreSQL types."""
        # Find the user_profiles table (has composite types, enums, etc.)
        profiles_class = None
        for cls in introspection.classes:
            if cls.relname == "user_profiles" and introspection.get_namespace(cls.relnamespace).nspname == "test_schema":
                profiles_class = cls
                break

        assert profiles_class is not None, "User profiles table not found in test schema"

        # Create resolver
        resolver = TableViewResolver(oid=profiles_class.oid, introspection=introspection)

        # Verify resolver properties
        assert resolver.name == "user_profiles"
        assert resolver.schema == "test_schema"

        # Verify fields include complex types
        expected_fields = ["id", "user_id", "status", "home_address", "work_address"]
        for field in expected_fields:
            assert field in resolver.fields

        # Verify return type model has the fields
        model_fields = resolver.return_type.model_fields
        for field in expected_fields:
            assert field in model_fields
