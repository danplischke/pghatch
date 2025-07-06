"""
Unit tests for pghatch.introspection.introspection module.
Tests the core introspection functionality including database metadata extraction.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import json

from pghatch.introspection.introspection import Introspection, make_introspection_query
from pghatch.introspection.tables import PgNamespace, PgClass, PgType, PgAttribute, PgProc


class TestIntrospection:
    """Test the Introspection class functionality."""

    def test_introspection_initialization(self, sample_introspection_data):
        """Test Introspection object initialization with valid data."""
        introspection = Introspection.model_validate(sample_introspection_data)

        assert introspection.database.datname == "test_db"
        assert len(introspection.namespaces) == 2
        assert len(introspection.classes) == 1
        assert len(introspection.attributes) == 1
        assert introspection.current_user == "postgres"
        assert introspection.pg_version == "PostgreSQL 15.0"
        assert introspection.introspection_version == 1

    def test_introspection_catalog_mapping(self, sample_introspection_data):
        """Test that catalog OID mapping is set up correctly."""
        introspection = Introspection.model_validate(sample_introspection_data)

        assert introspection.PG_CLASS == "1259"
        assert introspection.PG_PROC == "1255"
        assert introspection.PG_TYPE == "1247"
        assert introspection.PG_NAMESPACE == "2615"
        assert introspection.PG_CONSTRAINT == "2606"

    def test_introspection_invalid_catalog_mapping(self, sample_introspection_data):
        """Test that initialization fails with invalid catalog mapping."""
        # Remove a required catalog entry
        sample_introspection_data["catalog_by_oid"].pop("1259")  # pg_class

        with pytest.raises(ValueError, match="Invalid introspection results"):
            Introspection.model_validate(sample_introspection_data)

    def test_get_namespace(self, sample_introspection_data):
        """Test getting namespace by OID."""
        introspection = Introspection.model_validate(sample_introspection_data)

        namespace = introspection.get_namespace("2200")
        assert namespace is not None
        assert namespace.nspname == "public"

        namespace = introspection.get_namespace("16384")
        assert namespace is not None
        assert namespace.nspname == "test_schema"

        # Test non-existent namespace
        namespace = introspection.get_namespace("99999")
        assert namespace is None

    def test_get_type(self, sample_introspection_data):
        """Test getting type by OID."""
        introspection = Introspection.model_validate(sample_introspection_data)

        pg_type = introspection.get_type("23")
        assert pg_type is not None
        assert pg_type.typname == "int4"
        assert pg_type.typcategory == "N"

        # Test non-existent type
        pg_type = introspection.get_type("99999")
        assert pg_type is None

    def test_get_class(self, sample_introspection_data):
        """Test getting class by OID."""
        introspection = Introspection.model_validate(sample_introspection_data)

        pg_class = introspection.get_class("16385")
        assert pg_class is not None
        assert pg_class.relname == "users"
        assert pg_class.relkind == "r"

        # Test non-existent class
        pg_class = introspection.get_class("99999")
        assert pg_class is None

    def test_get_role(self, sample_introspection_data):
        """Test getting role by OID."""
        introspection = Introspection.model_validate(sample_introspection_data)

        role = introspection.get_role("10")
        assert role is not None
        assert role.rolname == "postgres"
        assert role.rolsuper is True

        # Test non-existent role
        role = introspection.get_role("99999")
        assert role is None

    def test_get_attributes(self, sample_introspection_data):
        """Test getting attributes by relation OID."""
        # Add more attributes to test data
        sample_introspection_data["attributes"].extend([
            {
                "attrelid": "16385",
                "attname": "name",
                "atttypid": "25",  # text type
                "attlen": -1,
                "attnum": 2,
                "attcacheoff": -1,
                "atttypmod": -1,
                "attndims": 0,
                "attbyval": False,
                "attalign": "i",
                "attstorage": "x",
                "attcompression": "",
                "attnotnull": True,
                "atthasdef": False,
                "atthasmissing": False,
                "attidentity": "",
                "attgenerated": "",
                "attisdropped": False,
                "attislocal": True,
                "attinhcount": 0,
                "attcollation": "100",
                "attstattarget": -1,
                "attacl": None,
                "attoptions": None,
                "attfdwoptions": None,
                "attmissingval": None
            }
        ])

        introspection = Introspection.model_validate(sample_introspection_data)

        attributes = introspection.get_attributes("16385")
        assert len(attributes) == 2
        assert attributes[0].attname == "id"
        assert attributes[0].attnum == 1
        assert attributes[1].attname == "name"
        assert attributes[1].attnum == 2

        # Test non-existent relation
        attributes = introspection.get_attributes("99999")
        assert len(attributes) == 0

    def test_get_constraints(self, sample_introspection_data):
        """Test getting constraints by relation OID."""
        # Add constraint to test data
        sample_introspection_data["constraints"] = [
            {
                "oid": "16400",
                "conname": "users_pkey",
                "connamespace": "16384",
                "contype": "p",
                "condeferrable": False,
                "condeferred": False,
                "convalidated": True,
                "conrelid": "16385",
                "contypid": "0",
                "conindid": "16401",
                "conparentid": "0",
                "confrelid": "0",
                "confupdtype": "",
                "confdeltype": "",
                "confmatchtype": "",
                "conislocal": True,
                "coninhcount": 0,
                "connoinherit": False,
                "conkey": [1],
                "confkey": None,
                "conpfeqop": None,
                "conppeqop": None,
                "conffeqop": None,
                "confdelsetcols": None,
                "conexclop": None,
                "conbin": None
            }
        ]

        introspection = Introspection.model_validate(sample_introspection_data)

        constraints = introspection.get_constraints("16385")
        assert len(constraints) == 1
        assert constraints[0].conname == "users_pkey"
        assert constraints[0].contype == "p"

        # Test non-existent relation
        constraints = introspection.get_constraints("99999")
        assert len(constraints) == 0

    def test_get_foreign_constraints(self, sample_introspection_data):
        """Test getting foreign key constraints by referenced relation OID."""
        # Add foreign key constraint to test data
        sample_introspection_data["constraints"] = [
            {
                "oid": "16402",
                "conname": "posts_user_id_fkey",
                "connamespace": "16384",
                "contype": "f",
                "condeferrable": False,
                "condeferred": False,
                "convalidated": True,
                "conrelid": "16386",  # posts table
                "contypid": "0",
                "conindid": "0",
                "conparentid": "0",
                "confrelid": "16385",  # references users table
                "confupdtype": "a",
                "confdeltype": "c",
                "confmatchtype": "s",
                "conislocal": True,
                "coninhcount": 0,
                "connoinherit": False,
                "conkey": [4],  # user_id column
                "confkey": [1],  # id column in users
                "conpfeqop": [96],
                "conppeqop": [96],
                "conffeqop": [96],
                "confdelsetcols": None,
                "conexclop": None,
                "conbin": None
            }
        ]

        introspection = Introspection.model_validate(sample_introspection_data)

        foreign_constraints = introspection.get_foreign_constraints("16385")
        assert len(foreign_constraints) == 1
        assert foreign_constraints[0].conname == "posts_user_id_fkey"
        assert foreign_constraints[0].contype == "f"

    def test_get_enums(self, sample_introspection_data):
        """Test getting enum values by type OID."""
        # Add enum type and values to test data
        enum_type_oid = "16403"
        sample_introspection_data["types"].append({
            "oid": enum_type_oid,
            "typname": "user_status",
            "typnamespace": "16384",
            "typowner": "10",
            "typlen": 4,
            "typbyval": True,
            "typtype": "e",
            "typcategory": "E",
            "typispreferred": False,
            "typisdefined": True,
            "typdelim": ",",
            "typrelid": "0",
            "typsubscript": "0",
            "typelem": "0",
            "typarray": "16404",
            "typinput": "enum_in",
            "typoutput": "enum_out",
            "typreceive": "enum_recv",
            "typsend": "enum_send",
            "typmodin": "0",
            "typmodout": "0",
            "typanalyze": "0",
            "typalign": "i",
            "typstorage": "p",
            "typnotnull": False,
            "typbasetype": "0",
            "typtypmod": -1,
            "typndims": 0,
            "typcollation": "0",
            "typdefaultbin": None,
            "typdefault": None,
            "typacl": None
        })

        sample_introspection_data["enums"] = [
            {
                "oid": "16405",
                "enumtypid": enum_type_oid,
                "enumsortorder": 1.0,
                "enumlabel": "pending"
            },
            {
                "oid": "16406",
                "enumtypid": enum_type_oid,
                "enumsortorder": 2.0,
                "enumlabel": "active"
            },
            {
                "oid": "16407",
                "enumtypid": enum_type_oid,
                "enumsortorder": 3.0,
                "enumlabel": "suspended"
            }
        ]

        introspection = Introspection.model_validate(sample_introspection_data)

        enums = introspection.get_enums(enum_type_oid)
        assert len(enums) == 3
        assert enums[0].enumlabel == "pending"
        assert enums[1].enumlabel == "active"
        assert enums[2].enumlabel == "suspended"

        # Test non-existent enum type
        enums = introspection.get_enums("99999")
        assert len(enums) == 0

    def test_get_description(self, sample_introspection_data):
        """Test getting object descriptions."""
        # Add description to test data
        sample_introspection_data["descriptions"] = [
            {
                "objoid": "16385",
                "classoid": "1259",  # pg_class
                "objsubid": 0,
                "description": "Users table"
            },
            {
                "objoid": "16385",
                "classoid": "1259",  # pg_class
                "objsubid": 1,  # id column
                "description": "User ID"
            }
        ]

        introspection = Introspection.model_validate(sample_introspection_data)

        # Test table description
        description = introspection.get_description("1259", "16385", 0)
        assert description == "Users table"

        # Test column description
        description = introspection.get_description("1259", "16385", 1)
        assert description == "User ID"

        # Test non-existent description
        description = introspection.get_description("1259", "99999", 0)
        assert description is None

    def test_get_current_user(self, sample_introspection_data):
        """Test getting current user role."""
        introspection = Introspection.model_validate(sample_introspection_data)

        current_user = introspection.get_current_user()
        assert current_user is not None
        assert current_user.rolname == "postgres"

    def test_extension_resource_filtering(self, sample_introspection_data):
        """Test filtering of extension resources when include_extension_resources is False."""
        # Add extension and dependencies to test data
        sample_introspection_data["extensions"] = [
            {
                "oid": "16408",
                "extname": "test_extension",
                "extowner": "10",
                "extnamespace": "16384",
                "extrelocatable": True,
                "extversion": "1.0",
                "extconfig": None,
                "extcondition": None
            }
        ]

        # Add a procedure that depends on the extension
        proc_oid = "16409"
        sample_introspection_data["procs"] = [
            {
                "oid": proc_oid,
                "proname": "extension_function",
                "pronamespace": "16384",
                "proowner": "10",
                "prolang": "12",
                "procost": 1.0,
                "prorows": 0.0,
                "provariadic": "0",
                "prosupport": "0",
                "prokind": "f",
                "prosecdef": False,
                "proleakproof": False,
                "proisstrict": True,
                "proretset": False,
                "provolatile": "i",
                "proparallel": "s",
                "pronargs": 0,
                "pronargdefaults": 0,
                "prorettype": "23",
                "proargtypes": [],
                "prosrc": "SELECT 1;",
                "proallargtypes": None,
                "proargmodes": None,
                "proargnames": None,
                "proargdefaults": None,
                "protrftypes": None,
                "probin": None,
                "prosqlbody": None,
                "proconfig": None,
                "proacl": None
            }
        ]

        # Add dependency linking the procedure to the extension
        sample_introspection_data["depends"] = [
            {
                "classid": "1255",  # pg_proc
                "objid": proc_oid,
                "objsubid": 0,
                "refclassid": "1255",  # pg_extension (should be correct OID)
                "refobjid": "16408",
                "refobjsubid": 0,
                "deptype": "e"  # extension dependency
            }
        ]

        # Update catalog mapping to include pg_extension
        sample_introspection_data["catalog_by_oid"]["1255"] = "pg_extension"

        # Test with extension resources excluded
        sample_introspection_data["include_extension_resources"] = False
        introspection = Introspection.model_validate(sample_introspection_data)

        # The extension-dependent procedure should be filtered out
        # Note: This test might need adjustment based on actual filtering logic
        assert len(introspection.procs) == 0  # Should be filtered out

    def test_del_items_utility_method(self):
        """Test the del_items utility method."""
        test_objects = [
            MagicMock(oid="1"),
            MagicMock(oid="2"),
            MagicMock(oid="3")
        ]

        # Delete items with oid "2"
        Introspection.del_items(["2"], test_objects, "oid")

        assert len(test_objects) == 2
        assert test_objects[0].oid == "1"
        assert test_objects[1].oid == "3"


class TestMakeIntrospectionQuery:
    """Test the make_introspection_query function."""

    @pytest.mark.asyncio
    async def test_make_introspection_query_success(self, mock_asyncpg_connection, sample_introspection_data):
        """Test successful introspection query execution."""
        # Mock the database response
        mock_asyncpg_connection.fetchval.return_value = json.dumps(sample_introspection_data)

        result = await make_introspection_query(mock_asyncpg_connection)

        assert isinstance(result, Introspection)
        assert result.database.datname == "test_db"
        assert len(result.namespaces) == 2

        # Verify the query was executed
        mock_asyncpg_connection.fetchval.assert_called_once()
        call_args = mock_asyncpg_connection.fetchval.call_args[0]
        assert "json_build_object" in call_args[0]
        assert "pg_catalog.pg_database" in call_args[0]

    @pytest.mark.asyncio
    async def test_make_introspection_query_no_result(self, mock_asyncpg_connection):
        """Test introspection query with no result."""
        mock_asyncpg_connection.fetchval.return_value = None

        with pytest.raises(ValueError, match="No introspection data found"):
            await make_introspection_query(mock_asyncpg_connection)

    @pytest.mark.asyncio
    async def test_make_introspection_query_invalid_json(self, mock_asyncpg_connection):
        """Test introspection query with invalid JSON response."""
        mock_asyncpg_connection.fetchval.return_value = "invalid json"

        with pytest.raises(Exception):  # JSON decode error
            await make_introspection_query(mock_asyncpg_connection)

    @pytest.mark.asyncio
    async def test_make_introspection_query_database_error(self, mock_asyncpg_connection):
        """Test introspection query with database error."""
        mock_asyncpg_connection.fetchval.side_effect = Exception("Database connection error")

        with pytest.raises(Exception, match="Database connection error"):
            await make_introspection_query(mock_asyncpg_connection)

    @pytest.mark.asyncio
    async def test_introspection_query_structure(self, mock_asyncpg_connection, sample_introspection_data):
        """Test that the introspection query has the expected structure."""
        mock_asyncpg_connection.fetchval.return_value = json.dumps(sample_introspection_data)

        await make_introspection_query(mock_asyncpg_connection)

        # Get the executed query
        call_args = mock_asyncpg_connection.fetchval.call_args[0]
        query = call_args[0]

        # Verify key components of the query
        expected_components = [
            "json_build_object",
            "pg_catalog.pg_database",
            "pg_catalog.pg_namespace",
            "pg_catalog.pg_class",
            "pg_catalog.pg_attribute",
            "pg_catalog.pg_constraint",
            "pg_catalog.pg_proc",
            "pg_catalog.pg_roles",
            "pg_catalog.pg_type",
            "current_user",
            "version()"
        ]

        for component in expected_components:
            assert component in query, f"Expected '{component}' in introspection query"


@pytest.mark.integration
class TestIntrospectionIntegration:
    """Integration tests for introspection with real database."""

    @pytest.mark.asyncio
    async def test_real_introspection_query(self, clean_db_connection, setup_test_schema):
        """Test introspection query against real database."""
        introspection = await make_introspection_query(clean_db_connection)

        assert isinstance(introspection, Introspection)
        assert introspection.database.datname == "postgres"
        assert introspection.current_user == "postgres"
        assert len(introspection.namespaces) > 0

        # Find test schema
        test_schema = introspection.get_namespace(None)
        for ns in introspection.namespaces:
            if ns.nspname == "test_schema":
                test_schema = ns
                break

        assert test_schema is not None
        assert test_schema.nspname == "test_schema"

    @pytest.mark.asyncio
    async def test_introspection_with_test_objects(self, clean_db_connection, setup_test_schema):
        """Test introspection includes test schema objects."""
        introspection = await make_introspection_query(clean_db_connection)

        # Find test schema namespace
        test_schema_oid = None
        for ns in introspection.namespaces:
            if ns.nspname == "test_schema":
                test_schema_oid = ns.oid
                break

        assert test_schema_oid is not None

        # Check for test tables
        test_tables = [cls for cls in introspection.classes
                      if cls.relnamespace == test_schema_oid and cls.relkind == "r"]
        table_names = [table.relname for table in test_tables]

        expected_tables = ["users", "posts", "user_profiles"]
        for expected_table in expected_tables:
            assert expected_table in table_names

        # Check for test functions
        test_functions = [proc for proc in introspection.procs
                         if proc.pronamespace == test_schema_oid]
        function_names = [func.proname for func in test_functions]

        expected_functions = ["get_user_count", "get_users_by_status", "get_active_users"]
        for expected_function in expected_functions:
            assert expected_function in function_names

    @pytest.mark.asyncio
    async def test_introspection_type_mapping(self, clean_db_connection, setup_test_schema):
        """Test that introspection correctly maps PostgreSQL types."""
        introspection = await make_introspection_query(clean_db_connection)

        # Find integer type
        int_type = None
        for typ in introspection.types:
            if typ.typname == "int4":
                int_type = typ
                break

        assert int_type is not None
        assert int_type.typcategory == "N"  # Numeric
        assert int_type.typlen == 4

        # Find text type
        text_type = None
        for typ in introspection.types:
            if typ.typname == "text":
                text_type = typ
                break

        assert text_type is not None
        assert text_type.typcategory == "S"  # String
        assert text_type.typlen == -1  # Variable length
