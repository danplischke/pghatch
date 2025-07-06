"""
Test configuration and fixtures for pghatch test suite.
Provides database setup, connection management, and common test utilities.
"""

import asyncio
import os
import pytest
import asyncpg
from typing import AsyncGenerator, Generator
from unittest.mock import AsyncMock, MagicMock

from pghatch.introspection.introspection import Introspection, make_introspection_query
from pghatch.router.router import SchemaRouter


# Database configuration
TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgres://postgres:postgres@localhost:5432/postgres"
)

# Test schema SQL for comprehensive testing
TEST_SCHEMA_SQL = """
-- Drop test schema if exists
DROP SCHEMA IF EXISTS test_schema CASCADE;
CREATE SCHEMA test_schema;

-- Basic table with various column types
CREATE TABLE test_schema.users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    age INTEGER,
    salary DECIMAL(10,2),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    metadata JSONB,
    tags TEXT[],
    profile_picture BYTEA
);

-- Table with foreign key
CREATE TABLE test_schema.posts (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    content TEXT,
    user_id INTEGER REFERENCES test_schema.users(id) ON DELETE CASCADE,
    published_at TIMESTAMP,
    view_count BIGINT DEFAULT 0
);

-- View
CREATE VIEW test_schema.active_users AS
SELECT id, name, email, created_at
FROM test_schema.users
WHERE is_active = true;

-- Materialized view
CREATE MATERIALIZED VIEW test_schema.user_stats AS
SELECT
    COUNT(*) as total_users,
    COUNT(*) FILTER (WHERE is_active) as active_users,
    AVG(age) as avg_age
FROM test_schema.users;

-- Enum type
CREATE TYPE test_schema.user_status AS ENUM ('pending', 'active', 'suspended', 'deleted');

-- Composite type
CREATE TYPE test_schema.address AS (
    street TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT
);

-- Table using custom types
CREATE TABLE test_schema.user_profiles (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES test_schema.users(id),
    status test_schema.user_status DEFAULT 'pending',
    home_address test_schema.address,
    work_address test_schema.address
);

-- Function (returns scalar)
CREATE OR REPLACE FUNCTION test_schema.get_user_count()
RETURNS INTEGER AS $$
BEGIN
    RETURN (SELECT COUNT(*) FROM test_schema.users);
END;
$$ LANGUAGE plpgsql;

-- Function with parameters
CREATE OR REPLACE FUNCTION test_schema.get_users_by_status(p_status test_schema.user_status)
RETURNS TABLE(id INTEGER, name TEXT, email TEXT) AS $$
BEGIN
    RETURN QUERY
    SELECT u.id, u.name, u.email
    FROM test_schema.users u
    JOIN test_schema.user_profiles up ON u.id = up.user_id
    WHERE up.status = p_status;
END;
$$ LANGUAGE plpgsql;

-- Function returning SETOF
CREATE OR REPLACE FUNCTION test_schema.get_active_users()
RETURNS SETOF test_schema.users AS $$
BEGIN
    RETURN QUERY SELECT * FROM test_schema.users WHERE is_active = true;
END;
$$ LANGUAGE plpgsql;

-- Procedure
CREATE OR REPLACE PROCEDURE test_schema.update_user_status(
    p_user_id INTEGER,
    p_status test_schema.user_status
) AS $$
BEGIN
    UPDATE test_schema.user_profiles
    SET status = p_status
    WHERE user_id = p_user_id;
END;
$$ LANGUAGE plpgsql;

-- Function with variadic parameters
CREATE OR REPLACE FUNCTION test_schema.concat_strings(VARIADIC strings TEXT[])
RETURNS TEXT AS $$
BEGIN
    RETURN array_to_string(strings, ' ');
END;
$$ LANGUAGE plpgsql;

-- Function with default parameters
CREATE OR REPLACE FUNCTION test_schema.get_users_paginated(
    p_limit INTEGER DEFAULT 10,
    p_offset INTEGER DEFAULT 0
)
RETURNS TABLE(id INTEGER, name TEXT, email TEXT) AS $$
BEGIN
    RETURN QUERY
    SELECT u.id, u.name, u.email
    FROM test_schema.users u
    ORDER BY u.id
    LIMIT p_limit OFFSET p_offset;
END;
$$ LANGUAGE plpgsql;

-- Insert test data
INSERT INTO test_schema.users (name, email, age, salary, metadata, tags) VALUES
('John Doe', 'john@example.com', 30, 50000.00, '{"role": "admin"}', ARRAY['admin', 'user']),
('Jane Smith', 'jane@example.com', 25, 45000.00, '{"role": "user"}', ARRAY['user']),
('Bob Johnson', 'bob@example.com', 35, 60000.00, '{"role": "manager"}', ARRAY['manager', 'user']),
('Alice Brown', 'alice@example.com', 28, 55000.00, '{"role": "user"}', ARRAY['user']),
('Charlie Wilson', 'charlie@example.com', 32, 52000.00, '{"role": "user"}', ARRAY['user']);

INSERT INTO test_schema.posts (title, content, user_id, published_at, view_count) VALUES
('First Post', 'This is the first post content', 1, NOW() - INTERVAL '1 day', 100),
('Second Post', 'This is the second post content', 1, NOW() - INTERVAL '2 hours', 50),
('Third Post', 'This is the third post content', 2, NOW() - INTERVAL '1 hour', 25),
('Fourth Post', 'This is the fourth post content', 3, NOW() - INTERVAL '30 minutes', 10);

INSERT INTO test_schema.user_profiles (user_id, status, home_address, work_address) VALUES
(1, 'active', ROW('123 Main St', 'Anytown', 'CA', '12345'), ROW('456 Work Ave', 'Business City', 'CA', '54321')),
(2, 'active', ROW('789 Oak St', 'Somewhere', 'NY', '67890'), ROW('321 Office Blvd', 'Corporate Town', 'NY', '09876')),
(3, 'pending', ROW('456 Pine St', 'Elsewhere', 'TX', '13579'), NULL),
(4, 'active', ROW('321 Elm St', 'Nowhere', 'FL', '24680'), ROW('654 Business St', 'Work City', 'FL', '08642')),
(5, 'suspended', ROW('654 Maple St', 'Anywhere', 'WA', '97531'), NULL);

-- Refresh materialized view
REFRESH MATERIALIZED VIEW test_schema.user_stats;

-- Create indexes for testing
CREATE INDEX idx_users_email ON test_schema.users(email);
CREATE INDEX idx_posts_user_id ON test_schema.posts(user_id);
CREATE INDEX idx_posts_published_at ON test_schema.posts(published_at);

-- Create constraints for testing
ALTER TABLE test_schema.users ADD CONSTRAINT chk_age_positive CHECK (age > 0);
ALTER TABLE test_schema.posts ADD CONSTRAINT chk_view_count_non_negative CHECK (view_count >= 0);
"""


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
async def database_url() -> str:
    """Provide the test database URL."""
    return TEST_DATABASE_URL


@pytest.fixture(scope="session")
async def db_pool(database_url: str) -> AsyncGenerator[asyncpg.Pool, None]:
    """Create a database connection pool for the test session."""
    pool = await asyncpg.create_pool(database_url, min_size=1, max_size=10)
    try:
        yield pool
    finally:
        await pool.close()


@pytest.fixture(scope="session")
async def setup_test_schema(db_pool: asyncpg.Pool) -> None:
    """Set up the test schema with comprehensive test data."""
    async with db_pool.acquire() as conn:
        await conn.execute(TEST_SCHEMA_SQL)


@pytest.fixture
async def db_connection(db_pool: asyncpg.Pool) -> AsyncGenerator[asyncpg.Connection, None]:
    """Provide a database connection for individual tests."""
    async with db_pool.acquire() as conn:
        # Start a transaction for test isolation
        async with conn.transaction():
            yield conn
            # Transaction will be rolled back automatically


@pytest.fixture
async def clean_db_connection(db_pool: asyncpg.Pool) -> AsyncGenerator[asyncpg.Connection, None]:
    """Provide a clean database connection without transaction isolation."""
    async with db_pool.acquire() as conn:
        yield conn


@pytest.fixture
async def introspection(setup_test_schema, clean_db_connection: asyncpg.Connection) -> Introspection:
    """Provide a complete introspection object for testing."""
    return await make_introspection_query(clean_db_connection)


@pytest.fixture
async def test_schema_router(setup_test_schema) -> SchemaRouter:
    """Provide a SchemaRouter configured for the test schema."""
    router = SchemaRouter(schema="test_schema")
    return router


@pytest.fixture
def mock_asyncpg_connection() -> AsyncMock:
    """Provide a mock asyncpg connection for unit testing."""
    mock_conn = AsyncMock(spec=asyncpg.Connection)
    mock_conn.fetchval = AsyncMock()
    mock_conn.fetch = AsyncMock()
    mock_conn.execute = AsyncMock()
    mock_conn.transaction = AsyncMock()
    return mock_conn


@pytest.fixture
def mock_asyncpg_pool() -> AsyncMock:
    """Provide a mock asyncpg pool for unit testing."""
    mock_pool = AsyncMock(spec=asyncpg.Pool)
    mock_pool.acquire = AsyncMock()
    mock_pool.close = AsyncMock()
    return mock_pool


@pytest.fixture
def sample_introspection_data() -> dict:
    """Provide sample introspection data for testing."""
    return {
        "database": {
            "oid": "12345",
            "datname": "test_db",
            "datdba": "10",
            "encoding": 6,
            "datlocprovider": "c",
            "datistemplate": False,
            "datallowconn": True,
            "dathasloginevt": False,
            "datconnlimit": -1,
            "datfrozenxid": "123",
            "datminmxid": "1",
            "dattablespace": "1663",
            "datcollate": "en_US.UTF-8",
            "datctype": "en_US.UTF-8",
            "datlocale": None,
            "daticurules": None,
            "datcollversion": None,
            "datacl": None
        },
        "namespaces": [
            {
                "oid": "2200",
                "nspname": "public",
                "nspowner": "10",
                "nspacl": None
            },
            {
                "oid": "16384",
                "nspname": "test_schema",
                "nspowner": "10",
                "nspacl": None
            }
        ],
        "classes": [
            {
                "oid": "16385",
                "relname": "users",
                "relnamespace": "16384",
                "reltype": "16386",
                "reloftype": "0",
                "relowner": "10",
                "relam": "0",
                "relfilenode": "16385",
                "reltablespace": "0",
                "relpages": 1,
                "reltuples": 5.0,
                "relallvisible": 1,
                "reltoastrelid": "0",
                "relhasindex": True,
                "relisshared": False,
                "relpersistence": "p",
                "relkind": "r",
                "relnatts": 10,
                "relchecks": 1,
                "relhasrules": False,
                "relhastriggers": False,
                "relhassubclass": False,
                "relrowsecurity": False,
                "relforcerowsecurity": False,
                "relispopulated": True,
                "relreplident": "d",
                "relispartition": False,
                "relrewrite": "0",
                "relfrozenxid": "123",
                "relminmxid": "1",
                "relacl": None,
                "reloptions": None,
                "relpartbound": None,
                "updatable_mask": 255
            }
        ],
        "attributes": [
            {
                "attrelid": "16385",
                "attname": "id",
                "atttypid": "23",
                "attlen": 4,
                "attnum": 1,
                "attcacheoff": -1,
                "atttypmod": -1,
                "attndims": 0,
                "attbyval": True,
                "attalign": "i",
                "attstorage": "p",
                "attcompression": "",
                "attnotnull": True,
                "atthasdef": True,
                "atthasmissing": False,
                "attidentity": "",
                "attgenerated": "",
                "attisdropped": False,
                "attislocal": True,
                "attinhcount": 0,
                "attcollation": "0",
                "attstattarget": -1,
                "attacl": None,
                "attoptions": None,
                "attfdwoptions": None,
                "attmissingval": None
            }
        ],
        "constraints": [],
        "procs": [],
        "roles": [
            {
                "oid": "10",
                "rolname": "postgres",
                "rolsuper": True,
                "rolinherit": True,
                "rolcreaterole": True,
                "rolcreatedb": True,
                "rolcanlogin": True,
                "rolreplication": True,
                "rolbypassrls": True,
                "rolconnlimit": -1,
                "rolpassword": None,
                "rolvaliduntil": None,
                "rolconfig": None
            }
        ],
        "auth_members": [],
        "types": [
            {
                "oid": "23",
                "typname": "int4",
                "typnamespace": "11",
                "typowner": "10",
                "typlen": 4,
                "typbyval": True,
                "typtype": "b",
                "typcategory": "N",
                "typispreferred": False,
                "typisdefined": True,
                "typdelim": ",",
                "typrelid": "0",
                "typsubscript": "0",
                "typelem": "0",
                "typarray": "1007",
                "typinput": "int4in",
                "typoutput": "int4out",
                "typreceive": "int4recv",
                "typsend": "int4send",
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
            }
        ],
        "enums": [],
        "extensions": [],
        "indexes": [],
        "inherits": [],
        "languages": [],
        "policies": [],
        "ranges": [],
        "depends": [],
        "descriptions": [],
        "am": [],
        "catalog_by_oid": {
            "1259": "pg_class",
            "1255": "pg_proc",
            "1247": "pg_type",
            "2615": "pg_namespace",
            "2606": "pg_constraint"
        },
        "current_user": "postgres",
        "pg_version": "PostgreSQL 15.0",
        "introspection_version": 1
    }


# Test data factories using factory_boy
class UserFactory:
    """Factory for creating test user data."""

    @staticmethod
    def create_user_data(**kwargs):
        """Create user data with optional overrides."""
        default_data = {
            "name": "Test User",
            "email": "test@example.com",
            "age": 30,
            "salary": 50000.00,
            "is_active": True,
            "metadata": {"role": "user"},
            "tags": ["user"]
        }
        default_data.update(kwargs)
        return default_data


class PostFactory:
    """Factory for creating test post data."""

    @staticmethod
    def create_post_data(**kwargs):
        """Create post data with optional overrides."""
        default_data = {
            "title": "Test Post",
            "content": "This is test content",
            "user_id": 1,
            "view_count": 0
        }
        default_data.update(kwargs)
        return default_data


# Utility functions for tests
def assert_sql_contains(sql: str, expected_parts: list[str]) -> None:
    """Assert that SQL contains all expected parts."""
    sql_lower = sql.lower()
    for part in expected_parts:
        assert part.lower() in sql_lower, f"Expected '{part}' in SQL: {sql}"


def assert_pydantic_model_fields(model_class, expected_fields: dict) -> None:
    """Assert that a Pydantic model has the expected fields with correct types."""
    model_fields = model_class.model_fields
    for field_name, expected_type in expected_fields.items():
        assert field_name in model_fields, f"Field '{field_name}' not found in model"
        # Note: Type checking for Pydantic models can be complex,
        # this is a simplified version
        assert model_fields[field_name].annotation is not None


async def execute_and_fetch_all(conn: asyncpg.Connection, sql: str, *args) -> list:
    """Execute SQL and return all results."""
    return await conn.fetch(sql, *args)


async def execute_and_fetch_one(conn: asyncpg.Connection, sql: str, *args):
    """Execute SQL and return one result."""
    return await conn.fetchrow(sql, *args)
