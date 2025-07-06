import asyncio
from asyncio import Task

import asyncpg
from fastapi import APIRouter, FastAPI
from contextlib import asynccontextmanager

from pghatch.logging_config import get_logger, log_performance
from pghatch.introspection.introspection import make_introspection_query
from pghatch.router.resolver.proc_resolver import ProcResolver
from pghatch.router.resolver.table_resolver import TableViewResolver

logger = get_logger(__name__)

WATCH_SQL = """
drop schema if exists pghatch_watch cascade;

create schema pghatch_watch;

create function pghatch_watch.notify_watchers_ddl() returns event_trigger as $$
begin
  perform pg_notify(
    'pghatch_watch',
    json_build_object(
      'type',
      'ddl',
      'payload',
      (select json_agg(json_build_object('schema', schema_name, 'command', command_tag)) from pg_event_trigger_ddl_commands() as x)
    )::text
  );
end;
$$ language plpgsql;

create function pghatch_watch.notify_watchers_drop() returns event_trigger as $$
begin
  perform pg_notify(
    'pghatch_watch',
    json_build_object(
      'type',
      'drop',
      'payload',
      (select json_agg(distinct x.schema_name) from pg_event_trigger_dropped_objects() as x)
    )::text
  );
end;
$$ language plpgsql;

create event trigger pghatch_watch_ddl
  on ddl_command_end
  when tag in (
    -- Ref: https://www.postgresql.org/docs/10/static/event-trigger-matrix.html
    'ALTER AGGREGATE',
    'ALTER DOMAIN',
    'ALTER EXTENSION',
    'ALTER FOREIGN TABLE',
    'ALTER FUNCTION',
    'ALTER POLICY',
    'ALTER SCHEMA',
    'ALTER TABLE',
    'ALTER TYPE',
    'ALTER VIEW',
    'COMMENT',
    'CREATE AGGREGATE',
    'CREATE DOMAIN',
    'CREATE EXTENSION',
    'CREATE FOREIGN TABLE',
    'CREATE FUNCTION',
    'CREATE INDEX',
    'CREATE POLICY',
    'CREATE RULE',
    'CREATE SCHEMA',
    'CREATE TABLE',
    'CREATE TABLE AS',
    'CREATE VIEW',
    'DROP AGGREGATE',
    'DROP DOMAIN',
    'DROP EXTENSION',
    'DROP FOREIGN TABLE',
    'DROP FUNCTION',
    'DROP INDEX',
    'DROP OWNED',
    'DROP POLICY',
    'DROP RULE',
    'DROP SCHEMA',
    'DROP TABLE',
    'DROP TYPE',
    'DROP VIEW',
    'GRANT',
    'REVOKE',
    'SELECT INTO'
  )
  execute procedure pghatch_watch.notify_watchers_ddl();

create event trigger pghatch_watch_drop
  on sql_drop
  execute procedure pghatch_watch.notify_watchers_drop();
"""


class SchemaRouter(APIRouter):
    def __init__(self, schema: str, **kwargs):
        super().__init__(**kwargs, lifespan=self.lifespan)
        logger.info("Initializing SchemaRouter for schema: %s", schema)
        self.schema = schema
        self.initialized = False
        self.check_connection_interval = 5
        self._pool = None
        self._app: FastAPI | None = None
        self._watcher: Task | None = None

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        logger.info("Starting SchemaRouter lifespan for schema: %s", self.schema)
        self._app = app

        try:
            logger.debug("Creating database connection pool")
            self._pool = await asyncpg.create_pool(dsn="postgres://postgres:postgres@localhost:5432/postgres")
            logger.info("Database connection pool created successfully")

            await self.start()
            self.initialized = True
            logger.info("SchemaRouter initialization completed for schema: %s", self.schema)

            yield

        finally:
            logger.info("Shutting down SchemaRouter for schema: %s", self.schema)
            if self._pool:
                await self._pool.close()
                logger.debug("Database connection pool closed")

    async def watch_schema(self):
        """Watch for schema changes and maintain database connection."""
        try:
            logger.debug("Setting up schema watcher for schema: %s", self.schema)
            async with self._pool.acquire() as conn:
                logger.debug("Executing watch SQL setup")
                await conn.execute(WATCH_SQL)
                await conn.add_listener('pghatch_watch', self.restart)
                logger.info("Schema change watcher active for schema: %s", self.schema)

                # Keep connection alive with periodic heartbeat
                while True:
                    await asyncio.sleep(self.check_connection_interval)
                    await conn.execute("SELECT 1")
                    logger.debug("Database connection heartbeat successful")

        except Exception as e:
            logger.error("Schema watcher connection lost for schema %s: %s", self.schema, str(e), exc_info=True)
            await self.watch()

    async def watch(self):
        """Start or restart the schema watcher task."""
        logger.debug("Starting schema watcher task")
        if self._watcher is not None:
            logger.debug("Cancelling existing watcher task")
            self._watcher.cancel()
        self._watcher = asyncio.create_task(self.watch_schema())

    async def restart(self, connection, pid, channel, payload):
        """Handle schema change notifications and restart the router."""
        logger.warning("Schema change detected for schema %s, restarting router. Payload: %s", self.schema, payload)
        await self.start()

    @log_performance(logger, "schema introspection and route setup")
    async def start(self):
        """Start the schema router by introspecting the database and setting up routes."""
        logger.info("Starting schema introspection and route setup for schema: %s", self.schema)

        # Clear existing routes
        routes_removed = 0
        for route in list(self.routes):
            for app_route in list(self._app.routes):
                if route == app_route:
                    self._app.routes.remove(app_route)
                    routes_removed += 1
            if route in self.routes:
                self.routes.remove(route)

        if routes_removed > 0:
            logger.debug("Removed %d existing routes", routes_removed)

        # Perform database introspection
        async with self._pool.acquire() as conn:
            logger.debug("Performing database introspection")
            introspection = await make_introspection_query(conn)

            # Set up table/view resolvers
            table_count = 0
            for cls in introspection.classes:
                namespace = introspection.get_namespace(cls.relnamespace)
                if namespace.nspname == self.schema and cls.relkind in ("r", "v", "m", "f", "p"):
                    logger.debug("Creating resolver for %s: %s", cls.relkind, cls.relname)
                    TableViewResolver(oid=cls.oid, introspection=introspection).mount(self)
                    table_count += 1

            logger.info("Created %d table/view resolvers for schema: %s", table_count, self.schema)

            # Set up procedure resolvers
            proc_count = 0
            for proc in introspection.procs:
                namespace = introspection.get_namespace(proc.pronamespace)
                if namespace.nspname == self.schema and proc.prokind in ("f", "p"):
                    logger.debug("Creating resolver for procedure: %s", proc.proname)
                    ProcResolver(oid=proc.oid, introspection=introspection).mount(self)
                    proc_count += 1

            logger.info("Created %d procedure resolvers for schema: %s", proc_count, self.schema)

            # Start schema watcher and include router
            await self.watch()
            self._app.include_router(self)
            self._app.openapi_schema = None

            logger.info("Schema router setup completed for schema: %s (%d tables/views, %d procedures)",
                       self.schema, table_count, proc_count)
