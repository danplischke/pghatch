import asyncio
import logging
from asyncio import Task

import asyncpg
from fastapi import APIRouter, FastAPI
from contextlib import asynccontextmanager

from pghatch.introspection.introspection import make_introspection_query
from pghatch.router.resolver.proc_resolver import ProcResolver
from pghatch.router.resolver.table_resolver import TableViewResolver

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
        logging.warning("Initializing SchemaRouter for schema: %s", schema)
        self.schema = schema
        self.initialized = False
        self.check_connection_interval = 5
        self._pool = None
        self._app: FastAPI | None = None
        self._watcher: Task | None = None

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        self._app = app
        self._pool = await asyncpg.create_pool(dsn="postgres://postgres:postgres@localhost:5432/postgres")
        await self.start()
        self.initialized = True
        yield
        await self._pool.close()

    async def watch_schema(self):
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(WATCH_SQL)
                await conn.add_listener('pghatch_watch', self.restart)
                logging.warning("Watching schema changes...")
                while True:
                    await asyncio.sleep(self.check_connection_interval)
                    await conn.execute("SELECT 1")
        except (Exception,):
            logging.error("Connection lost")
            await self.watch()

    async def watch(self):
        if self._watcher is not None:
            self._watcher.cancel()
        self._watcher = asyncio.create_task(self.watch_schema())

    async def restart(self, a, b, c, d):
        logging.warning("Restarting SchemaRouter for schema: %s", self.schema)
        await self.start()

    async def start(self):
        logging.warning("Starting SchemaRouter for schema: %s", self.schema)

        for route in self.routes:
            for app_route in self._app.routes:
                if route == app_route:
                    self._app.routes.remove(app_route)
            self.routes.remove(route)


        async with self._pool.acquire() as conn:
            introspection = await make_introspection_query(conn)
            for cls in introspection.classes:
                if introspection.get_namespace(
                        cls.relnamespace
                ).nspname == self.schema and cls.relkind in ("r", "v", "m", "f", "p"):
                    TableViewResolver(oid=cls.oid, introspection=introspection).mount(self)

            for proc in introspection.procs:
                if introspection.get_namespace(
                        proc.pronamespace
                ).nspname == self.schema and proc.prokind in ("f", "p"):
                    ProcResolver(oid=proc.oid, introspection=introspection).mount(self)

            await self.watch()
            self._app.include_router(self)
            self._app.openapi_schema = None