import asyncio
import logging

import asyncpg

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


async def watch_schema(pool: asyncpg.Pool | None = None, check_connection_interval: int = 5):
    async with pool.acquire() as conn:
        await conn.execute(WATCH_SQL)
        await conn.add_listener('pghatch_watch', self.restart)
        logging.warning("Watching schema changes...")
        while True:
            await asyncio.sleep(check_connection_interval)
            await conn.execute("SELECT 1")