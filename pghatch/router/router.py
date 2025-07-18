import asyncio
import logging
from asyncio import Task

import asyncpg
from fastapi import APIRouter, FastAPI
from contextlib import asynccontextmanager

from pghatch.introspection.introspection import make_introspection_query
from pghatch.router.resolver.proc_resolver import ProcResolver
from pghatch.router.resolver.table_resolver import TableViewResolver
from pghatch.router.watch import watch_schema


class SchemaRouter(APIRouter):
    def __init__(self,
                 connection_str: str | None = None,
                 schema: str | None = None,
                 **kwargs):
        super().__init__(**kwargs, lifespan=self.lifespan)

        logging.info(f"Initializing SchemaRouter for schema: {schema}")
        self.connection_str = connection_str
        self.schema = schema
        self.check_connection_interval = 5

        self.initialized = False
        self._pool = None
        self._app: FastAPI | None = None
        self._watcher: Task | None = None

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        self._app = app
        self._pool = await asyncpg.create_pool(dsn=self.connection_str)

        logging.warning(f"Starting SchemaRouter for schema: {self.schema}", )
        await self.start()

        self.initialized = True
        yield
        self._watcher.cancel()
        await asyncio.wait_for(asyncio.create_task(self._pool.close()), timeout=10)

    async def watch_schema(self):
        try:
            await watch_schema(self._pool, self.check_connection_interval)
        except (Exception,):
            logging.error("Connection lost")
            await self.watch()

    async def watch(self):
        if self._watcher is not None:
            self._watcher.cancel()
        self._watcher = asyncio.create_task(self.watch_schema())

    async def restart(self, a, b, c, d):
        logging.info(f"Restarting SchemaRouter for schema: {self.schema}", )
        await self.start()

    async def start(self):
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
