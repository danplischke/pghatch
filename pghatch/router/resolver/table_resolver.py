import asyncio
import typing

from fastapi import APIRouter
from pglast.ast import ResTarget
from pglast.enums import LimitOption
from pydantic import Field, create_model, BaseModel
from pydantic.alias_generators import to_camel

from pghatch.logging_config import get_logger, log_performance
from pghatch.introspection.introspection import Introspection
from pghatch.router.resolver.resolver import Resolver

logger = get_logger(__name__)


class TableViewLimit(BaseModel):
    """
    Model for table/view limit parameters.
    """

    limit: int | None = Field(
        default=None, examples=[None], description="Maximum number of rows to return."
    )
    offset: int | None = Field(
        default=None,
        examples=[None],
        description="Number of rows to skip before starting to return rows.",
    )


class TableViewResolver(Resolver):
    def __init__(self, oid: str, introspection: Introspection):
        logger.debug("Initializing TableViewResolver for OID: %s", oid)

        cls = introspection.get_class(oid)
        if cls is None:
            logger.error("Class with OID %s not found in introspection data", oid)
            raise ValueError(f"Class with OID {oid} not found in introspection data.")

        self.cls = cls
        self.name = cls.relname
        self.oid = oid
        self.schema = introspection.get_namespace(cls.relnamespace).nspname

        logger.debug("Creating resolver for %s.%s (type: %s)", self.schema, self.name, cls.relkind)

        self.type, self.fields, self.return_type = self._create_return_type(
            introspection
        )

        logger.info("TableViewResolver created for %s.%s with %d fields",
                   self.schema, self.name, len(self.fields))

    def _create_return_type(
        self, introspection: Introspection
    ) -> tuple[str, list[str], type]:
        field_definitions = {}
        fields = list()
        for attr in introspection.get_attributes(self.oid):  # order by attnum
            if attr.attisdropped:
                continue

            typ = attr.get_type(introspection)
            fields.append(attr.attname)

            field_definitions[attr.attname] = (
                attr.get_py_type(introspection),
                Field(introspection.get_description(introspection.PG_CLASS, typ.oid)),
            )
        typ = self.cls.relkind
        return (
            typ,
            fields,
            create_model(
                to_camel(self.name),
                **field_definitions,
            ),
        )

    def mount(self, router: APIRouter):
        endpoint_path = f"/{self.schema}/{self.name}"
        logger.debug("Mounting table/view resolver at endpoint: %s", endpoint_path)

        router.add_api_route(
            endpoint_path,
            self.resolve,
            methods=["POST"],
            response_model=typing.List[self.return_type],
            summary=f"Get data from {self.schema}.{self.name}",
            description=f"Fetches data from the table or view {self.schema}.{self.name}.",
        )

        logger.info("Mounted endpoint %s for %s.%s", endpoint_path, self.schema, self.name)

    @log_performance(logger, f"table/view query")
    async def resolve(self, limit: typing.Union[TableViewLimit, None] = None):
        from pglast.ast import SelectStmt, A_Const, Integer, RangeVar
        from pglast.stream import RawStream
        import asyncpg

        logger.debug("Resolving query for %s.%s", self.schema, self.name)

        # Log query parameters
        if limit:
            logger.debug("Query parameters - limit: %s, offset: %s", limit.limit, limit.offset)
        else:
            logger.debug("Query parameters - no limit specified")

        select_stmt = SelectStmt(
            targetList=[ResTarget(name=attr) for attr in self.fields],
            fromClause=[
                RangeVar(
                    relname=self.name,
                    schemaname=self.schema,
                    inh=True,
                    relpersistence=self.type,
                )
            ],
            limitCount=A_Const(val=Integer(ival=limit.limit))
            if limit is not None and limit.limit is not None
            else None,
            limitOffset=A_Const(val=Integer(ival=limit.offset)) # TODO: change to server-side binding / remote cursor pagination
            if limit is not None and limit.offset is not None
            else None,
            limitOption=LimitOption.LIMIT_OPTION_COUNT
            if limit is not None and limit.limit is not None
            else None,
        )

        sql = RawStream()(select_stmt)
        logger.debug("Generated SQL: %s", sql)

        try:
            logger.debug("Connecting to database")
            conn = await asyncpg.connect(
                user="postgres", password="postgres", database="postgres", host="127.0.0.1"
            )

            logger.debug("Executing query")
            values = await conn.fetch(sql)

            logger.info("Query executed successfully for %s.%s, returned %d rows",
                       self.schema, self.name, len(values))

            await conn.close()
            logger.debug("Database connection closed")

            return [self.return_type(**dict(row)) for row in values]

        except Exception as e:
            logger.error("Query execution failed for %s.%s: %s",
                        self.schema, self.name, str(e), exc_info=True)
            raise


if __name__ == "__main__":
    async def task():
        while True:
            await asyncio.sleep(1)
            print("Running...")

    async def main():
        asyncio.create_task(task())

        await asyncio.sleep(20)


    asyncio.run(main())
