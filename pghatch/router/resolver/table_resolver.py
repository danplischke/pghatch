import asyncio
import typing

from fastapi import APIRouter
from pglast.ast import ResTarget
from pglast.enums import LimitOption
from pydantic import Field, create_model, BaseModel
from pydantic.alias_generators import to_camel

from pghatch.introspection.introspection import Introspection
from pghatch.router.resolver.resolver import Resolver


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
        cls = introspection.get_class(oid)
        if cls is None:
            raise ValueError(f"Class with OID {oid} not found in introspection data.")
        self.cls = cls
        self.name = cls.relname
        self.oid = oid
        self.schema = introspection.get_namespace(cls.relnamespace).nspname
        self.type, self.fields, self.return_type = self._create_return_type(
            introspection
        )
        self.router = None

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

            attr_py_type = attr.get_py_type(introspection)
            field_definitions[attr.attname] = (
                attr_py_type,
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
        self.router = router
        router.add_api_route(
            f"/{self.schema}/{self.name}",
            self.resolve,
            methods=["POST"],
            response_model=typing.List[self.return_type],
            summary=f"Get data from {self.schema}.{self.name}",
            description=f"Fetches data from the table or view {self.schema}.{self.name}.",
        )

    async def resolve(self, limit: typing.Union[TableViewLimit, None] = None):
        from pglast.ast import SelectStmt, A_Const, Integer, RangeVar
        from pglast.stream import RawStream
        from pghatch.query_builder import QueryBuilder, col

        builder = QueryBuilder()
        query = builder.select(
            *[col(attr) for attr in self.fields]
        ).from_(table=self.name, schema=self.schema)

        if limit is not None:
            if limit.limit is not None:
                query = query.limit(limit.limit.limit)
            if limit.offset is not None:
                query = query.offset(limit.offset.real)

        sql, params = query.build()

        async with self.router._pool.acquire() as conn:
            values = await conn.fetch(sql, *params)

        return [self.return_type(**dict(row)) for row in values]


if __name__ == "__main__":
    async def task():
        while True:
            await asyncio.sleep(1)
            print("Running...")


    async def main():
        asyncio.create_task(task())

        await asyncio.sleep(20)


    asyncio.run(main())
