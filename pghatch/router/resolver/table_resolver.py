import asyncio
import json
import typing

from fastapi import APIRouter
from pglast.ast import ResTarget
from pydantic import Field, create_model, BaseModel
from pydantic.alias_generators import to_camel

from pghatch.introspection.introspection import Introspection, make_introspection_query
from pghatch.query.select import select_table
from pghatch.router.resolver.condition_modelsv2 import create_table_view_condition_model
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
        self.type, self.fields, self.return_type, self.condition_type = self._create_return_type(
            introspection
        )
        self.router = None

    def _create_return_type(
            self, introspection: Introspection
    ) -> tuple[str, list[str], type, type[BaseModel] | None]:
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

        table_result_type = create_model(
                to_camel(self.name),
                **field_definitions,
            )

        return (
            typ,
            fields,
            create_model(
                f"{to_camel(self.name)}_result",
                total=(int, None),
                result=typing.List[table_result_type]
            ),
            create_table_view_condition_model(self.oid, introspection)
        )

    def mount(self, router: APIRouter):
        self.router = router

        conditions_type = self.condition_type

        async def _resolve(
                item: conditions_type = None
        ):
            return await self.resolve(item)

        router.add_api_route(
            f"/{self.schema}/{self.name}",
            _resolve,
            methods=["POST"],
            response_model=typing.List[self.return_type],
            summary=f"Get data from {self.schema}.{self.name}",
            description=f"Fetches data from the table or view {self.schema}.{self.name}.",
        )

    async def resolve(self, input_args: BaseModel | None):
        from pglast.stream import RawStream

        stmt, params = select_table(
            name=self.name,
            schema=self.schema,
            existing_columns=self.fields,
            column_selection=input_args.columns if input_args and hasattr(input_args, "columns") else None,
            include_result_count=input_args.total if input_args and hasattr(input_args, "total") else False,
            conditions=input_args.where if input_args and hasattr(input_args, "where") else None,
            limit=input_args.limit if input_args and hasattr(input_args, "limit") else None,
            offset=input_args.offset if input_args and hasattr(input_args, "offset") else None,
            order_by=input_args.order_by if input_args and hasattr(input_args, "order_by") else None,
        )


        sql = RawStream()(stmt)
        async with self.router._pool.acquire() as conn:
            values = await conn.fetch(sql)

        values: asyncpg.Record = next(iter(values))
        result = values.get("result")
        result = json.loads(result)
        return self.return_type(**result)


if __name__ == "__main__":
    import asyncpg

    async def main():
        pool = await asyncpg.create_pool(
            dsn="postgresql://postgres:postgres@localhost/postgres"
        )
        async with pool.acquire() as conn:
            introspection = await make_introspection_query(conn)

        for cls in introspection.classes:
            if introspection.get_namespace(
                    cls.relnamespace
            ).nspname == "public" and cls.relkind in ("r", "v", "m", "f", "p"):
                condition_model = create_table_view_condition_model(cls.oid, introspection)
                print(condition_model.schema_json(indent=4))

    asyncio.run(main())
