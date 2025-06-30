import asyncio

from fastapi import APIRouter
from psycopg import AsyncConnection
from psycopg._adapters_map import AdaptersMap
from psycopg.pq import Format
from psycopg.types import TypeInfo
from psycopg.types.string import
from psycopg import adapters

from pgrestcue.introspection.introspection import make_introspection_query
from sqlmodel import SQLModel
from pydantic import create_model


class SchemaRouter(APIRouter):

    def __init__(self, schema: str, **kwargs):
        super().__init__(**kwargs)
        self.schema = schema

    async def start(self):
        introspection = make_introspection_query() # TODO: make async
        async with await AsyncConnection.connect("dbname=postgres user=postgres password=postgres host=localhost port=5432") as aconn:
            for cls in introspection.classes:
                field_definitions = {}
                for field in cls.get_attributes(introspection):
                    t = field.get_type(introspection)

                    t = await TypeInfo.fetch(aconn, t.typname)
                    l = AdaptersMap(template=adapters)
                    loader = l.get_loader(t.oid, Format.TEXT)
                    print(loader.)

        #     # Create a Pydantic model for the class
        #
        # Hero = create_model(
        #     "Hero",
        #     __base__=SQLModel,
        #     __cls_kwargs__={"table": True},
        #     **field_definitions,
        # )
        #


if __name__ == '__main__':
    from fastapi import FastAPI

    app = FastAPI()
    router = SchemaRouter(schema="public")
    print(router.on_startup)
    asyncio.run(router.start())
