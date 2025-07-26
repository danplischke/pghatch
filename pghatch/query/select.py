from pydantic import BaseModel
from sqlalchemy import Column

from pghatch.introspection.introspection import make_introspection_query
from pghatch.query.builder import Query
from pghatch.query.builder.builder import select
from pghatch.query.builder.expressions import ResTargetExpression, ColumnExpression, FunctionExpression, Parameter
from pghatch.query.builder.functions import json_build_object, json_agg, count
from pghatch.query.builder.expressions import or_, and_,


def get_condition_operation(condition: BaseModel, table_alias: str = None) -> ColumnExpression | None:
    if condition.operator == '=':
        return ColumnExpression(condition.field, table_alias).eq(Parameter(condition.value))


def add_condition(condition: BaseModel, query: Query) -> Query:
    if condition.operator == "and" or condition.operator == "or":
        conditions = [sub_condition for sub_condition in condition.conditions if sub_condition is not None]
        if condition.operator == "and":
            query.where(and_(*conditions))
        else:
            query.where(or_(*conditions))

    else:
        query.where(get_condition_operation(condition))

    return query


def select_table(
        oid: str,
        introspection,
        attr_selection: list[str] | None = None,
        include_result_count: bool = False,
        condition: BaseModel | None = None,
) -> tuple[str, list[str], type, type[BaseModel] | None]:
    table_counter = 0
    table = introspection.get_class(oid)
    table_alias = f"table_{table_counter}"
    q = Query()

    existing_columns = introspection.get_attributes(oid)
    existing_columns = [col.attname for col in existing_columns]

    if attr_selection is not None:
        if not all(attr in existing_columns for attr in attr_selection):
            raise ValueError(
                f"Some attributes {attr_selection} do not exist in table with OID {oid}."
            )
    else:
        attr_selection = existing_columns

    selection = [(attr, ColumnExpression(attr, table_alias)) for attr in attr_selection]
    flattened_selection = [el for sublist in selection for el in sublist]

    q = q.select(json_build_object(
        *flattened_selection
    ))

    q = q.from_(table.relname, schema=table.get_namespace(introspection).nspname, alias=table_alias)

    # Add conditions if needed
    expr = list()
    if include_result_count:
        expr.extend(
            ['total', count(table_alias)]
        )

    table_counter += 1
    sub_alias = f"subquery_{table_counter}"
    final = select(json_build_object(
        'result', json_agg(
            ColumnExpression(sub_alias),
        ),
        *expr
    )).from_subquery(q, sub_alias)

    if condition is not None:
        if condition.operator == '=':

    return final.build()


if __name__ == '__main__':
    import asyncpg
    import asyncio

    from pglast import parse_sql

    parsed = parse_sql("""
                       SELECT jsonb_build_object('result', jsonb_agg(result)) AS json_array, COUNT(result) as count
                       FROM (SELECT jsonb_build_object('name', tt.name, 'syns2', tt.syns2) AS result
                           FROM test_table tt
                           LIMIT 10001) subquery;
                       """)


    async def main():
        pool = await asyncpg.create_pool(
            dsn="postgresql://postgres:postgres@localhost/postgres"
        )
        async with pool.acquire() as conn:
            introspection = await make_introspection_query(conn)
            build = select_table(
                "16388",
                introspection,
                attr_selection=['name', 'syns2'],
            )

            print(build)


    asyncio.run(main())
