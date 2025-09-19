from _ast import Expression

from pydantic import BaseModel

from pghatch.introspection.introspection import make_introspection_query
from pghatch.query.builder import Query
from pghatch.query.builder.builder import select
from pghatch.query.builder.expressions import ColumnExpression, _Parameter
from pghatch.query.builder.expressions import or_, and_
from pghatch.query.builder.functions import json_build_object, json_agg, count
from pghatch.router.resolver.condition_modelsv2 import Condition


def get_condition_operation(query: Query, condition: Condition, table_alias: str = None) -> Expression | None:
    match condition.operator:
        case "=":
            return ColumnExpression(condition.field, table_alias).eq(query.param(condition.value))
        case "<":
            return ColumnExpression(condition.field, table_alias).lt(query.param(condition.value))
        case "<=":
            return ColumnExpression(condition.field, table_alias).le(query.param(condition.value))
        case ">":
            return ColumnExpression(condition.field, table_alias).gt(query.param(condition.value))
        case ">=":
            return ColumnExpression(condition.field, table_alias).ge(query.param(condition.value))
        case "like":
            return ColumnExpression(condition.field, table_alias).like(query.param(condition.value))
        case "ilike":
            return ColumnExpression(condition.field, table_alias).ilike(query.param(condition.value))
        case "in":
            return ColumnExpression(condition.field, table_alias).in_(query.param(condition.value))
        case "not in":
            return ColumnExpression(condition.field, table_alias).nin(query.param(condition.value))
        case "is null":
            return ColumnExpression(condition.field, table_alias).is_null()
        case "is not null":
            return ColumnExpression(condition.field, table_alias).is_not_null()
        case _:
            return None


def add_condition(condition: Condition, query: Query) -> Query:
    if condition.operator == "and" or condition.operator == "or":
        conditions = [sub_condition for sub_condition in condition.conditions if sub_condition is not None]
        if condition.operator == "and":
            query.where(and_(*conditions))
        else:
            query.where(or_(*conditions))
    else:
        operation = get_condition_operation(query, condition)
        if operation is not None:
            query.where(operation)
        else:
            raise ValueError(f"Unsupported operator: {condition.operator}")

    return query


def add_conditions(conditions: list[Condition], query: Query) -> Query:
    for condition in conditions:
        query = add_condition(condition, query)
    return query


def select_table(
        name: str,
        schema: str,
        existing_columns: list[str],
        column_selection: list[str] | None = None,
        include_result_count: bool = False,
        conditions: list[Condition] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        order_by: list[tuple[str, str]] | None = None,
) -> tuple[str, list[str]]:
    table_counter = 0
    table_alias = f"table_{table_counter}"
    q = Query()

    if column_selection is not None:
        if not all(attr in existing_columns for attr in column_selection):
            raise ValueError(
                f"Some attributes {column_selection} do not exist in table {name}."
            )
    else:
        column_selection = existing_columns

    selection = [(attr, ColumnExpression(attr, table_alias)) for attr in column_selection]
    flattened_selection = [el for sublist in selection for el in sublist]

    q = q.select(json_build_object(
        *flattened_selection
    ).as_("result"))

    q = q.from_(name, schema=schema, alias=table_alias)
    if conditions:
        q = add_conditions(conditions=conditions, query=q)
    if limit:
        q = q.limit(limit)
    if offset:
        q = q.offset(offset)
    if order_by:
        for col, direction in order_by:
            if col not in existing_columns:
                raise ValueError(f"Order by column {col} does not exist in table {name}.")
            if direction.lower() not in ("asc", "desc"):
                raise ValueError(f"Order by direction must be 'asc' or 'desc', got '{direction}'.")
            q = q.order_by(ColumnExpression(col, table_alias), direction.upper())


    table_counter += 1
    sub_alias = f"subquery_{table_counter}"
    # Add conditions if needed
    expr = list()
    if include_result_count:
        expr.extend(
            ['total', count(sub_alias)]
        )


    final = select(json_build_object(
        'result', json_agg(
            ColumnExpression("result", sub_alias),
        ),
        *expr
    ).as_("result")).from_subquery(q, sub_alias)

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
                include_result_count=True,
            )

            print(build)


    asyncio.run(main())
