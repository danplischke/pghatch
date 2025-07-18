"""
Examples demonstrating the PostgreSQL Query Builder usage.

This file shows various ways to use the query builder module to construct
complex PostgreSQL queries using the native AST via pglast.
"""

import asyncio
import asyncpg
from pghatch.query_builder import Query, col, func, literal, and_, or_, not_
from pghatch.introspection.introspection import make_introspection_query


async def basic_examples():
    """Basic query building examples."""
    print("=== Basic Query Examples ===\n")

    # Simple SELECT
    qb = Query()
    query = qb.select("id", "name", "email").from_("users")
    sql, params = query.build()
    print(f"Simple SELECT:\n{sql}\n")

    # SELECT with WHERE
    qb = Query()
    query = qb.select("*").from_("users").where(col("active").eq(True))
    sql, params = query.build()
    print(f"SELECT with WHERE:\n{sql}\n")

    # SELECT with multiple conditions
    qb = Query()
    query = (qb.select("*")
             .from_("users")
             .where(col("active").eq(True))
             .where(col("age").gt(18)))
    sql, params = query.build()
    print(f"SELECT with multiple WHERE conditions:\n{sql}\n")


async def join_examples():
    """JOIN query examples."""
    print("=== JOIN Examples ===\n")

    # LEFT JOIN
    qb = Query()
    query = (qb.select("u.name", "p.title", "p.created_at")
             .from_("users", alias="u")
             .left_join("posts", on=col("u.id").eq(col("p.user_id")), alias="p")
             .where(col("u.active").eq(True))
             .order_by("p.created_at", "DESC"))
    sql, params = query.build()
    print(f"LEFT JOIN with ORDER BY:\n{sql}\n")

    # Multiple JOINs
    qb = Query()
    query = (qb.select("u.name", "p.title", "c.content")
             .from_("users", alias="u")
             .inner_join("posts", on=col("u.id").eq(col("p.user_id")), alias="p")
             .left_join("comments", on=col("p.id").eq(col("c.post_id")), alias="c")
             .where(col("u.active").eq(True)))
    sql, params = query.build()
    print(f"Multiple JOINs:\n{sql}\n")


async def aggregate_examples():
    """Aggregate function examples."""
    print("=== Aggregate Function Examples ===\n")

    # GROUP BY with aggregates
    qb = Query()
    query = (qb.select("department", func.count("*").as_("employee_count"), func.avg("salary").as_("avg_salary"))
             .from_("employees")
             .group_by("department")
             .having(func.count("*").gt(5))
             .order_by("avg_salary", "DESC"))
    sql, params = query.build()
    print(f"GROUP BY with aggregates:\n{sql}\n")

    # Complex aggregation with JOINs
    qb = Query()
    query = (qb.select(
                "u.name",
                func.count("o.id").as_("order_count"),
                func.sum("o.total").as_("total_spent"),
                func.avg("o.total").as_("avg_order_value")
             )
             .from_("users", alias="u")
             .left_join("orders", on=col("u.id").eq(col("o.user_id")), alias="o")
             .where(col("u.active").eq(True))
             .group_by("u.id", "u.name")
             .having(func.count("o.id").gt(0))
             .order_by("total_spent", "DESC")
             .limit(10))
    sql, params = query.build()
    print(f"Complex aggregation with JOINs:\n{sql}\n")


async def function_examples():
    """PostgreSQL function examples."""
    print("=== PostgreSQL Function Examples ===\n")

    # String functions
    qb = Query()
    query = (qb.select(
                "id",
                func.upper("name").as_("upper_name"),
                func.concat("first_name", " ", "last_name").as_("full_name"),
                func.substring("email", 1, func.length("email")).as_("email_copy")
             )
             .from_("users")
             .where(func.lower("name").like("%john%")))
    sql, params = query.build()
    print(f"String functions:\n{sql}\n")

    # Date functions
    qb = Query()
    query = (qb.select(
                "id",
                "created_at",
                func.date_trunc("month", "created_at").as_("month"),
                func.extract("year", "created_at").as_("year"),
                func.age("created_at").as_("age_since_creation")
             )
             .from_("posts")
             .where(func.date_trunc("year", "created_at").eq("2023-01-01")))
    sql, params = query.build()
    print(f"Date functions:\n{sql}\n")

    # JSON functions
    qb = Query()
    query = (qb.select(
                "id",
                func.json_extract_path_text("metadata", "title").as_("meta_title"),
                func.jsonb_array_length("tags").as_("tag_count")
             )
             .from_("articles")
             .where(func.jsonb_extract_path_text("metadata", "status").eq("published")))
    sql, params = query.build()
    print(f"JSON functions:\n{sql}\n")


async def complex_expression_examples():
    """Complex expression examples."""
    print("=== Complex Expression Examples ===\n")

    # Complex WHERE with AND/OR
    qb = Query()
    condition = and_(
        col("active").eq(True),
        or_(
            col("age").gt(18),
            and_(
                col("verified").eq(True),
                col("parent_consent").eq(True)
            )
        ),
        not_(col("banned").eq(True))
    )
    query = qb.select("*").from_("users").where(condition)
    sql, params = query.build()
    print(f"Complex WHERE with AND/OR/NOT:\n{sql}\n")

    # CASE expression
    qb = Query()
    case_expr = (func.case()
                .when(col("age").lt(18), "Minor")
                .when(col("age").lt(65), "Adult")
                .else_("Senior")
                .end())
    query = (qb.select("name", "age", case_expr.as_("age_group"))
             .from_("users")
             .order_by("age"))
    sql, params = query.build()
    print(f"CASE expression:\n{sql}\n")

    # IN and NOT IN
    qb = Query()
    query = (qb.select("*")
             .from_("products")
             .where(
                 and_(
                     col("category_id").in_([1, 2, 3, 4]),
                     col("status").ne("discontinued"),
                     col("price").gt(0)
                 )
             ))
    sql, params = query.build()
    print(f"IN clause with multiple conditions:\n{sql}\n")


async def window_function_examples():
    """Window function examples."""
    print("=== Window Function Examples ===\n")

    # ROW_NUMBER and RANK
    qb = Query()
    query = (qb.select(
                "name",
                "salary",
                "department",
                func.row_number().as_("row_num"),
                func.rank().as_("salary_rank"),
                func.dense_rank().as_("dense_rank")
             )
             .from_("employees")
             .order_by("department", "salary"))
    sql, params = query.build()
    print(f"Window functions (ROW_NUMBER, RANK):\n{sql}\n")

    # LAG and LEAD
    qb = Query()
    query = (qb.select(
                "date",
                "sales",
                func.lag("sales", 1).as_("prev_sales"),
                func.lead("sales", 1).as_("next_sales")
             )
             .from_("daily_sales")
             .order_by("date"))
    sql, params = query.build()
    print(f"LAG and LEAD functions:\n{sql}\n")


async def array_examples():
    """Array function examples."""
    print("=== Array Function Examples ===\n")

    # Array functions
    qb = Query()
    query = (qb.select(
                "id",
                "tags",
                func.array_length("tags", 1).as_("tag_count"),
                func.array_append("tags", "new_tag").as_("tags_with_new")
             )
             .from_("posts")
             .where(func.array_length("tags", 1).gt(0)))
    sql, params = query.build()
    print(f"Array functions:\n{sql}\n")

    # UNNEST
    qb = Query()
    query = (qb.select("id", func.unnest("tags").as_("tag"))
             .from_("posts")
             .where(col("published").eq(True)))
    sql, params = query.build()
    print(f"UNNEST function:\n{sql}\n")


async def pagination_examples():
    """Pagination examples."""
    print("=== Pagination Examples ===\n")

    # Basic pagination
    qb = Query()
    query = (qb.select("*")
             .from_("users")
             .where(col("active").eq(True))
             .order_by("created_at", "DESC")
             .limit(20)
             .offset(40))  # Page 3 (0-indexed)
    sql, params = query.build()
    print(f"Basic pagination (page 3, 20 per page):\n{sql}\n")

    # Cursor-based pagination using window functions
    qb = Query()
    query = (qb.select(
                "*",
                func.row_number().as_("row_num")
             )
             .from_("posts")
             .where(col("published").eq(True))
             .order_by("created_at", "DESC"))
    sql, params = query.build()
    print(f"Cursor-based pagination setup:\n{sql}\n")


async def conditional_examples():
    """Conditional function examples."""
    print("=== Conditional Function Examples ===\n")

    # COALESCE
    qb = Query()
    query = (qb.select(
                "id",
                func.coalesce("nickname", "first_name", "username").as_("display_name"),
                func.nullif("bio", "").as_("bio_or_null")
             )
             .from_("users"))
    sql, params = query.build()
    print(f"COALESCE and NULLIF:\n{sql}\n")

    # GREATEST and LEAST
    qb = Query()
    query = (qb.select(
                "id",
                func.greatest("score1", "score2", "score3").as_("best_score"),
                func.least("price1", "price2", "price3").as_("lowest_price")
             )
             .from_("comparisons"))
    sql, params = query.build()
    print(f"GREATEST and LEAST:\n{sql}\n")


async def execute_with_pool_example():
    """Example of executing queries with a connection pool."""
    print("=== Query Execution Example ===\n")

    # This would be used in a real application
    # pool = await asyncpg.create_pool("postgresql://user:pass@localhost/db")

    # Mock execution for demonstration
    qb = Query()
    query = (qb.select("id", "name", "email")
             .from_("users")
             .where(col("active").eq(True))
             .limit(10))

    sql, params = query.build()
    print(f"Query to execute:\n{sql}")
    print(f"Parameters: {params}\n")

    # In a real application:
    # result = await query.execute(pool)
    # users = result.to_dicts()
    # first_user = result.first()

    print("# Example usage in application:")
    print("result = await query.execute(pool)")
    print("users = result.to_dicts()")
    print("first_user = result.first()")
    print("user_models = result.to_models()  # if model_class provided\n")


async def introspection_integration_example():
    """Example of using query builder with introspection."""
    print("=== Introspection Integration Example ===\n")

    # This would be used with real introspection data
    print("# Example with introspection:")
    print("introspection = await make_introspection_query(connection)")
    print("qb = QueryBuilder(introspection=introspection)")
    print("")
    print("# Access user-defined functions:")
    print("user_functions = qb.functions.list_user_functions('public')")
    print("print(f'Available functions: {user_functions}')")
    print("")
    print("# Use a user-defined function:")
    print("query = qb.select('*').from_('users').where(")
    print("    qb.functions.get_user_function('is_premium', 'public')('user_id')")
    print(")")


async def main():
    """Run all examples."""
    print("PostgreSQL Query Builder Examples")
    print("=" * 50)
    print()

    await basic_examples()
    await join_examples()
    await aggregate_examples()
    await function_examples()
    await complex_expression_examples()
    await window_function_examples()
    await array_examples()
    await pagination_examples()
    await conditional_examples()
    await execute_with_pool_example()
    await introspection_integration_example()

    print("=" * 50)
    print("All examples completed!")


if __name__ == "__main__":
    asyncio.run(main())
