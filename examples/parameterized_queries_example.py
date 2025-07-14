"""
Example demonstrating safe parameterized queries to prevent SQL injection.

This example shows how to use the query builder's parameter system to safely
bind user input and prevent SQL injection attacks.
"""

import asyncio
import asyncpg
from pghatch.query_builder import Query, col, func, param, and_, or_


async def main():
    """Demonstrate parameterized queries for SQL injection prevention."""

    # Example user inputs (potentially dangerous)
    user_name = "John'; DROP TABLE users; --"  # SQL injection attempt
    user_age = 25
    user_email = "john@example.com"
    search_pattern = "%admin%"

    print("=== Safe Parameterized Queries Example ===\n")

    # 1. Basic parameterized WHERE clause
    print("1. Basic parameterized query:")
    qb1 = Query()
    qb1.select("id", "name", "email").from_("users").where(
        col("name").eq(param(user_name))  # Safe: user_name will be bound as parameter
    )
    sql1, params1 = qb1.build()
    print(f"SQL: {sql1}")
    print(f"Parameters: {params1}")
    print()

    # 2. Multiple parameters
    print("2. Multiple parameters:")
    qb2 = Query()
    qb2.select("*").from_("users").where(
        and_(
            col("name").eq(param(user_name)),
            col("age").gt(param(user_age)),
            col("email").like(param(search_pattern))
        )
    )
    sql2, params2 = qb2.build()
    print(f"SQL: {sql2}")
    print(f"Parameters: {params2}")
    print()

    # 3. Parameters in IN clause
    print("3. Parameters in IN clause:")
    user_ids = [1, 2, 3, 4, 5]
    qb3 = Query()
    qb3.select("name", "email").from_("users").where(
        col("id").in_([param(uid) for uid in user_ids])  # Each ID is parameterized
    )
    sql3, params3 = qb3.build()
    print(f"SQL: {sql3}")
    print(f"Parameters: {params3}")
    print()

    # 4. Parameters in function calls
    print("4. Parameters in function calls:")
    qb4 = Query()
    qb4.select(
        "name",
        func.concat("Hello, ", param(user_name)).as_("greeting")
    ).from_("users").where(
        func.length("name").gt(param(5))
    )
    sql4, params4 = qb4.build()
    print(f"SQL: {sql4}")
    print(f"Parameters: {params4}")
    print()

    # 5. Complex query with mixed parameters and literals
    print("5. Complex query with mixed parameters:")
    qb5 = Query()
    qb5.select(
        "u.name",
        "u.email",
        func.count("o.id").as_("order_count")
    ).from_("users", alias="u").left_join(
        "orders",
        on=col("u.id").eq(col("o.user_id")),
        alias="o"
    ).where(
        and_(
            col("u.active").eq(param(True)),  # Boolean parameter
            col("u.created_at").gt(param("2023-01-01")),  # Date parameter
            or_(
                col("u.name").ilike(param(f"%{user_name}%")),
                col("u.email").eq(param(user_email))
            )
        )
    ).group_by("u.id", "u.name", "u.email").having(
        func.count("o.id").gt(param(0))
    ).order_by("order_count", "DESC").limit(param(10))

    sql5, params5 = qb5.build()
    print(f"SQL: {sql5}")
    print(f"Parameters: {params5}")
    print()

    # 6. CASE expression with parameters
    print("6. CASE expression with parameters:")
    min_age = 18
    max_age = 65
    qb6 = Query()
    case_expr = (func.case()
                .when(col("age").lt(param(min_age)), param("Minor"))
                .when(col("age").lt(param(max_age)), param("Adult"))
                .else_(param("Senior"))
                .end())

    qb6.select("name", case_expr.as_("age_group")).from_("users")
    sql6, params6 = qb6.build()
    print(f"SQL: {sql6}")
    print(f"Parameters: {params6}")
    print()

    print("=== Security Benefits ===")
    print("✅ All user inputs are safely parameterized")
    print("✅ SQL injection attempts are neutralized")
    print("✅ Parameters are bound server-side by PostgreSQL")
    print("✅ No string concatenation or formatting in SQL")
    print("✅ Type safety and automatic escaping")
    print()

    print("=== Usage with AsyncPG ===")
    print("# The parameters are automatically passed to asyncpg:")
    print("# rows = await connection.fetch(sql, *parameters)")
    print("# result = await qb.execute(pool)  # Handles parameters automatically")


def demonstrate_unsafe_vs_safe():
    """Show the difference between unsafe and safe approaches."""

    user_input = "admin'; DROP TABLE users; --"

    print("=== UNSAFE vs SAFE Comparison ===\n")

    # UNSAFE: String formatting (DON'T DO THIS!)
    print("❌ UNSAFE (vulnerable to SQL injection):")
    unsafe_sql = f"SELECT * FROM users WHERE name = '{user_input}'"
    print(f"SQL: {unsafe_sql}")
    print("^ This would execute the DROP TABLE command!")
    print()

    # SAFE: Parameterized query
    print("✅ SAFE (using parameters):")
    qb = Query()
    qb.select("*").from_("users").where(col("name").eq(param(user_input)))
    safe_sql, params = qb.build()
    print(f"SQL: {safe_sql}")
    print(f"Parameters: {params}")
    print("^ The malicious input is safely bound as a parameter value")
    print()


async def real_world_example():
    """Real-world example with actual database operations."""

    print("=== Real-World Usage Example ===\n")

    # This would be your actual database connection
    # pool = await asyncpg.create_pool("postgresql://user:pass@localhost/db")

    # Example: User search with filters
    def build_user_search_query(
        name_filter: str = None,
        min_age: int = None,
        max_age: int = None,
        email_domain: str = None,
        is_active: bool = None,
        limit: int = 50
    ):
        """Build a dynamic user search query with safe parameters."""

        qb = Query()
        qb.select(
            "id",
            "name",
            "email",
            "age",
            "created_at",
            "last_login"
        ).from_("users")

        # Build WHERE conditions dynamically
        conditions = []

        if name_filter:
            conditions.append(col("name").ilike(param(f"%{name_filter}%")))

        if min_age is not None:
            conditions.append(col("age").ge(param(min_age)))

        if max_age is not None:
            conditions.append(col("age").le(param(max_age)))

        if email_domain:
            conditions.append(col("email").like(param(f"%@{email_domain}")))

        if is_active is not None:
            conditions.append(col("active").eq(param(is_active)))

        # Apply conditions if any
        if conditions:
            if len(conditions) == 1:
                qb.where(conditions[0])
            else:
                from pghatch.query_builder import and_
                qb.where(and_(*conditions))

        qb.order_by("created_at", "DESC").limit(param(limit))

        return qb

    # Example usage
    search_qb = build_user_search_query(
        name_filter="john",
        min_age=18,
        email_domain="example.com",
        is_active=True,
        limit=25
    )

    sql, params = search_qb.build()
    print("Dynamic search query:")
    print(f"SQL: {sql}")
    print(f"Parameters: {params}")
    print()

    # How you'd execute it:
    print("Execution:")
    print("# result = await search_qb.execute(pool)")
    print("# users = result.rows")
    print("# for user in users:")
    print("#     print(f\"User: {user['name']} <{user['email']}>\")")


if __name__ == "__main__":
    asyncio.run(main())
    print("\n" + "="*60 + "\n")
    demonstrate_unsafe_vs_safe()
    print("\n" + "="*60 + "\n")
    asyncio.run(real_world_example())
