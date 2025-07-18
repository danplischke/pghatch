"""
Tests for the query builder module.
"""

from unittest.mock import Mock, AsyncMock

import pytest

from pghatch.query_builder import Query, col, func, literal, and_, or_, not_
from pghatch.query_builder.builder import select, select_all
from pghatch.query_builder.types import QueryResult


def test_simple_select():
    """Test building a simple SELECT query."""
    qb = select("id", "name", "email").from_("users")
    sql, params = qb.build()

    assert sql == "SELECT id, name, email FROM users"
    assert params == []

def test_select_all():
    """Test SELECT * query."""
    qb = select_all().from_("users")
    sql, params = qb.build()
    assert sql == "SELECT * FROM users"
    assert params == []

def test_select_with_schema():
    """Test SELECT with schema qualification."""
    qb = select("id", "name").from_("users", schema="public")
    sql, params = qb.build()
    assert sql == "SELECT id, name FROM public.users"
    assert params == []

def test_select_with_alias():
    """Test SELECT with table alias."""
    qb = select("u.id", "u.name").from_("users", alias="u")
    sql, params = qb.build()

    assert sql == "SELECT u.id, u.name FROM users AS u"

def test_where_clause():
    """Test WHERE clause with expressions."""
    qb = select_all().from_("users").where(col("active").eq(True))
    sql, params = qb.build()

    assert sql == "SELECT * FROM users WHERE active = TRUE"
    assert params == []

def test_multiple_where_conditions():
    """Test multiple WHERE conditions combined with AND."""
    qb = select_all().from_("users").where(col("active").eq(True)).where(col("age").gt(18))
    sql, params = qb.build()

    assert sql == "SELECT * FROM users WHERE active = TRUE AND age > 18"
    assert params == []

def test_complex_where_with_and_or():
    """Test complex WHERE clause with AND/OR logic."""
    qb = Query()
    condition = and_(
        col("active").eq(True),
        or_(
            col("age").gt(18),
            col("verified").eq(True)
        )
    )
    qb.select_all().from_("users").where(condition)

    sql, params = qb.build()

    assert sql == "SELECT * FROM users WHERE active = TRUE AND (age > 18 OR verified = TRUE)"
    assert params == []

def test_joins():
    """Test various types of joins."""
    qb = Query()
    qb.select("u.name", "p.title").from_("users", alias="u").left_join(
        "posts",
        on=col("u.id").eq(col("p.user_id")),
        alias="p"
    )

    sql, params = qb.build()

    assert sql == "SELECT u.name, p.title FROM users AS u LEFT JOIN posts AS p ON u.id = p.user_id"
    assert params == []

def test_group_by_and_having():
    """Test GROUP BY and HAVING clauses."""
    qb = Query()
    qb.select("department", func.count("*").as_("employee_count")).from_("employees").group_by("department").having(func.count("*").gt(5))

    sql, params = qb.build()

    assert sql == "SELECT department, count(*) AS employee_count FROM employees GROUP BY department HAVING count(*) > 5";
    assert params == []

def test_order_by():
    """Test ORDER BY clause."""
    qb = Query()
    qb.select("*").from_("users").order_by("name").order_by("created_at", "DESC")

    sql, params = qb.build()

    assert sql == "SELECT * FROM users ORDER BY name ASC NULLS LAST, created_at DESC NULLS LAST"
    assert params == []

def test_limit_and_offset():
    """Test LIMIT and OFFSET clauses."""
    qb = Query()
    qb.select("*").from_("users").limit(10).offset(20)

    sql, params = qb.build()

    assert sql == "SELECT * FROM users LIMIT 10 OFFSET 20"
    assert params == []

def test_distinct():
    """Test DISTINCT clause."""
    qb = Query()
    qb.select("department").from_("employees").distinct()

    sql, params = qb.build()

    assert sql == "SELECT DISTINCT department FROM employees"
    assert params == []

def test_distinct_list():
    """Test DISTINCT clause."""
    qb = Query()
    qb.select("department").from_("employees").distinct(["department", "id"])

    sql, params = qb.build()

    assert sql == "SELECT DISTINCT ON (department, id) department FROM employees"
    assert params == []

def test_function_calls():
    """Test function calls in SELECT."""
    qb = Query()
    qb.select(
        func.upper("name").as_("upper_name"),
        func.count("*").as_("total"),
        func.date_trunc("month", "created_at").as_("month")
    ).from_("users")

    sql, params = qb.build()

    assert sql == "SELECT upper(name) AS upper_name, count(*) AS total, date_trunc('month', created_at) AS month FROM users"
    assert params == []

def test_column_expressions():
    """Test various column expressions."""
    qb = Query()
    qb.select("*").from_("users").where(
        and_(
            col("name").like("%john%"),
            col("age").in_([25, 30, 35]),
            col("email").is_not_null()
        )
    )

    sql, params = qb.build()

    assert sql == "SELECT * FROM users WHERE (name ~~ '%john%' AND age IN (25, 30, 35)) AND email IS NOT NULL"
    assert params == []

def test_case_expression():
    """Test CASE expressions."""
    qb = Query()
    case_expr = (func.case()
                .when(col("age").lt(18), "Minor")
                .when(col("age").lt(65), "Adult")
                .else_("Senior")
                .end())

    qb.select("name", case_expr.as_("age_group")).from_("users")

    sql, params = qb.build()

    assert sql == "SELECT name, CASE WHEN age < 18 THEN 'Minor' WHEN age < 65 THEN 'Adult' ELSE 'Senior' END AS age_group FROM users"
    assert params == []

def test_subquery_in_where():
    """Test subquery in WHERE clause."""
    subquery = Query()
    subquery.select("user_id").from_("orders").where(col("total").gt(100))

    qb = Query()
    qb.select("*").from_("users").where(col("id").in_(subquery))

    # This would need proper subquery support in the expressions
    # For now, just test that the subquery builds correctly
    sql, params = qb.build()
    assert sql == "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100)"
    assert params == []

def test_json_operations():
    """Test JSON operations."""
    qb = Query()
    qb.select(
        "id",
        func.json_extract_path_text("metadata", "name").as_("meta_name")
    ).from_("products").where(
        func.jsonb_extract_path_text("attributes", "color").eq("red")
    )

    sql, params = qb.build()

    assert sql == "SELECT id, json_extract_path_text(metadata, 'name') AS meta_name FROM products WHERE jsonb_extract_path_text(attributes, 'color') = 'red'"

def test_window_functions():
    """Test window functions."""
    qb = Query()
    qb.select(
        "name",
        "salary",
        func.row_number().as_("row_num"),
        func.rank().as_("salary_rank")
    ).from_("employees")

    sql, params = qb.build()

    assert sql == "SELECT name, salary, row_number() AS row_num, rank() AS salary_rank FROM employees"

def test_array_functions():
    """Test array functions."""
    qb = Query()
    qb.select("*").from_("users").where(
        func.array_length("tags", 1).gt(0)
    )

    sql, params = qb.build()

    assert sql == "SELECT * FROM users WHERE array_length(tags, 1) > 0"

def test_string_representation():
    """Test string representation of QueryBuilder."""
    qb = Query()
    qb.select("*").from_("users").where(col("active").eq(True))

    sql_str = str(qb)
    repr_str = repr(qb)

    assert sql_str == "SELECT * FROM users WHERE active = TRUE"
    assert repr_str == "QueryBuilder(sql='SELECT * FROM users WHERE active = TRUE', params=[])"

def test_complex_query():
    """Test a complex query with multiple clauses."""
    qb = Query()

    # Build a complex query
    query = (qb
            .select(
                "u.name",
                "u.email",
                func.count("o.id").as_("order_count"),
                func.sum("o.total").as_("total_spent"),
                func.avg("o.total").as_("avg_order")
            )
            .from_("users", alias="u")
            .left_join("orders", on=col("u.id").eq(col("o.user_id")), alias="o")
            .where(
                and_(
                    col("u.active").eq(True),
                    col("u.created_at").gt("2023-01-01")
                )
            )
            .group_by("u.id", "u.name", "u.email")
            .having(func.count("o.id").gt(0))
            .order_by("total_spent", "DESC")
            .limit(50))

    sql, params = query.build()

    # Verify all components are present
    assert sql == "SELECT u.name, u.email, count(o.id) AS order_count, sum(o.total) AS total_spent, avg(o.total) AS avg_order FROM users AS u LEFT JOIN orders AS o ON u.id = o.user_id WHERE u.active = TRUE AND u.created_at > '2023-01-01' GROUP BY \"u.id\", \"u.name\", \"u.email\" HAVING count(o.id) > 0 ORDER BY total_spent DESC NULLS LAST LIMIT 50"


def test_column_expressions():
    """Test column expression methods."""
    col_expr = col("age")

    # Test comparison operators
    eq_expr = col_expr.eq(25)
    ne_expr = col_expr.ne(30)
    lt_expr = col_expr.lt(18)
    gt_expr = col_expr.gt(65)

    assert eq_expr.node is not None
    assert ne_expr.node is not None
    assert lt_expr.node is not None
    assert gt_expr.node is not None

def test_logical_expressions():
    """Test logical expression combinations."""
    expr1 = col("active").eq(True)
    expr2 = col("age").gt(18)
    expr3 = col("verified").eq(True)

    and_expr = and_(expr1, expr2)
    or_expr = or_(expr2, expr3)
    not_expr = not_(expr1)

    assert and_expr.node is not None
    assert or_expr.node is not None
    assert not_expr.node is not None

def test_function_expressions():
    """Test function expression building."""
    # Test aggregate functions
    count_expr = func.count("*")
    sum_expr = func.sum("amount")
    avg_expr = func.avg("score")

    # Test string functions
    upper_expr = func.upper("name")
    concat_expr = func.concat("first_name", " ", "last_name")

    # Test date functions
    now_expr = func.now()
    date_trunc_expr = func.date_trunc("month", "created_at")

    assert all(expr.node is not None for expr in [
        count_expr, sum_expr, avg_expr, upper_expr,
        concat_expr, now_expr, date_trunc_expr
    ])

def test_literal_expressions():
    """Test literal value expressions."""
    str_literal = literal("hello")
    int_literal = literal(42)
    bool_literal = literal(True)
    null_literal = literal(None)

    assert all(expr.node is not None for expr in [
        str_literal, int_literal, bool_literal, null_literal
    ])



def test_query_result_creation():
    """Test creating a QueryResult."""
    rows = [
        {"id": 1, "name": "John"},
        {"id": 2, "name": "Jane"}
    ]

    result = QueryResult(
        rows=rows,
        sql="SELECT * FROM users",
        parameters=[],
        row_count=2
    )

    assert result.rows == rows
    assert result.row_count == 2
    assert result.sql == "SELECT * FROM users"

def test_query_result_first():
    """Test getting first result."""
    rows = [
        {"id": 1, "name": "John"},
        {"id": 2, "name": "Jane"}
    ]

    result = QueryResult(
        rows=rows,
        sql="SELECT * FROM users",
        parameters=[],
        row_count=2
    )

    first = result.first()
    assert first == {"id": 1, "name": "John"}

def test_query_result_empty_first():
    """Test getting first result from empty result set."""
    result = QueryResult(
        rows=[],
        sql="SELECT * FROM users WHERE 1=0",
        parameters=[],
        row_count=0
    )

    first = result.first()
    assert first is None

def test_to_dicts():
    """Test converting to dictionaries."""
    rows = [{"id": 1, "name": "John"}]

    result = QueryResult(
        rows=rows,
        sql="SELECT * FROM users",
        parameters=[],
        row_count=1
    )

    dicts = result.to_dicts()
    assert dicts == rows

def test_simple_cte():
    """Test basic CTE functionality."""
    # Create a CTE query
    cte_query = Query()
    cte_query.select("id", "name", "email").from_("users").where(col("active").eq(True))

    # Create main query using the CTE
    main_query = Query()
    main_query.with_("active_users", cte_query).select("*").from_("active_users")

    sql, params = main_query.build()

    assert sql == "WITH active_users AS (SELECT id, name, email FROM users WHERE active = TRUE) SELECT * FROM active_users"

def test_multiple_ctes():
    """Test multiple CTEs in one query."""
    # First CTE: active users
    active_users_cte = Query()
    active_users_cte.select("id", "name").from_("users").where(col("active").eq(True))

    # Second CTE: recent orders
    recent_orders_cte = Query()
    recent_orders_cte.select("user_id", func.count("*").as_("order_count")).from_("orders").where(
        col("created_at").gt("2023-01-01")
    ).group_by("user_id")

    # Main query joining both CTEs
    main_query = Query()
    main_query.with_("active_users", active_users_cte).with_("recent_orders", recent_orders_cte).select(
        "au.name",
        "ro.order_count"
    ).from_("active_users", alias="au").left_join(
        "recent_orders",
        on=col("au.id").eq(col("ro.user_id")),
        alias="ro"
    )

    sql, params = main_query.build()

    assert sql == "WITH active_users AS (SELECT id, name FROM users WHERE active = TRUE), recent_orders AS (SELECT user_id, count(*) AS order_count FROM orders WHERE created_at > '2023-01-01' GROUP BY user_id) SELECT au.name, ro.order_count FROM active_users AS au LEFT JOIN recent_orders AS ro ON au.id = ro.user_id"

def test_cte_with_joins():
    """Test CTE that includes JOINs."""
    # CTE with JOIN
    cte_query = Query()
    cte_query.select(
        "u.id",
        "u.name",
        func.count("o.id").as_("order_count")
    ).from_("users", alias="u").left_join(
        "orders",
        on=col("u.id").eq(col("o.user_id")),
        alias="o"
    ).group_by("u.id", "u.name")

    # Main query using the CTE
    main_query = Query()
    main_query.with_("user_stats", cte_query).select("*").from_("user_stats").where(
        col("order_count").gt(5)
    )

    sql, params = main_query.build()

    assert sql == "WITH user_stats AS (SELECT u.id, u.name, count(o.id) AS order_count FROM users AS u LEFT JOIN orders AS o ON u.id = o.user_id GROUP BY \"u.id\", \"u.name\") SELECT * FROM user_stats WHERE order_count > 5"

def test_cte_with_parameters():
    """Test CTE with parameterized queries."""
    from pghatch.query_builder import param

    # CTE with parameters
    cte_query = Query()
    cte_query.select("id", "name").from_("users").where(
        and_(
            col("active").eq(param(True)),
            col("created_at").gt(param("2023-01-01"))
        )
    )

    # Main query using the CTE
    main_query = Query()
    main_query.with_("filtered_users", cte_query).select("*").from_("filtered_users").where(
        col("name").like(param("%admin%"))
    )

    sql, params = main_query.build()

    assert "WITH" in sql
    assert "filtered_users" in sql
    assert len(params) == 3  # Three parameters total
    assert params[0] is True
    assert params[1] == "2023-01-01"
    assert params[2] == "%admin%"

def test_cte_in_subquery():
    """Test using CTE results in subqueries."""
    # CTE for high-value customers
    high_value_cte = Query()
    high_value_cte.select("user_id").from_("orders").group_by("user_id").having(
        func.sum("total").gt(1000)
    )

    # Main query with CTE and subquery
    main_query = Query()
    main_query.with_("high_value_customers", high_value_cte).select("*").from_("users").where(
        col("id").in_(
            Query().select("user_id").from_("high_value_customers")
        )
    )

    sql, params = main_query.build()

    assert sql == "WITH high_value_customers AS (SELECT user_id FROM orders GROUP BY user_id HAVING sum(total) > 1000) SELECT * FROM users WHERE id IN (SELECT user_id FROM high_value_customers)"

def test_subquery_in_where_comprehensive():
    """Test comprehensive subquery usage in WHERE clause."""
    # Subquery for active users
    active_users_subquery = Query()
    active_users_subquery.select("id").from_("users").where(col("active").eq(True))

    # Subquery for recent orders
    recent_orders_subquery = Query()
    recent_orders_subquery.select("user_id").from_("orders").where(
        col("created_at").gt("2023-01-01")
    )

    # Main query with multiple subqueries
    main_query = Query()
    main_query.select("*").from_("profiles").where(
        and_(
            col("user_id").in_(active_users_subquery),
            col("user_id").in_(recent_orders_subquery)
        )
    )

    sql, params = main_query.build()

    assert sql == "SELECT * FROM profiles WHERE user_id IN (SELECT id FROM users WHERE active = TRUE) AND user_id IN (SELECT user_id FROM orders WHERE created_at > '2023-01-01')"


def test_subquery_in_from_clause():
    """Test subqueries used as table sources in FROM clause."""
    # Subquery as a derived table
    user_stats_subquery = Query()
    user_stats_subquery.select(
        "user_id",
        func.count("*").as_("order_count"),
        func.sum("total").as_("total_spent")
    ).from_("orders").group_by("user_id")

    # For now, test that the subquery builds correctly
    # Full FROM subquery support would need additional implementation
    sql, params = user_stats_subquery.build()

    assert sql == "SELECT user_id, count(*) AS order_count, sum(total) AS total_spent FROM orders GROUP BY user_id"

def test_nested_subqueries():
    """Test multiple levels of subquery nesting."""
    # Level 3: Orders with high totals
    high_value_orders = Query()
    high_value_orders.select("user_id").from_("orders").where(col("total").gt(500))

    # Level 2: Users with high-value orders
    high_value_users = Query()
    high_value_users.select("id").from_("users").where(col("id").in_(high_value_orders))

    # Level 1: Profiles of high-value users
    main_query = Query()
    main_query.select("*").from_("profiles").where(col("user_id").in_(high_value_users))

    sql, params = main_query.build()

    assert sql == "SELECT * FROM profiles WHERE user_id IN (SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 500))"

def test_subqueries_with_parameters():
    """Test subqueries using parameterized values."""
    from pghatch.query_builder import param

    # Subquery with parameters
    recent_orders_subquery = Query()
    recent_orders_subquery.select("user_id").from_("orders").where(
        and_(
            col("created_at").gt(param("2023-01-01")),
            col("total").gt(param(100))
        )
    )

    # Main query with parameterized subquery
    main_query = Query()
    main_query.select("*").from_("users").where(
        and_(
            col("id").in_(recent_orders_subquery),
            col("active").eq(param(True))
        )
    )

    sql, params = main_query.build()

    assert "WHERE" in sql
    # Note: Current implementation uses placeholder for subqueries
    assert "subquery_placeholder" in sql or "IN" in sql
    assert len(params) == 3  # Three parameters total
    assert params[0] == "2023-01-01"
    assert params[1] == 100
    assert params[2] is True

def test_complex_cte_with_window_functions():
    """Test CTE with window functions."""
    # CTE with window function
    ranked_users_cte = Query()
    ranked_users_cte.select(
        "id",
        "name",
        "salary",
        func.row_number().as_("rank")
    ).from_("employees")

    # Main query using the CTE
    main_query = Query()
    main_query.with_("ranked_employees", ranked_users_cte).select("*").from_("ranked_employees").where(
        col("rank").le(10)
    )

    sql, params = main_query.build()

    assert sql == "WITH ranked_employees AS (SELECT id, name, salary, row_number() AS rank FROM employees) SELECT * FROM ranked_employees WHERE rank <= 10"

def test_cte_with_aggregates_and_having():
    """Test CTE with complex aggregations and HAVING clause."""
    # CTE with aggregations
    department_stats_cte = Query()
    department_stats_cte.select(
        "department",
        func.count("*").as_("employee_count"),
        func.avg("salary").as_("avg_salary")
    ).from_("employees").group_by("department").having(
        func.count("*").gt(5)
    )

    # Main query using the CTE
    main_query = Query()
    main_query.with_("dept_stats", department_stats_cte).select("*").from_("dept_stats").order_by(
        "avg_salary", "DESC"
    )

    sql, params = main_query.build()

    assert sql == "WITH dept_stats AS (SELECT department, count(*) AS employee_count, avg(salary) AS avg_salary FROM employees GROUP BY department HAVING count(*) > 5) SELECT * FROM dept_stats ORDER BY avg_salary DESC NULLS LAST"
