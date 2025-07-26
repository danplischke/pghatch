"""
Simplified tests for the query builder module.
"""

from unittest.mock import Mock
from pghatch.query import Query, col, func, literal, and_, or_, not_
from pghatch.query.builder.types import QueryResult
from pghatch.introspection.introspection import Introspection


class TestQueryBuilderBasic:
    """Basic test cases for the QueryBuilder class."""

    def test_simple_select(self):
        """Test building a simple SELECT query."""
        qb = Query()
        qb.select("id", "name", "email").from_("users")

        sql, params = qb.build()

        assert "SELECT" in sql
        assert "id" in sql
        assert "name" in sql
        assert "email" in sql
        assert "users" in sql
        assert params == []

    def test_select_all(self):
        """Test SELECT * query."""
        qb = Query()
        qb.select_all().from_("users")

        sql, params = qb.build()

        assert "SELECT *" in sql
        assert "users" in sql

    def test_where_clause(self):
        """Test WHERE clause with expressions."""
        qb = Query()
        qb.select("*").from_("users").where(col("active").eq(True))

        sql, params = qb.build()

        assert "WHERE" in sql
        assert "active" in sql

    def test_multiple_where_conditions(self):
        """Test multiple WHERE conditions combined with AND."""
        qb = Query()
        qb.select("*").from_("users").where(col("active").eq(True)).where(col("age").gt(18))

        sql, params = qb.build()

        assert "WHERE" in sql
        assert "active" in sql
        assert "age" in sql
        assert "AND" in sql

    def test_order_by(self):
        """Test ORDER BY clause."""
        qb = Query()
        qb.select("*").from_("users").order_by("name").order_by("created_at", "DESC")

        sql, params = qb.build()

        assert "ORDER BY" in sql
        assert "name" in sql
        assert "created_at" in sql
        assert "DESC" in sql

    def test_limit_and_offset(self):
        """Test LIMIT and OFFSET clauses."""
        qb = Query()
        qb.select("*").from_("users").limit(10).offset(20)

        sql, params = qb.build()

        assert "LIMIT" in sql
        assert "OFFSET" in sql
        assert "10" in sql
        assert "20" in sql

    def test_distinct(self):
        """Test DISTINCT clause."""
        qb = Query()
        qb.select("department").from_("employees").distinct()

        sql, params = qb.build()

        assert "DISTINCT" in sql

    def test_string_representation(self):
        """Test string representation of QueryBuilder."""
        qb = Query()
        qb.select("*").from_("users").where(col("active").eq(True))

        sql_str = str(qb)
        repr_str = repr(qb)

        assert "SELECT" in sql_str
        assert "QueryBuilder" in repr_str
        assert "sql=" in repr_str

    def test_with_introspection(self):
        """Test QueryBuilder with introspection data."""
        # Mock introspection
        mock_introspection = Mock(spec=Introspection)
        mock_introspection.procs = []

        qb = Query(introspection=mock_introspection)

        assert qb.introspection is mock_introspection
        assert qb.functions.introspection is mock_introspection


class TestExpressions:
    """Test cases for expression builders."""

    def test_column_expressions(self):
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

    def test_logical_expressions(self):
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

    def test_basic_function_expressions(self):
        """Test basic function expression building."""
        # Test aggregate functions
        count_expr = func.count("*")
        sum_expr = func.sum("amount")
        avg_expr = func.avg("score")

        # Test string functions
        upper_expr = func.upper("name")
        lower_expr = func.lower("name")

        # Test date functions
        now_expr = func.now()

        assert all(expr.node is not None for expr in [
            count_expr, sum_expr, avg_expr, upper_expr,
            lower_expr, now_expr
        ])

    def test_literal_expressions(self):
        """Test literal value expressions."""
        str_literal = literal("hello")
        int_literal = literal(42)
        bool_literal = literal(True)

        assert all(expr.node is not None for expr in [
            str_literal, int_literal, bool_literal
        ])


class TestQueryResult:
    """Test cases for QueryResult class."""

    def test_query_result_creation(self):
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

    def test_query_result_first(self):
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

    def test_query_result_empty_first(self):
        """Test getting first result from empty result set."""
        result = QueryResult(
            rows=[],
            sql="SELECT * FROM users WHERE 1=0",
            parameters=[],
            row_count=0
        )

        first = result.first()
        assert first is None

    def test_to_dicts(self):
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
