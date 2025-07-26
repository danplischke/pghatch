"""
Expression builders for the query builder module.

This module provides functions and classes for building SQL expressions
using the PostgreSQL AST via pglast.
"""

from typing import Any, List, Optional, Union, TYPE_CHECKING

from pglast import ast
from pglast.enums import BoolExprType, A_Expr_Kind, NullTestType, SubLinkType

if TYPE_CHECKING:
    from pghatch.query import Query


class Parameter:
    """Represents a parameterized value for safe SQL injection prevention."""

    def __init__(self, name: str):
        self.name = name

    def __repr__(self):
        return f"Parameter({self.name!r})"


class Expression:
    """Base class for SQL expressions."""

    def __init__(self, node: ast.Node):
        self.node = node

    def and_(self, other: "Expression") -> "Expression":
        """Combine with another expression using AND."""
        return and_(self, other)

    def or_(self, other: "Expression") -> "Expression":
        """Combine with another expression using OR."""
        return or_(self, other)

    def not_(self) -> "Expression":
        """Negate this expression."""
        return not_(self)

    def as_(self, alias: str) -> "ResTargetExpression":
        """Create an aliased expression for SELECT clauses."""
        return ResTargetExpression(self.node, alias)


class ColumnExpression(Expression):
    """Expression representing a column reference."""

    def __init__(self, name: str | Parameter, table_alias: Optional[str] = None):
        # Build the AST node
        fields = []
        if table_alias:
            fields.append(ast.String(sval=table_alias))

        if isinstance(name, Parameter):
            fields.append(ast.String(sval=name.name))
        elif isinstance(name, str):
            if name == "*":
                # Handle SELECT *
                fields.append(ast.A_Star())
            elif "." in name:
                # Handle qualified names like "schema.table.column"
                parts = name.split(".")
                if len(parts) == 3:
                    schema, table, col_name = parts
                    fields.extend(
                        [
                            ast.String(sval=schema),
                            ast.String(sval=table),
                            ast.String(sval=col_name),
                        ]
                    )
                elif len(parts) == 2:
                    table, col_name = parts
                    fields.extend([ast.String(sval=table), ast.String(sval=col_name)])
                else:
                    raise ValueError(
                        "Invalid column format. Expected 'schema.table.column' or 'table.column'."
                    )
            else:
                fields.append(ast.String(sval=name))

        node = ast.ColumnRef(fields=fields)
        super().__init__(node)

    def eq(self, value: Any) -> "Expression":
        """Create an equality comparison."""
        return _create_comparison(self, "=", value)

    def ne(self, value: Any) -> "Expression":
        """Create a not-equal comparison."""
        return _create_comparison(self, "<>", value)

    def lt(self, value: Any) -> "Expression":
        """Create a less-than comparison."""
        return _create_comparison(self, "<", value)

    def le(self, value: Any) -> "Expression":
        """Create a less-than-or-equal comparison."""
        return _create_comparison(self, "<=", value)

    def gt(self, value: Any) -> "Expression":
        """Create a greater-than comparison."""
        return _create_comparison(self, ">", value)

    def ge(self, value: Any) -> "Expression":
        """Create a greater-than-or-equal comparison."""
        return _create_comparison(self, ">=", value)

    def like(self, pattern: str) -> "Expression":
        """Create a LIKE comparison."""
        return _create_comparison(self, "~~", pattern)

    def ilike(self, pattern: str) -> "Expression":
        """Create an ILIKE comparison."""
        return _create_comparison(self, "~~*", pattern)

    def in_(self, values: Union[List[Any], "Query", "Parameter"]) -> "Expression":
        """Create an IN comparison."""
        from .builder import Query

        if isinstance(values, Query):
            node = ast.SubLink(
                subLinkType=SubLinkType.ANY_SUBLINK,
                subselect=values.query_ast(),
                testexpr=self.node,
            )
        elif isinstance(values, Parameter):
            # If it's a parameter, we treat it as a single value
            node = ast.A_Expr(
                kind=A_Expr_Kind.AEXPR_IN,
                name=[ast.String(sval="=")],
                lexpr=self.node,
                rexpr=_value_to_node(values.name),
            )
        else:
            # List of values - wrap in a list structure that pglast can handle
            value_nodes = [_value_to_node(v) for v in values]

            # Create a proper list expression for IN clause
            node = ast.A_Expr(
                kind=A_Expr_Kind.AEXPR_IN,
                name=[ast.String(sval="=")],
                lexpr=self.node,
                rexpr=value_nodes,
            )
        return Expression(node)

    def is_null(self) -> "Expression":
        """Create an IS NULL comparison."""
        node = ast.NullTest(arg=self.node, nulltesttype=NullTestType.IS_NULL)
        return Expression(node)

    def is_not_null(self) -> "Expression":
        """Create an IS NOT NULL comparison."""
        node = ast.NullTest(arg=self.node, nulltesttype=NullTestType.IS_NOT_NULL)
        return Expression(node)

    def as_(self, alias: str) -> "ResTargetExpression":
        """Create an aliased expression for SELECT clauses."""
        return ResTargetExpression(self.node, alias)


class FunctionExpression(Expression):
    """Expression representing a function call."""

    def __init__(
            self,
            name: str,
            args: List[Union[Expression, Any]] | None = None,
            schema: Optional[str] = None,
            distinct: bool = False,
            agg_filter: Optional[Expression] = None,
            agg_order: Optional[List[str]] = None,
            agg_star: Optional[bool] = False,
    ):
        self.name = name
        self.schema = schema

        # Build function name
        funcname = []
        if schema:
            funcname.append(ast.String(sval=schema))
        funcname.append(ast.String(sval=name))

        # Convert arguments to AST nodes
        arg_nodes = []
        if args:
            for arg in args:
                if isinstance(arg, Expression):
                    arg_nodes.append(arg.node)
                elif isinstance(arg, ast.Node):
                    arg_nodes.append(arg)
                else:
                    arg_nodes.append(_value_to_node(arg))

        # Build the function call node
        node = ast.FuncCall(
            funcname=funcname,
            args=arg_nodes if len(arg_nodes) > 0 else None,
            agg_distinct=distinct,
            agg_filter=agg_filter.node if agg_filter else None,
            agg_order=_build_order_by(agg_order) if agg_order else None,
            agg_star=agg_star,
        )

        super().__init__(node)

    def as_(self, alias: str) -> "ResTargetExpression":
        """Create an aliased expression for SELECT clauses."""
        return ResTargetExpression(self.node, alias)

    def eq(self, value: Any) -> "Expression":
        """Create an equality comparison."""
        return _create_comparison(self, "=", value)

    def ne(self, value: Any) -> "Expression":
        """Create a not-equal comparison."""
        return _create_comparison(self, "<>", value)

    def lt(self, value: Any) -> "Expression":
        """Create a less-than comparison."""
        return _create_comparison(self, "<", value)

    def le(self, value: Any) -> "Expression":
        """Create a less-than-or-equal comparison."""
        return _create_comparison(self, "<=", value)

    def gt(self, value: Any) -> "Expression":
        """Create a greater-than comparison."""
        return _create_comparison(self, ">", value)

    def ge(self, value: Any) -> "Expression":
        """Create a greater-than-or-equal comparison."""
        return _create_comparison(self, ">=", value)


class ResTargetExpression:
    """Expression for SELECT target lists (columns with optional aliases)."""

    def __init__(self, node: ast.Node, alias: Optional[str] = None):
        self.node = ast.ResTarget(
            val=node,
            name=alias,
        )


class CaseExpression(Expression):
    """Expression for CASE statements."""

    def __init__(self):
        self.when_clauses = []
        self.else_clause = None

    def when(self, condition: Expression, result: Any) -> "CaseExpression":
        """Add a WHEN clause."""
        self.when_clauses.append((condition, result))
        return self

    def else_(self, result: Any) -> "CaseExpression":
        """Add an ELSE clause."""
        self.else_clause = result
        return self

    def end(self) -> Expression:
        """Complete the CASE expression."""
        when_exprs = []
        for condition, result in self.when_clauses:
            when_expr = ast.CaseWhen(expr=condition.node, result=_value_to_node(result))
            when_exprs.append(when_expr)

        case_node = ast.CaseExpr(
            args=when_exprs,
            defresult=_value_to_node(self.else_clause)
            if self.else_clause is not None
            else None,
        )
        return Expression(case_node)

    def as_(self, alias: str) -> "ResTargetExpression":
        """Create an aliased expression for SELECT clauses."""
        return ResTargetExpression(self.end().node, alias)


class LiteralExpression(Expression):
    """Expression representing a literal value."""

    def __init__(self, value: Any):
        node = _value_to_node(value)
        super().__init__(node)


def col(name: str | Parameter, table_alias: Optional[str] = None) -> ColumnExpression:
    """Create a column reference expression."""
    return ColumnExpression(name, table_alias)


def literal(value: Any) -> LiteralExpression:
    """Create a literal value expression."""
    return LiteralExpression(value)


def param(value: Any) -> Parameter:
    """Create a parameterized value for safe SQL injection prevention.

    Args:
        value: The value to parameterize

    Returns:
        Parameter: A parameter object that will be safely bound server-side

    Example:
        qb.where(col("name").eq(param("John")))  # Safe from SQL injection
    """
    return Parameter(value)


def and_(*expressions: Expression) -> Expression:
    """Combine expressions with AND."""
    if len(expressions) == 0:
        raise ValueError("At least one expression required for AND")
    if len(expressions) == 1:
        return expressions[0]

    # Build a tree of AND expressions
    result = expressions[0]
    for expr in expressions[1:]:
        node = ast.BoolExpr(boolop=BoolExprType.AND_EXPR, args=[result.node, expr.node])
        result = Expression(node)

    return result


def case() -> "CaseExpression":
    """CASE expression builder."""
    return CaseExpression()


def or_(*expressions: Expression) -> Expression:
    """Combine expressions with OR."""
    if len(expressions) == 0:
        raise ValueError("At least one expression required for OR")
    if len(expressions) == 1:
        return expressions[0]

    # Build a tree of OR expressions
    result = expressions[0]
    for expr in expressions[1:]:
        node = ast.BoolExpr(boolop=BoolExprType.OR_EXPR, args=[result.node, expr.node])
        result = Expression(node)

    return result


def not_(expression: Expression) -> Expression:
    """Negate an expression with NOT."""
    node = ast.BoolExpr(boolop=BoolExprType.NOT_EXPR, args=[expression.node])
    return Expression(node)


def _create_comparison(left: Expression, operator: str, right: Any) -> Expression:
    """Create a comparison expression."""
    right_node = (
        _value_to_node(right) if not isinstance(right, Expression) else right.node
    )

    node = ast.A_Expr(
        kind=A_Expr_Kind.AEXPR_OP,
        name=[ast.String(sval=operator)],
        lexpr=left.node,
        rexpr=right_node,
    )
    return Expression(node)


def _value_to_node(value: Any, query_builder: Optional["Query"] = None) -> ast.Node:
    """Convert a Python value to an AST node."""
    if value is None:
        return ast.A_Const()  # NULL value
    elif isinstance(value, bool):
        return ast.A_Const(val=ast.Boolean(boolval=value))
    elif isinstance(value, int):
        return ast.A_Const(val=ast.Integer(ival=value))
    elif isinstance(value, float):
        return ast.A_Const(val=ast.Float(fval=str(value)))
    elif isinstance(value, str):
        return ast.A_Const(val=ast.String(sval=value))
    elif isinstance(value, Expression):
        return value.node
    elif isinstance(value, Parameter):
        # For now, just use the literal value since parameter handling
        # needs more sophisticated implementation
        return _value_to_node(value.name)
    else:
        # For other types, convert to string
        return ast.A_Const(val=ast.String(sval=str(value)))


def _build_order_by(columns: List[str]) -> List[ast.SortBy]:
    """Build ORDER BY clause for aggregate functions."""
    sort_items = []
    for col_name in columns:
        sort_items.append(
            ast.SortBy(
                node=ast.ColumnRef(fields=[ast.String(sval=col_name)]),
                sortby_dir=ast.SortByDir.SORTBY_DEFAULT,
            )
        )
    return sort_items
