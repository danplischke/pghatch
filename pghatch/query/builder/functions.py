"""
PostgreSQL function registry and builders.

This module provides comprehensive support for PostgreSQL built-in functions
and user-defined functions discovered through introspection.
"""

from typing import Any, List, Optional, Union

from pglast import ast

from pghatch.introspection.introspection import Introspection
from .expressions import Expression, FunctionExpression, col, literal


def count(
        expr: Optional[Union[Expression, str]] = None, distinct: bool = False
) -> FunctionExpression:
    """COUNT aggregate function."""
    if expr is None or expr == "*":
        # COUNT(*)
        return FunctionExpression("count", None, agg_star=True, distinct=distinct)
    elif isinstance(expr, str):
        args = [col(expr)]
    else:
        args = [expr]
    return FunctionExpression("count", args, distinct=distinct)


def sum(expr: Union[Expression, str], distinct: bool = False) -> FunctionExpression:
    """SUM aggregate function."""
    args = [col(expr) if isinstance(expr, str) else expr]
    return FunctionExpression("sum", args, distinct=distinct)


def avg(expr: Union[Expression, str], distinct: bool = False) -> FunctionExpression:
    """AVG aggregate function."""
    args = [col(expr) if isinstance(expr, str) else expr]
    return FunctionExpression("avg", args, distinct=distinct)


def max(expr: Union[Expression, str]) -> FunctionExpression:
    """MAX aggregate function."""
    args = [col(expr) if isinstance(expr, str) else expr]
    return FunctionExpression("max", args)


def min(expr: Union[Expression, str]) -> FunctionExpression:
    """MIN aggregate function."""
    args = [col(expr) if isinstance(expr, str) else expr]
    return FunctionExpression("min", args)


def upper(expr: Union[Expression, str]) -> FunctionExpression:
    """UPPER string function."""
    args = [col(expr) if isinstance(expr, str) else expr]
    return FunctionExpression("upper", args)


def lower(expr: Union[Expression, str]) -> FunctionExpression:
    """LOWER string function."""
    args = [col(expr) if isinstance(expr, str) else expr]
    return FunctionExpression("lower", args)


def length(expr: Union[Expression, str]) -> FunctionExpression:
    """LENGTH string function."""
    args = [col(expr) if isinstance(expr, str) else expr]
    return FunctionExpression("length", args)


def coalesce(*exprs: Union[Expression, str, Any]) -> FunctionExpression:
    """COALESCE function."""
    args = []
    for expr in exprs:
        if isinstance(expr, str):
            args.append(col(expr))
        elif isinstance(expr, Expression):
            args.append(expr)
        else:
            args.append(literal(expr))
    return FunctionExpression("coalesce", args)


def date_trunc(precision: str, expr: Union[Expression, str]) -> FunctionExpression:
    """DATE_TRUNC function."""
    args = [literal(precision), col(expr) if isinstance(expr, str) else expr]
    return FunctionExpression("date_trunc", args)


def now() -> FunctionExpression:
    """NOW() function."""
    return FunctionExpression("now", [])


def current_timestamp() -> FunctionExpression:
    """CURRENT_TIMESTAMP function."""
    return FunctionExpression("current_timestamp", [])


def concat(*exprs: Union[Expression, str, Any]) -> FunctionExpression:
    """CONCAT function."""
    args = []
    for expr in exprs:
        if isinstance(expr, str):
            args.append(literal(expr))
        elif isinstance(expr, Expression):
            args.append(expr)
        else:
            args.append(literal(expr))
    return FunctionExpression("concat", args)


def json_extract_path_text(
        json_expr: Union[Expression, str], *path_elements: str
) -> FunctionExpression:
    """JSON_EXTRACT_PATH_TEXT function."""
    args = [col(json_expr) if isinstance(json_expr, str) else json_expr]
    for path in path_elements:
        args.append(literal(path))
    return FunctionExpression("json_extract_path_text", args)


def jsonb_extract_path_text(
        json_expr: Union[Expression, str], *path_elements: str
) -> FunctionExpression:
    """JSONB_EXTRACT_PATH_TEXT function."""
    args = [col(json_expr) if isinstance(json_expr, str) else json_expr]
    for path in path_elements:
        args.append(literal(path))
    return FunctionExpression("jsonb_extract_path_text", args)


def row_number() -> FunctionExpression:
    """ROW_NUMBER() window function."""
    return FunctionExpression("row_number", [])


def array_length(
        array_expr: Union[Expression, str], dimension: int = 1
) -> FunctionExpression:
    """ARRAY_LENGTH function."""
    args = [
        col(array_expr) if isinstance(array_expr, str) else array_expr,
        literal(dimension),
    ]
    return FunctionExpression("array_length", args)


def rank() -> FunctionExpression:
    """RANK() window function."""
    return FunctionExpression("rank", [])


def substring(
        string: Union[Expression, str], start: int, length: Optional[int] = None
) -> FunctionExpression:
    """SUBSTRING function."""
    args = [col(string) if isinstance(string, str) else string, literal(start)]
    if length is not None:
        args.append(literal(length))
    return FunctionExpression("substring", args)


def trim(
        string: Union[Expression, str], chars: Optional[str] = None
) -> FunctionExpression:
    """TRIM function."""
    args = [col(string) if isinstance(string, str) else string]
    if chars:
        args.append(literal(chars))
    return FunctionExpression("trim", args)


def ltrim(
        string: Union[Expression, str], chars: Optional[str] = None
) -> FunctionExpression:
    """LTRIM function."""
    args = [col(string) if isinstance(string, str) else string]
    if chars:
        args.append(literal(chars))
    return FunctionExpression("ltrim", args)


def rtrim(
        string: Union[Expression, str], chars: Optional[str] = None
) -> FunctionExpression:
    """RTRIM function."""
    args = [col(string) if isinstance(string, str) else string]
    if chars:
        args.append(literal(chars))
    return FunctionExpression("rtrim", args)


def replace(
         string: Union[Expression, str], from_str: str, to_str: str
) -> FunctionExpression:
    """REPLACE function."""
    args = [
        col(string) if isinstance(string, str) else string,
        literal(from_str),
        literal(to_str),
    ]
    return FunctionExpression("replace", args)


def split_part(
        string: Union[Expression, str], delimiter: str, field: int
) -> FunctionExpression:
    """SPLIT_PART function."""
    args = [
        col(string) if isinstance(string, str) else string,
        literal(delimiter),
        literal(field),
    ]
    return FunctionExpression("split_part", args)


def regexp_replace(
        string: Union[Expression, str],
        pattern: str,
        replacement: str,
        flags: Optional[str] = None,
) -> FunctionExpression:
    """REGEXP_REPLACE function."""
    args = [
        col(string) if isinstance(string, str) else string,
        literal(pattern),
        literal(replacement),
    ]
    if flags:
        args.append(literal(flags))
    return FunctionExpression("regexp_replace", args)


# Mathematical Functions
def abs(value: Union[Expression, str]) -> FunctionExpression:
    """ABS function."""
    args = [col(value) if isinstance(value, str) else value]
    return FunctionExpression("abs", args)


def ceil(value: Union[Expression, str]) -> FunctionExpression:
    """CEIL function."""
    args = [col(value) if isinstance(value, str) else value]
    return FunctionExpression("ceil", args)


def floor(value: Union[Expression, str]) -> FunctionExpression:
    """FLOOR function."""
    args = [col(value) if isinstance(value, str) else value]
    return FunctionExpression("floor", args)


def round(
         value: Union[Expression, str], precision: Optional[int] = None
) -> FunctionExpression:
    """ROUND function."""
    args = [col(value) if isinstance(value, str) else value]
    if precision is not None:
        args.append(literal(precision))
    return FunctionExpression("round", args)


def power(
         base: Union[Expression, str], exponent: Union[Expression, str, int]
) -> FunctionExpression:
    """POWER function."""
    args = [
        col(base) if isinstance(base, str) else base,
        col(exponent)
        if isinstance(exponent, str)
        else literal(exponent)
        if isinstance(exponent, (int, float))
        else exponent,
    ]
    return FunctionExpression("power", args)


def sqrt( value: Union[Expression, str]) -> FunctionExpression:
    """SQRT function."""
    args = [col(value) if isinstance(value, str) else value]
    return FunctionExpression("sqrt", args)


def random() -> FunctionExpression:
    """RANDOM function."""
    return FunctionExpression("random", [])


# Date/Time Functions
def extract(field: str, source: Union[Expression, str]) -> FunctionExpression:
    """EXTRACT function."""
    # EXTRACT is a special case - it uses different syntax
    # For now, we'll use the function call syntax
    args = [literal(field), col(source) if isinstance(source, str) else source]
    return FunctionExpression("extract", args)


def date_part(
        field: str, source: Union[Expression, str]
) -> FunctionExpression:
    """DATE_PART function."""
    args = [literal(field), col(source) if isinstance(source, str) else source]
    return FunctionExpression("date_part", args)


def age(
        timestamp1: Union[Expression, str],
        timestamp2: Optional[Union[Expression, str]] = None,
) -> FunctionExpression:
    """AGE function."""
    args = [col(timestamp1) if isinstance(timestamp1, str) else timestamp1]
    if timestamp2:
        args.append(col(timestamp2) if isinstance(timestamp2, str) else timestamp2)
    return FunctionExpression("age", args)


def to_char(value: Union[Expression, str], format: str) -> FunctionExpression:
    """TO_CHAR function."""
    args = [col(value) if isinstance(value, str) else value, literal(format)]
    return FunctionExpression("to_char", args)


def to_date( text: Union[Expression, str], format: str) -> FunctionExpression:
    """TO_DATE function."""
    args = [col(text) if isinstance(text, str) else text, literal(format)]
    return FunctionExpression("to_date", args)


def to_timestamp(
        text: Union[Expression, str], format: str
) -> FunctionExpression:
    """TO_TIMESTAMP function."""
    args = [col(text) if isinstance(text, str) else text, literal(format)]
    return FunctionExpression("to_timestamp", args)


# JSON Functions
def json_extract_path(
        json_col: Union[Expression, str], *path: str
) -> FunctionExpression:
    """JSON_EXTRACT_PATH function."""
    args = [col(json_col) if isinstance(json_col, str) else json_col]
    args.extend([literal(p) for p in path])
    return FunctionExpression("json_extract_path", args)


def jsonb_extract_path(
        jsonb_col: Union[Expression, str], *path: str
) -> FunctionExpression:
    """JSONB_EXTRACT_PATH function."""
    args = [col(jsonb_col) if isinstance(jsonb_col, str) else jsonb_col]
    args.extend([literal(p) for p in path])
    return FunctionExpression("jsonb_extract_path", args)


def json_array_length( json_col: Union[Expression, str]) -> FunctionExpression:
    """JSON_ARRAY_LENGTH function."""
    args = [col(json_col) if isinstance(json_col, str) else json_col]
    return FunctionExpression("json_array_length", args)


def jsonb_array_length(
         jsonb_col: Union[Expression, str]
) -> FunctionExpression:
    """JSONB_ARRAY_LENGTH function."""
    args = [col(jsonb_col) if isinstance(jsonb_col, str) else jsonb_col]
    return FunctionExpression("jsonb_array_length", args)


def json_build_object(
        *pairs: Union[Expression, str, Any], use_col_names: bool = False
) -> FunctionExpression:
    """JSON_BUILD_OBJECT function."""
    args = []
    for pair in pairs:
        if isinstance(pair, str):
            args.append(ast.A_Const(val=ast.String(sval=pair)))
        elif isinstance(pair, Expression):
            args.append(pair)
        else:
            args.append(literal(pair))
    return FunctionExpression("json_build_object", args)


def json_agg(
        expression: Union[Expression, str]
) -> FunctionExpression:
    """JSON_AGG function."""
    args = [col(expression) if isinstance(expression, str) else expression]
    return FunctionExpression("json_agg", args)


def jsonb_build_object(
        *pairs: Union[Expression, str, Any]
) -> FunctionExpression:
    """JSONB_BUILD_OBJECT function."""
    args = []
    for pair in pairs:
        if isinstance(pair, str):
            args.append(col(pair))
        elif isinstance(pair, Expression):
            args.append(pair)
        else:
            args.append(literal(pair))
    return FunctionExpression("jsonb_build_object", args)


def dense_rank(self) -> FunctionExpression:
    """DENSE_RANK window function."""
    return FunctionExpression("dense_rank", [])


def lag(
        self, expr: Union[Expression, str], offset: int = 1, default: Any = None
) -> FunctionExpression:
    """LAG window function."""
    args = [col(expr) if isinstance(expr, str) else expr, literal(offset)]
    if default is not None:
        args.append(literal(default))
    return FunctionExpression("lag", args)


def lead(
        self, expr: Union[Expression, str], offset: int = 1, default: Any = None
) -> FunctionExpression:
    """LEAD window function."""
    args = [col(expr) if isinstance(expr, str) else expr, literal(offset)]
    if default is not None:
        args.append(literal(default))
    return FunctionExpression("lead", args)


def first_value(self, expr: Union[Expression, str]) -> FunctionExpression:
    """FIRST_VALUE window function."""
    args = [col(expr) if isinstance(expr, str) else expr]
    return FunctionExpression("first_value", args)


def last_value(self, expr: Union[Expression, str]) -> FunctionExpression:
    """LAST_VALUE window function."""
    args = [col(expr) if isinstance(expr, str) else expr]
    return FunctionExpression("last_value", args)


def nth_value(self, expr: Union[Expression, str], n: int) -> FunctionExpression:
    """NTH_VALUE window function."""
    args = [col(expr) if isinstance(expr, str) else expr, literal(n)]
    return FunctionExpression("nth_value", args)


def array_append(
        self, array: Union[Expression, str], element: Any
) -> FunctionExpression:
    """ARRAY_APPEND function."""
    args = [col(array) if isinstance(array, str) else array, literal(element)]
    return FunctionExpression("array_append", args)


def array_prepend(
        self, element: Any, array: Union[Expression, str]
) -> FunctionExpression:
    """ARRAY_PREPEND function."""
    args = [literal(element), col(array) if isinstance(array, str) else array]
    return FunctionExpression("array_prepend", args)


def array_cat(
        self, array1: Union[Expression, str], array2: Union[Expression, str]
) -> FunctionExpression:
    """ARRAY_CAT function."""
    args = [
        col(array1) if isinstance(array1, str) else array1,
        col(array2) if isinstance(array2, str) else array2,
    ]
    return FunctionExpression("array_cat", args)


def unnest(self, array: Union[Expression, str]) -> FunctionExpression:
    """UNNEST function."""
    args = [col(array) if isinstance(array, str) else array]
    return FunctionExpression("unnest", args)


# Conditional Functions
def case(self) -> "CaseBuilder":
    """Start building a CASE expression."""
    return CaseBuilder()


def greatest(self, *values: Union[Expression, str, Any]) -> FunctionExpression:
    """GREATEST function."""
    args = []
    for value in values:
        if isinstance(value, str):
            args.append(col(value))
        elif isinstance(value, Expression):
            args.append(value)
        else:
            args.append(literal(value))
    return FunctionExpression("greatest", args)


def least(self, *values: Union[Expression, str, Any]) -> FunctionExpression:
    """LEAST function."""
    args = []
    for value in values:
        if isinstance(value, str):
            args.append(col(value))
        elif isinstance(value, Expression):
            args.append(value)
        else:
            args.append(literal(value))
    return FunctionExpression("least", args)


def nullif(
        self, value1: Union[Expression, str, Any], value2: Union[Expression, str, Any]
) -> FunctionExpression:
    """NULLIF function."""
    args = []
    for value in [value1, value2]:
        if isinstance(value, str):
            args.append(col(value))
        elif isinstance(value, Expression):
            args.append(value)
        else:
            args.append(literal(value))
    return FunctionExpression("nullif", args)


class CaseBuilder:
    """Builder for CASE expressions."""

    def __init__(self):
        self.when_clauses = []
        self.else_clause = None

    def when(
            self, condition: Expression, result: Union[Expression, str, Any]
    ) -> "CaseBuilder":
        """Add a WHEN clause."""
        result_expr = (
            col(result)
            if isinstance(result, str)
            else literal(result)
            if not isinstance(result, Expression)
            else result
        )
        self.when_clauses.append((condition, result_expr))
        return self

    def else_(self, result: Union[Expression, str, Any]) -> "CaseBuilder":
        """Add an ELSE clause."""
        self.else_clause = (
            col(result)
            if isinstance(result, str)
            else literal(result)
            if not isinstance(result, Expression)
            else result
        )
        return self

    def end(self) -> Expression:
        """Build the CASE expression."""
        when_exprs = []
        for condition, result in self.when_clauses:
            when_exprs.append(ast.CaseWhen(expr=condition.node, result=result.node))

        node = ast.CaseExpr(
            args=when_exprs,
            defresult=self.else_clause.node if self.else_clause else None,
        )

        return Expression(node)


class PostgreSQLFunctions:
    """Registry of PostgreSQL built-in functions organized by category."""

    def __init__(self, introspection: Optional[Introspection] = None):
        self.introspection = introspection
        self._user_functions = {}
        if introspection:
            self._load_user_functions()

    def _load_user_functions(self):
        """Load user-defined functions from introspection data."""
        for proc in self.introspection.procs:
            namespace = self.introspection.get_namespace(proc.pronamespace)
            if namespace:
                schema_name = namespace.nspname
                func_name = proc.proname
                key = f"{schema_name}.{func_name}"
                self._user_functions[key] = proc

    def get_user_function(self, name: str, schema: str = "public") -> Optional[Any]:
        """Get a user-defined function by name and schema."""
        key = f"{schema}.{name}"
        return self._user_functions.get(key)

    def list_user_functions(self, schema: Optional[str] = None) -> List[str]:
        """List all user-defined functions, optionally filtered by schema."""
        if schema:
            return [
                key
                for key in self._user_functions.keys()
                if key.startswith(f"{schema}.")
            ]
        return list(self._user_functions.keys())
