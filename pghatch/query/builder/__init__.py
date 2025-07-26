"""
PostgreSQL Query Builder using pglast AST.

This module provides a fluent interface for building type-safe PostgreSQL queries
using the native PostgreSQL AST via pglast.
"""

from .builder import Query
from pghatch.query.builder.expressions import col, literal, param, and_, or_, not_
from pghatch.query.builder.types import QueryResult, ExecutionContext

__all__ = [
    "Query",
    "col",
    "literal",
    "param",
    "and_",
    "or_",
    "not_",
    "QueryResult",
    "ExecutionContext",
]
