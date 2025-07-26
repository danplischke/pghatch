"""
Type definitions for the query builder module.
"""

from typing import Any, Dict, List, Optional, Union, TypeVar, Generic, TYPE_CHECKING

from asyncpg import Connection, Pool

if TYPE_CHECKING:
    from pghatch.query.builder.expressions import Parameter


T = TypeVar("T")


class ExecutionContext:
    """Context for query execution with connection management."""

    def __init__(self, pool: Pool, connection: Optional[Connection] = None):
        self.pool = pool
        self.connection = connection
        self._owned_connection = connection is None

    async def __aenter__(self):
        if self.connection is None:
            self.connection = await self.pool.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._owned_connection and self.connection:
            await self.pool.release(self.connection)
            self.connection = None


class QueryResult(Generic[T]):
    """Result container for executed queries."""

    def __init__(
        self,
        rows: List[Dict[str, Any]],
        sql: str,
        parameters: List[Any],
        row_count: int,
        model_class: Optional[type] = None,
    ):
        self.rows = rows
        self.sql = sql
        self.parameters = parameters
        self.row_count = row_count
        self.model_class = model_class

    def to_models(self) -> List[T]:
        """Convert rows to Pydantic models if model_class is provided."""
        if not self.model_class:
            raise ValueError("No model class specified for conversion")
        return [self.model_class(**row) for row in self.rows]

    def to_dicts(self) -> List[Dict[str, Any]]:
        """Return rows as dictionaries."""
        return self.rows

    def first(self) -> Optional[Dict[str, Any]]:
        """Get the first row or None."""
        return self.rows[0] if self.rows else None

    def first_model(self) -> Optional[T]:
        """Get the first row as a model or None."""
        if not self.rows or not self.model_class:
            return None
        return self.model_class(**self.rows[0])


class ColumnReference:
    """Represents a column reference in a query."""

    def __init__(
        self,
        name: Union[str, "Parameter"],
        table_alias: Optional[str] = None,
        column_alias: Optional[str] = None,
    ):
        self.name = name
        self.table_alias = table_alias
        self.column_alias = column_alias

    @property
    def qualified_name(self) -> str:
        """Get the fully qualified column name."""
        if self.table_alias:
            return f"{self.table_alias}.{self.name}"
        return self.name

    @property
    def alias(self) -> str:
        """Get the column alias if set, otherwise return the column name."""
        return self.column_alias or self.name


class TableReference:
    """Represents a table reference in a query."""

    def __init__(
        self, name: str, schema: Optional[str] = None, alias: Optional[str] = None
    ):
        self.name = name
        self.schema = schema
        self.alias = alias

    @property
    def qualified_name(self) -> str:
        """Get the fully qualified table name."""
        if self.schema:
            return f"{self.schema}.{self.name}"
        return self.name

    @property
    def reference_name(self) -> str:
        """Get the name to use for referencing this table."""
        return self.alias or self.name


class JoinType:
    """Enumeration of SQL join types."""

    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    CROSS = "CROSS"


class OrderDirection:
    """Enumeration of SQL order directions."""

    ASC = "ASC"
    DESC = "DESC"


class AggregateFunction:
    """Represents an aggregate function call."""

    def __init__(
        self,
        name: str,
        args: List[Union[str, ColumnReference]],
        distinct: bool = False,
        filter_clause: Optional[Any] = None,
    ):
        self.name = name
        self.args = args
        self.distinct = distinct
        self.filter_clause = filter_clause
