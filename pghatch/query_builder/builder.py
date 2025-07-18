"""
Main QueryBuilder class for building PostgreSQL queries using pglast AST.

This module provides the primary interface for constructing type-safe,
parameterized PostgreSQL queries.
"""

from typing import Any, Dict, List, Optional, Union, Tuple

import asyncpg
from pglast import ast
from pglast.enums import JoinType as PgJoinType, SortByDir, LimitOption
from pglast.stream import RawStream

from .expressions import (
    Expression,
    FunctionExpression,
    ResTargetExpression,
    Parameter,
    ColumnExpression,
)
from .types import QueryResult, TableReference, JoinType, OrderDirection


def select(
    *columns: Union[
        str, Expression, FunctionExpression, Parameter, ResTargetExpression
    ],
) -> "Query":
    """
    Create a new Query instance with a SELECT clause.

    Args:
        *columns: Column names (strings), expressions, or function calls

    Returns:
        Query: New Query instance with SELECT clause
    """
    query = Query()
    return query.select(*columns)


def select_all() -> "Query":
    """
    Create a new Query instance with SELECT *.

    Returns:
        Query: New Query instance with SELECT *
    """
    query = Query()
    return query.select_all()


def insert(table: str, schema: Optional[str] = None) -> "InsertQuery":
    """
    Create a new InsertQuery instance.

    Args:
        table: Table name to insert into
        schema: Optional schema name

    Returns:
        InsertQuery: New InsertQuery instance
    """
    return InsertQuery(table, schema)


def update(
    table: str, schema: Optional[str] = None, alias: Optional[str] = None
) -> "UpdateQuery":
    """
    Create a new UpdateQuery instance.

    Args:
        table: Table name to update
        schema: Optional schema name
        alias: Optional table alias

    Returns:
        UpdateQuery: New UpdateQuery instance
    """
    return UpdateQuery(table, schema, alias)


def delete(
    table: str, schema: Optional[str] = None, alias: Optional[str] = None
) -> "DeleteQuery":
    """
    Create a new DeleteQuery instance.

    Args:
        table: Table name to delete from
        schema: Optional schema name
        alias: Optional table alias

    Returns:
        DeleteQuery: New DeleteQuery instance
    """
    return DeleteQuery(table, schema, alias)


class Query:
    """
    Main query builder class for constructing PostgreSQL queries.

    Provides a fluent interface for building SELECT, INSERT, UPDATE, and DELETE
    queries using the PostgreSQL AST via pglast.
    """

    def __init__(self):
        # Query components
        self._select_list: List[Union[str, Expression, ResTargetExpression]] = []
        self._from_clause: Optional[TableReference] = None
        self._joins: List[Tuple[str, TableReference, Optional[Expression]]] = []
        self._where_clause: Optional[Expression] = None
        self._group_by: List[Union[str, Expression]] = []
        self._having_clause: Optional[Expression] = None
        self._order_by: List[Tuple[Union[str, Expression], str]] = []
        self._limit_count: Optional[int] = None
        self._offset_count: Optional[int] = None
        self._distinct: Optional[List[str | Expression]] = None

        # For CTEs (Common Table Expressions)
        self._ctes: List[Tuple[str, "Query"]] = []

        # Parameters for prepared statements
        self._parameters: List[Any] = []
        self._parameter_counter = 0

    def select(
        self,
        *columns: Union[
            str, Expression, FunctionExpression, Parameter, ResTargetExpression
        ],
    ) -> "Query":
        """
        Add columns to the SELECT clause.

        Args:
            *columns: Column names (strings), expressions, or function calls

        Returns:
            Query: Self for method chaining
        """
        for column in columns:
            if isinstance(column, str):
                self._select_list.append(
                    ResTargetExpression(ColumnExpression(column).node)
                )

            elif isinstance(column, Parameter):
                # Parameter object
                self._add_parameter(column.value)
                self._select_list.append(
                    ResTargetExpression(ast.ParamRef(number=self._parameter_counter))
                )
            elif isinstance(column, (Expression, FunctionExpression)):
                # Expression or function
                self._select_list.append(column)
            elif hasattr(column, "node"):
                # ResTargetExpression or other node-based objects
                self._select_list.append(column)
            else:
                # Convert to string as fallback
                self._select_list.append(str(column))

        return self

    def select_all(self) -> "Query":
        """Add SELECT * to the query."""
        self._select_list = [ast.A_Star()]
        return self

    def distinct(self, columns: list[str | Expression] = None) -> "Query":
        """Enable or disable DISTINCT."""

        if columns is None or len(columns) == 0:
            self._distinct = (None,)
        else:
            distinct_cols = list()
            for col in columns:
                if isinstance(col, str):
                    if "." in col:
                        # Handle qualified names like "schema.table.column"
                        parts = col.split(".")
                        if len(parts) == 3:
                            schema, table, col_name = parts
                            distinct_cols.append(
                                ast.ColumnRef(
                                    fields=[
                                        ast.String(sval=schema),
                                        ast.String(sval=table),
                                        ast.String(sval=col_name),
                                    ]
                                )
                            )
                        elif len(parts) == 2:
                            table, col_name = parts
                            distinct_cols.append(
                                ast.ColumnRef(
                                    fields=[
                                        ast.String(sval=table),
                                        ast.String(sval=col_name),
                                    ]
                                )
                            )
                        else:
                            raise ValueError(
                                "Invalid column format. Expected 'schema.table.column' or 'table.column'."
                            )
                    else:
                        distinct_cols.append(
                            ast.ColumnRef(fields=[ast.String(sval=col)])
                        )
                elif isinstance(col, Expression):
                    distinct_cols.append(col.node)

            self._distinct = tuple(distinct_cols)
        return self

    def from_(
        self, table: str, schema: Optional[str] = None, alias: Optional[str] = None
    ) -> "Query":
        """
        Set the FROM clause.

        Args:
            table: Table name
            schema: Optional schema name
            alias: Optional table alias

        Returns:
            Query: Self for method chaining
        """
        self._from_clause = TableReference(table, schema, alias)
        return self

    def join(
        self,
        table: str,
        on: Optional[Expression] = None,
        join_type: str = JoinType.INNER,
        schema: Optional[str] = None,
        alias: Optional[str] = None,
    ) -> "Query":
        """
        Add a JOIN clause.

        Args:
            table: Table to join
            on: Join condition expression
            join_type: Type of join (INNER, LEFT, RIGHT, FULL, CROSS)
            schema: Optional schema name
            alias: Optional table alias

        Returns:
            Query: Self for method chaining
        """
        table_ref = TableReference(table, schema, alias)
        self._joins.append((join_type, table_ref, on))
        return self

    def left_join(
        self,
        table: str,
        on: Optional[Expression] = None,
        schema: Optional[str] = None,
        alias: Optional[str] = None,
    ) -> "Query":
        """Add a LEFT JOIN clause."""
        return self.join(table, on, JoinType.LEFT, schema, alias)

    def right_join(
        self,
        table: str,
        on: Optional[Expression] = None,
        schema: Optional[str] = None,
        alias: Optional[str] = None,
    ) -> "Query":
        """Add a RIGHT JOIN clause."""
        return self.join(table, on, JoinType.RIGHT, schema, alias)

    def inner_join(
        self,
        table: str,
        on: Optional[Expression] = None,
        schema: Optional[str] = None,
        alias: Optional[str] = None,
    ) -> "Query":
        """Add an INNER JOIN clause."""
        return self.join(table, on, JoinType.INNER, schema, alias)

    def full_join(
        self,
        table: str,
        on: Optional[Expression] = None,
        schema: Optional[str] = None,
        alias: Optional[str] = None,
    ) -> "Query":
        """Add a FULL JOIN clause."""
        return self.join(table, on, JoinType.FULL, schema, alias)

    def cross_join(
        self, table: str, schema: Optional[str] = None, alias: Optional[str] = None
    ) -> "Query":
        """Add a CROSS JOIN clause."""
        return self.join(table, None, JoinType.CROSS, schema, alias)

    def where(self, condition: Expression) -> "Query":
        """
        Add a WHERE clause condition.

        Args:
            condition: Boolean expression for filtering

        Returns:
            Query: Self for method chaining
        """
        if self._where_clause is None:
            self._where_clause = condition
        else:
            # Combine with existing condition using AND
            from .expressions import and_

            self._where_clause = and_(self._where_clause, condition)

        return self

    def group_by(self, *columns: Union[str, Expression]) -> "Query":
        """
        Add columns to the GROUP BY clause.

        Args:
            *columns: Column names or expressions to group by

        Returns:
            Query: Self for method chaining
        """
        self._group_by.extend(columns)
        return self

    def having(self, condition: Expression) -> "Query":
        """
        Add a HAVING clause condition.

        Args:
            condition: Boolean expression for filtering groups

        Returns:
            Query: Self for method chaining
        """
        if self._having_clause is None:
            self._having_clause = condition
        else:
            # Combine with existing condition using AND
            from .expressions import and_

            self._having_clause = and_(self._having_clause, condition)

        return self

    def order_by(
        self, column: Union[str, Expression], direction: str = OrderDirection.ASC
    ) -> "Query":
        """
        Add a column to the ORDER BY clause.

        Args:
            column: Column name or expression to order by
            direction: Sort direction (ASC or DESC)

        Returns:
            Query: Self for method chaining
        """
        self._order_by.append((column, direction))
        return self

    def limit(self, count: int) -> "Query":
        """
        Set the LIMIT clause.

        Args:
            count: Maximum number of rows to return

        Returns:
            Query: Self for method chaining
        """
        self._limit_count = count
        return self

    def offset(self, count: int) -> "Query":
        """
        Set the OFFSET clause.

        Args:
            count: Number of rows to skip

        Returns:
            Query: Self for method chaining
        """
        self._offset_count = count
        return self

    def with_(self, name: str, query: "Query") -> "Query":
        """
        Add a Common Table Expression (CTE).

        Args:
            name: Name of the CTE
            query: QueryBuilder instance for the CTE query

        Returns:
            Query: Self for method chaining
        """
        self._ctes.append((name, query))
        return self

    def query_ast(self) -> ast.SelectStmt:
        """
        Build the SQL query and return it with parameters.

        Returns:
            Tuple[str, List[Any]]: SQL string and parameter list
        """
        # Reset parameters for this build
        self._parameters = []
        self._parameter_counter = 0

        # Build the AST
        select_stmt = self._build_select_stmt()

        # Handle CTEs
        if self._ctes:
            cte_list = []
            for cte_name, cte_query in self._ctes:
                # Build CTE query to collect parameters
                cte_sql, cte_params = cte_query.build()
                self._parameters.extend(cte_params)

                # Use the AST from the CTE query, not the SQL string
                cte_list.append(
                    ast.CommonTableExpr(
                        ctename=cte_name, ctequery=cte_query._build_select_stmt()
                    )
                )

            # Wrap in WITH clause - manually copy all attributes
            select_stmt = ast.SelectStmt(
                withClause=ast.WithClause(ctes=cte_list),
                distinctClause=select_stmt.distinctClause,
                targetList=select_stmt.targetList,
                fromClause=select_stmt.fromClause,
                whereClause=select_stmt.whereClause,
                groupClause=select_stmt.groupClause,
                havingClause=select_stmt.havingClause,
                sortClause=select_stmt.sortClause,
                limitCount=select_stmt.limitCount,
                limitOffset=select_stmt.limitOffset,
                limitOption=select_stmt.limitOption,
            )
        return select_stmt

    def build(self) -> Tuple[str, List[Any]]:
        select_stmt = self.query_ast()

        # Generate SQL
        sql = RawStream()(select_stmt)

        return sql, self._parameters

    def _build_select_stmt(self) -> ast.SelectStmt:
        """Build the main SELECT statement AST."""
        # Build target list (SELECT clause)
        target_list = self._build_target_list()

        # Build FROM clause
        from_clause = self._build_from_clause()

        # Build WHERE clause
        where_clause = self._where_clause.node if self._where_clause else None

        # Build GROUP BY clause
        group_clause = self._build_group_by()

        # Build HAVING clause
        having_clause = self._having_clause.node if self._having_clause else None

        # Build ORDER BY clause
        sort_clause = self._build_order_by()

        # Build LIMIT/OFFSET
        limit_count = None
        limit_offset = None
        limit_option = None

        if self._limit_count is not None:
            limit_count = ast.A_Const(val=ast.Integer(ival=self._limit_count))
            limit_option = LimitOption.LIMIT_OPTION_COUNT

        if self._offset_count is not None:
            limit_offset = ast.A_Const(val=ast.Integer(ival=self._offset_count))

        return ast.SelectStmt(
            distinctClause=self._distinct,
            targetList=target_list,
            fromClause=from_clause,
            whereClause=where_clause,
            groupClause=group_clause,
            havingClause=having_clause,
            sortClause=sort_clause,
            limitCount=limit_count,
            limitOffset=limit_offset,
            limitOption=limit_option,
        )

    def _build_target_list(self) -> List[ast.ResTarget]:
        """Build the SELECT target list."""
        if not self._select_list:
            # Default to SELECT *
            return [ast.ResTarget(val=ast.A_Star())]

        targets = []
        for item in self._select_list:
            if isinstance(item, str):
                # Simple column name
                targets.append(
                    ast.ResTarget(val=ast.ColumnRef(fields=[ast.String(sval=item)]))
                )
            elif isinstance(item, ast.A_Star):
                # SELECT *
                targets.append(ast.ResTarget(val=item))
            elif isinstance(item, Expression):
                # Expression
                targets.append(ast.ResTarget(val=item.node))
            elif hasattr(item, "node") and hasattr(item.node, "val"):
                # ResTargetExpression
                targets.append(item.node)
            else:
                # Fallback - treat as string
                targets.append(
                    ast.ResTarget(
                        val=ast.ColumnRef(fields=[ast.String(sval=str(item))])
                    )
                )

        return targets

    def _build_from_clause(self) -> Optional[List[ast.RangeVar]]:
        """Build the FROM clause."""
        if not self._from_clause:
            return None

        from_items = []

        # Main table
        range_var = ast.RangeVar(
            relname=self._from_clause.name,
            schemaname=self._from_clause.schema,
            alias=ast.Alias(aliasname=self._from_clause.alias)
            if self._from_clause.alias
            else None,
            inh=True,  # Include inheritance (removes ONLY keyword)
        )
        from_items.append(range_var)

        # Add joins
        for join_type, table_ref, condition in self._joins:
            join_node = ast.JoinExpr(
                jointype=self._map_join_type(join_type),
                larg=from_items[-1] if len(from_items) == 1 else ast.JoinExpr(),
                rarg=ast.RangeVar(
                    relname=table_ref.name,
                    schemaname=table_ref.schema,
                    alias=ast.Alias(aliasname=table_ref.alias)
                    if table_ref.alias
                    else None,
                    inh=True,  # Include inheritance (removes ONLY keyword)
                ),
                quals=condition.node if condition else None,
            )
            from_items = [join_node]

        return from_items

    def _build_group_by(self) -> Optional[List[ast.Node]]:
        """Build the GROUP BY clause."""
        if not self._group_by:
            return None

        group_items = []
        for item in self._group_by:
            if isinstance(item, str):
                group_items.append(ast.ColumnRef(fields=[ast.String(sval=item)]))
            elif isinstance(item, Expression):
                group_items.append(item.node)
            else:
                group_items.append(ast.ColumnRef(fields=[ast.String(sval=str(item))]))

        return group_items

    def _build_order_by(self) -> Optional[List[ast.SortBy]]:
        """Build the ORDER BY clause."""
        if not self._order_by:
            return None

        sort_items = []
        for column, direction in self._order_by:
            if isinstance(column, str):
                node = ast.ColumnRef(fields=[ast.String(sval=column)])
            elif isinstance(column, Expression):
                node = column.node
            else:
                node = ast.ColumnRef(fields=[ast.String(sval=str(column))])

            sort_dir = (
                SortByDir.SORTBY_ASC
                if direction == OrderDirection.ASC
                else SortByDir.SORTBY_DESC
            )

            sort_items.append(ast.SortBy(node=node, sortby_dir=sort_dir))

        return sort_items

    def _map_join_type(self, join_type: str) -> PgJoinType:
        """Map our join type strings to pglast join types."""
        mapping = {
            JoinType.INNER: PgJoinType.JOIN_INNER,
            JoinType.LEFT: PgJoinType.JOIN_LEFT,
            JoinType.RIGHT: PgJoinType.JOIN_RIGHT,
            JoinType.FULL: PgJoinType.JOIN_FULL,
            JoinType.CROSS: PgJoinType.JOIN_INNER,  # CROSS JOIN is handled differently
        }
        return mapping.get(join_type, PgJoinType.JOIN_INNER)

    def _add_parameter(self, value: Any) -> str:
        """Add a parameter and return its placeholder."""
        self._parameter_counter += 1
        self._parameters.append(value)
        return f"${self._parameter_counter}"

    async def execute(
        self,
        pool: asyncpg.Pool,
        connection: Optional[asyncpg.Connection] = None,
        model_class: Optional[type] = None,
    ) -> QueryResult:
        """
        Execute the query and return results.

        Args:
            pool: AsyncPG connection pool
            connection: Optional existing connection to use
            model_class: Optional Pydantic model class for result conversion

        Returns:
            QueryResult: Query results with metadata
        """
        sql, parameters = self.build()

        if connection:
            rows = await connection.fetch(sql, *parameters)
        else:
            async with pool.acquire() as conn:
                rows = await conn.fetch(sql, *parameters)

        # Convert asyncpg.Record objects to dictionaries
        dict_rows = [dict(row) for row in rows]

        return QueryResult(
            rows=dict_rows,
            sql=sql,
            parameters=parameters,
            row_count=len(dict_rows),
            model_class=model_class,
        )

    async def execute_one(
        self,
        pool: asyncpg.Pool,
        connection: Optional[asyncpg.Connection] = None,
        model_class: Optional[type] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Execute the query and return the first result.

        Args:
            pool: AsyncPG connection pool
            connection: Optional existing connection to use
            model_class: Optional Pydantic model class for result conversion

        Returns:
            Optional[Dict[str, Any]]: First row or None
        """
        result = await self.execute(pool, connection, model_class)
        return result.first()

    def __str__(self) -> str:
        """Return the SQL string representation."""
        sql, _ = self.build()
        return sql

    def __repr__(self) -> str:
        """Return a detailed string representation."""
        sql, params = self.build()
        return f"QueryBuilder(sql='{sql}', params={params})"


class InsertQuery:
    """
    Query builder for INSERT statements.

    Provides a fluent interface for building PostgreSQL INSERT queries
    using the PostgreSQL AST via pglast.
    """

    def __init__(self, table: str, schema: Optional[str] = None):
        self.table = TableReference(table, schema)
        self._columns: List[str] = []
        self._values: List[List[Any]] = []
        self._returning: List[Union[str, Expression]] = []
        self._on_conflict: Optional[Dict[str, Any]] = None
        self._parameters: List[Any] = []
        self._parameter_counter = 0

    def columns(self, *cols: str) -> "InsertQuery":
        """
        Specify the columns for the INSERT.

        Args:
            *cols: Column names

        Returns:
            InsertQuery: Self for method chaining
        """
        self._columns.extend(cols)
        return self

    def values(self, *vals: Any) -> "InsertQuery":
        """
        Add a row of values for the INSERT.

        Args:
            *vals: Values for each column

        Returns:
            InsertQuery: Self for method chaining
        """
        if len(vals) != len(self._columns):
            raise ValueError(
                f"Number of values ({len(vals)}) must match number of columns ({len(self._columns)})"
            )

        # Store values directly - parameter conversion happens in build()
        self._values.append(list(vals))
        return self

    def values_dict(self, **kwargs: Any) -> "InsertQuery":
        """
        Add values using a dictionary mapping column names to values.

        Args:
            **kwargs: Column name to value mappings

        Returns:
            InsertQuery: Self for method chaining
        """
        if not self._columns:
            self._columns = list(kwargs.keys())

        # Ensure all columns are present
        vals = []
        for col in self._columns:
            if col not in kwargs:
                raise ValueError(f"Missing value for column '{col}'")
            vals.append(kwargs[col])

        return self.values(*vals)

    def returning(self, *cols: Union[str, Expression]) -> "InsertQuery":
        """
        Add RETURNING clause.

        Args:
            *cols: Columns or expressions to return

        Returns:
            InsertQuery: Self for method chaining
        """
        self._returning.extend(cols)
        return self

    def on_conflict_do_nothing(self, *conflict_columns: str) -> "InsertQuery":
        """
        Add ON CONFLICT DO NOTHING clause.

        Args:
            *conflict_columns: Columns that might conflict

        Returns:
            InsertQuery: Self for method chaining
        """
        self._on_conflict = {"action": "nothing", "columns": list(conflict_columns)}
        return self

    def on_conflict_do_update(
        self, conflict_columns: List[str], **update_values: Any
    ) -> "InsertQuery":
        """
        Add ON CONFLICT DO UPDATE clause.

        Args:
            conflict_columns: Columns that might conflict
            **update_values: Column updates for conflicts

        Returns:
            InsertQuery: Self for method chaining
        """
        self._on_conflict = {
            "action": "update",
            "columns": conflict_columns,
            "updates": update_values,
        }
        return self

    def _add_parameter(self, value: Any) -> str:
        """Add a parameter and return its placeholder."""
        self._parameter_counter += 1
        self._parameters.append(value)
        return f"${self._parameter_counter}"

    def build(self) -> Tuple[str, List[Any]]:
        """
        Build the INSERT query.

        Returns:
            Tuple[str, List[Any]]: SQL string and parameter list
        """
        # Reset parameters for this build
        self._parameters = []
        self._parameter_counter = 0

        # Build the AST
        insert_stmt = self._build_insert_stmt()

        # Generate SQL
        sql = RawStream()(insert_stmt)

        return sql, self._parameters

    def _build_insert_stmt(self) -> ast.InsertStmt:
        """Build the INSERT statement AST."""
        # Build target relation
        relation = ast.RangeVar(
            relname=self.table.name, schemaname=self.table.schema, inh=True
        )

        # Build columns
        cols = None
        if self._columns:
            cols = [ast.ResTarget(name=col) for col in self._columns]

        # Build values
        values_lists = []
        for value_row in self._values:
            value_nodes = []
            for val in value_row:
                self._parameter_counter += 1
                self._parameters.append(val)
                value_nodes.append(ast.ParamRef(number=self._parameter_counter))
            values_lists.append(value_nodes)

        # Create VALUES clause
        select_stmt = ast.SelectStmt(valuesLists=values_lists)

        # Build RETURNING clause
        returning_list = None
        if self._returning:
            returning_list = []
            for ret in self._returning:
                if isinstance(ret, str):
                    returning_list.append(
                        ast.ResTarget(val=ast.ColumnRef(fields=[ast.String(sval=ret)]))
                    )
                elif isinstance(ret, Expression):
                    returning_list.append(ast.ResTarget(val=ret.node))

        return ast.InsertStmt(
            relation=relation,
            cols=cols,
            selectStmt=select_stmt,
            returningList=returning_list,
        )

    async def execute(
        self,
        pool: asyncpg.Pool,
        connection: Optional[asyncpg.Connection] = None,
        model_class: Optional[type] = None,
    ) -> QueryResult:
        """
        Execute the INSERT query.

        Args:
            pool: AsyncPG connection pool
            connection: Optional existing connection to use
            model_class: Optional Pydantic model class for result conversion

        Returns:
            QueryResult: Query results with metadata
        """
        sql, parameters = self.build()

        if connection:
            if self._returning:
                rows = await connection.fetch(sql, *parameters)
            else:
                await connection.execute(sql, *parameters)
                rows = []
        else:
            async with pool.acquire() as conn:
                if self._returning:
                    rows = await conn.fetch(sql, *parameters)
                else:
                    await conn.execute(sql, *parameters)
                    rows = []

        # Convert asyncpg.Record objects to dictionaries
        dict_rows = [dict(row) for row in rows]

        return QueryResult(
            rows=dict_rows,
            sql=sql,
            parameters=parameters,
            row_count=len(dict_rows),
            model_class=model_class,
        )

    def __str__(self) -> str:
        """Return the SQL string representation."""
        sql, _ = self.build()
        return sql

    def __repr__(self) -> str:
        """Return a detailed string representation."""
        sql, params = self.build()
        return f"InsertQuery(sql='{sql}', params={params})"


class UpdateQuery:
    """
    Query builder for UPDATE statements.

    Provides a fluent interface for building PostgreSQL UPDATE queries
    using the PostgreSQL AST via pglast.
    """

    def __init__(
        self, table: str, schema: Optional[str] = None, alias: Optional[str] = None
    ):
        self.table = TableReference(table, schema, alias)
        self._set_clauses: List[Tuple[str, Any]] = []
        self._joins: List[Tuple[str, TableReference, Optional[Expression]]] = []
        self._where_clause: Optional[Expression] = None
        self._returning: List[Union[str, Expression]] = []
        self._parameters: List[Any] = []
        self._parameter_counter = 0

    def set(self, column: str, value: Any) -> "UpdateQuery":
        """
        Add a SET clause.

        Args:
            column: Column name to update
            value: New value for the column

        Returns:
            UpdateQuery: Self for method chaining
        """
        self._set_clauses.append((column, value))
        return self

    def set_dict(self, **kwargs: Any) -> "UpdateQuery":
        """
        Add multiple SET clauses using a dictionary.

        Args:
            **kwargs: Column name to value mappings

        Returns:
            UpdateQuery: Self for method chaining
        """
        for column, value in kwargs.items():
            self.set(column, value)
        return self

    def join(
        self,
        table: str,
        on: Optional[Expression] = None,
        join_type: str = JoinType.INNER,
        schema: Optional[str] = None,
        alias: Optional[str] = None,
    ) -> "UpdateQuery":
        """
        Add a JOIN clause to the UPDATE.

        Args:
            table: Table to join
            on: Join condition expression
            join_type: Type of join (INNER, LEFT, RIGHT, FULL)
            schema: Optional schema name
            alias: Optional table alias

        Returns:
            UpdateQuery: Self for method chaining
        """
        table_ref = TableReference(table, schema, alias)
        self._joins.append((join_type, table_ref, on))
        return self

    def where(self, condition: Expression) -> "UpdateQuery":
        """
        Add a WHERE clause condition.

        Args:
            condition: Boolean expression for filtering

        Returns:
            UpdateQuery: Self for method chaining
        """
        if self._where_clause is None:
            self._where_clause = condition
        else:
            # Combine with existing condition using AND
            from .expressions import and_

            self._where_clause = and_(self._where_clause, condition)

        return self

    def returning(self, *cols: Union[str, Expression]) -> "UpdateQuery":
        """
        Add RETURNING clause.

        Args:
            *cols: Columns or expressions to return

        Returns:
            UpdateQuery: Self for method chaining
        """
        self._returning.extend(cols)
        return self

    def _add_parameter(self, value: Any) -> str:
        """Add a parameter and return its placeholder."""
        self._parameter_counter += 1
        self._parameters.append(value)
        return f"${self._parameter_counter}"

    def build(self) -> Tuple[str, List[Any]]:
        """
        Build the UPDATE query.

        Returns:
            Tuple[str, List[Any]]: SQL string and parameter list
        """
        # Reset parameters for this build
        self._parameters = []
        self._parameter_counter = 0

        # Build the AST
        update_stmt = self._build_update_stmt()

        # Generate SQL
        sql = RawStream()(update_stmt)

        return sql, self._parameters

    def _build_update_stmt(self) -> ast.UpdateStmt:
        """Build the UPDATE statement AST."""
        # Build target relation
        relation = ast.RangeVar(
            relname=self.table.name,
            schemaname=self.table.schema,
            alias=ast.Alias(aliasname=self.table.alias) if self.table.alias else None,
            inh=True,
        )

        # Build SET clauses
        target_list = []
        for column, value in self._set_clauses:
            self._parameter_counter += 1
            self._parameters.append(value)

            target_list.append(
                ast.ResTarget(
                    name=column, val=ast.ParamRef(number=self._parameter_counter)
                )
            )

        # Build FROM clause (for joins)
        from_clause = None
        if self._joins:
            from_items = []
            for join_type, table_ref, condition in self._joins:
                range_var = ast.RangeVar(
                    relname=table_ref.name,
                    schemaname=table_ref.schema,
                    alias=ast.Alias(aliasname=table_ref.alias)
                    if table_ref.alias
                    else None,
                    inh=True,
                )
                from_items.append(range_var)
            from_clause = from_items

        # Build WHERE clause
        where_clause = self._where_clause.node if self._where_clause else None

        # Build RETURNING clause
        returning_list = None
        if self._returning:
            returning_list = []
            for ret in self._returning:
                if isinstance(ret, str):
                    returning_list.append(
                        ast.ResTarget(val=ast.ColumnRef(fields=[ast.String(sval=ret)]))
                    )
                elif isinstance(ret, Expression):
                    returning_list.append(ast.ResTarget(val=ret.node))

        return ast.UpdateStmt(
            relation=relation,
            targetList=target_list,
            fromClause=from_clause,
            whereClause=where_clause,
            returningList=returning_list,
        )

    async def execute(
        self,
        pool: asyncpg.Pool,
        connection: Optional[asyncpg.Connection] = None,
        model_class: Optional[type] = None,
    ) -> QueryResult:
        """
        Execute the UPDATE query.

        Args:
            pool: AsyncPG connection pool
            connection: Optional existing connection to use
            model_class: Optional Pydantic model class for result conversion

        Returns:
            QueryResult: Query results with metadata
        """
        sql, parameters = self.build()

        if connection:
            if self._returning:
                rows = await connection.fetch(sql, *parameters)
            else:
                result = await connection.execute(sql, *parameters)
                rows = []
        else:
            async with pool.acquire() as conn:
                if self._returning:
                    rows = await conn.fetch(sql, *parameters)
                else:
                    result = await conn.execute(sql, *parameters)
                    rows = []

        # Convert asyncpg.Record objects to dictionaries
        dict_rows = [dict(row) for row in rows]

        return QueryResult(
            rows=dict_rows,
            sql=sql,
            parameters=parameters,
            row_count=len(dict_rows),
            model_class=model_class,
        )

    def __str__(self) -> str:
        """Return the SQL string representation."""
        sql, _ = self.build()
        return sql

    def __repr__(self) -> str:
        """Return a detailed string representation."""
        sql, params = self.build()
        return f"UpdateQuery(sql='{sql}', params={params})"


class DeleteQuery:
    """
    Query builder for DELETE statements.

    Provides a fluent interface for building PostgreSQL DELETE queries
    using the PostgreSQL AST via pglast.
    """

    def __init__(
        self, table: str, schema: Optional[str] = None, alias: Optional[str] = None
    ):
        self.table = TableReference(table, schema, alias)
        self._using: List[TableReference] = []
        self._where_clause: Optional[Expression] = None
        self._returning: List[Union[str, Expression]] = []
        self._parameters: List[Any] = []
        self._parameter_counter = 0

    def using(
        self, table: str, schema: Optional[str] = None, alias: Optional[str] = None
    ) -> "DeleteQuery":
        """
        Add a USING clause (PostgreSQL-specific).

        Args:
            table: Table name for USING clause
            schema: Optional schema name
            alias: Optional table alias

        Returns:
            DeleteQuery: Self for method chaining
        """
        table_ref = TableReference(table, schema, alias)
        self._using.append(table_ref)
        return self

    def where(self, condition: Expression) -> "DeleteQuery":
        """
        Add a WHERE clause condition.

        Args:
            condition: Boolean expression for filtering

        Returns:
            DeleteQuery: Self for method chaining
        """
        if self._where_clause is None:
            self._where_clause = condition
        else:
            # Combine with existing condition using AND
            from .expressions import and_

            self._where_clause = and_(self._where_clause, condition)

        return self

    def returning(self, *cols: Union[str, Expression]) -> "DeleteQuery":
        """
        Add RETURNING clause.

        Args:
            *cols: Columns or expressions to return

        Returns:
            DeleteQuery: Self for method chaining
        """
        self._returning.extend(cols)
        return self

    def _add_parameter(self, value: Any) -> str:
        """Add a parameter and return its placeholder."""
        self._parameter_counter += 1
        self._parameters.append(value)
        return f"${self._parameter_counter}"

    def build(self) -> Tuple[str, List[Any]]:
        """
        Build the DELETE query.

        Returns:
            Tuple[str, List[Any]]: SQL string and parameter list
        """
        # Reset parameters for this build
        self._parameters = []
        self._parameter_counter = 0

        # Build the AST
        delete_stmt = self._build_delete_stmt()

        # Generate SQL
        sql = RawStream()(delete_stmt)

        return sql, self._parameters

    def _build_delete_stmt(self) -> ast.DeleteStmt:
        """Build the DELETE statement AST."""
        # Build target relation
        relation = ast.RangeVar(
            relname=self.table.name,
            schemaname=self.table.schema,
            alias=ast.Alias(aliasname=self.table.alias) if self.table.alias else None,
            inh=True,
        )

        # Build USING clause
        using_clause = None
        if self._using:
            using_clause = []
            for table_ref in self._using:
                using_clause.append(
                    ast.RangeVar(
                        relname=table_ref.name,
                        schemaname=table_ref.schema,
                        alias=ast.Alias(aliasname=table_ref.alias)
                        if table_ref.alias
                        else None,
                        inh=True,
                    )
                )

        # Build WHERE clause
        where_clause = self._where_clause.node if self._where_clause else None

        # Build RETURNING clause
        returning_list = None
        if self._returning:
            returning_list = []
            for ret in self._returning:
                if isinstance(ret, str):
                    returning_list.append(
                        ast.ResTarget(val=ast.ColumnRef(fields=[ast.String(sval=ret)]))
                    )
                elif isinstance(ret, Expression):
                    returning_list.append(ast.ResTarget(val=ret.node))

        return ast.DeleteStmt(
            relation=relation,
            usingClause=using_clause,
            whereClause=where_clause,
            returningList=returning_list,
        )

    async def execute(
        self,
        pool: asyncpg.Pool,
        connection: Optional[asyncpg.Connection] = None,
        model_class: Optional[type] = None,
    ) -> QueryResult:
        """
        Execute the DELETE query.

        Args:
            pool: AsyncPG connection pool
            connection: Optional existing connection to use
            model_class: Optional Pydantic model class for result conversion

        Returns:
            QueryResult: Query results with metadata
        """
        sql, parameters = self.build()

        if connection:
            if self._returning:
                rows = await connection.fetch(sql, *parameters)
            else:
                result = await connection.execute(sql, *parameters)
                rows = []
        else:
            async with pool.acquire() as conn:
                if self._returning:
                    rows = await conn.fetch(sql, *parameters)
                else:
                    result = await conn.execute(sql, *parameters)
                    rows = []

        # Convert asyncpg.Record objects to dictionaries
        dict_rows = [dict(row) for row in rows]

        return QueryResult(
            rows=dict_rows,
            sql=sql,
            parameters=parameters,
            row_count=len(dict_rows),
            model_class=model_class,
        )

    def __str__(self) -> str:
        """Return the SQL string representation."""
        sql, _ = self.build()
        return sql

    def __repr__(self) -> str:
        """Return a detailed string representation."""
        sql, params = self.build()
        return f"DeleteQuery(sql='{sql}', params={params})"
