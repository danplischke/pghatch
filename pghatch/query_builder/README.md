# PostgreSQL Query Builder

A comprehensive, type-safe PostgreSQL query builder using the native PostgreSQL AST via pglast. This module provides a fluent interface for building complex SQL queries with full support for PostgreSQL features including functions, procedures, and advanced data types.

## Features

- **Type-Safe**: Full integration with PostgreSQL type system via introspection
- **AST-Based**: Uses pglast for native PostgreSQL AST generation
- **Comprehensive**: Support for all major SQL constructs and PostgreSQL-specific features
- **Secure**: Parameterized queries prevent SQL injection
- **Extensible**: Easy to add custom functions and operators
- **Integration**: Seamless integration with existing pghatch introspection system

## Quick Start

```python
from pghatch.query_builder import QueryBuilder, col, func, and_, or_

# Simple query
qb = QueryBuilder()
query = (qb.select("id", "name", "email")
         .from_("users")
         .where(col("active").eq(True))
         .order_by("name")
         .limit(10))

sql, params = query.build()
print(sql)  # Generated SQL
```

## Core Components

### QueryBuilder

The main class for building queries with a fluent interface:

```python
qb = QueryBuilder(introspection=introspection)  # Optional introspection data

# Build a complex query
query = (qb
    .select("u.name", func.count("o.id").as_("order_count"))
    .from_("users", alias="u")
    .left_join("orders", on=col("u.id").eq(col("o.user_id")), alias="o")
    .where(col("u.active").eq(True))
    .group_by("u.id", "u.name")
    .having(func.count("o.id").gt(0))
    .order_by("order_count", "DESC")
    .limit(50))
```

### Expressions

Type-safe expression builders for WHERE clauses and more:

```python
from pghatch.query_builder import col, literal, and_, or_, not_

# Column expressions
col("age").gt(18)
col("name").like("%john%")
col("email").is_not_null()
col("status").in_(["active", "pending"])

# Logical combinations
and_(
    col("active").eq(True),
    or_(
        col("age").gt(18),
        col("verified").eq(True)
    )
)
```

### Functions

Comprehensive PostgreSQL function support:

```python
from pghatch.query_builder import func

# Aggregate functions
func.count("*")
func.sum("amount")
func.avg("score")

# String functions
func.upper("name")
func.concat("first_name", " ", "last_name")
func.substring("text", 1, 10)

# Date functions
func.now()
func.date_trunc("month", "created_at")
func.extract("year", "timestamp_col")

# JSON functions
func.json_extract_path_text("metadata", "title")
func.jsonb_array_length("tags")

# Window functions
func.row_number()
func.rank()
func.lag("sales", 1)

# Array functions
func.array_length("tags", 1)
func.unnest("array_col")
```

## Advanced Features

### Complex Expressions

```python
# CASE expressions
case_expr = (func.case()
    .when(col("age").lt(18), "Minor")
    .when(col("age").lt(65), "Adult")
    .else_("Senior")
    .end())

query = qb.select("name", case_expr.as_("age_group")).from_("users")
```

### Joins

```python
# Various join types
qb.inner_join("table", on=condition)
qb.left_join("table", on=condition, alias="t")
qb.right_join("table", on=condition)
qb.full_join("table", on=condition)
qb.cross_join("table")
```

### Aggregation and Grouping

```python
query = (qb
    .select("department", func.count("*").as_("count"), func.avg("salary"))
    .from_("employees")
    .group_by("department")
    .having(func.count("*").gt(5)))
```

### Window Functions

```python
query = (qb
    .select(
        "name",
        "salary",
        func.row_number().as_("row_num"),
        func.rank().as_("salary_rank")
    )
    .from_("employees"))
```

### Pagination

```python
# Offset-based pagination
query = qb.select("*").from_("users").limit(20).offset(40)

# Cursor-based pagination
query = (qb
    .select("*", func.row_number().as_("row_num"))
    .from_("posts")
    .order_by("created_at", "DESC"))
```

## Query Execution

### With AsyncPG Pool

```python
import asyncpg
from pghatch.query_builder import QueryBuilder

# Create connection pool
pool = await asyncpg.create_pool("postgresql://user:pass@localhost/db")

# Build and execute query
qb = QueryBuilder()
query = qb.select("*").from_("users").where(col("active").eq(True))

# Execute and get results
result = await query.execute(pool)
users = result.to_dicts()
first_user = result.first()

# With Pydantic models
from pydantic import BaseModel

class User(BaseModel):
    id: int
    name: str
    email: str

result = await query.execute(pool, model_class=User)
user_models = result.to_models()
```

### Result Handling

```python
# QueryResult provides multiple ways to access data
result = await query.execute(pool)

# As dictionaries
rows = result.to_dicts()

# First row only
first = result.first()

# As Pydantic models (if model_class provided)
models = result.to_models()

# Metadata
print(f"Rows returned: {result.row_count}")
print(f"SQL executed: {result.sql}")
print(f"Parameters: {result.parameters}")
```

## Integration with Introspection

The query builder integrates seamlessly with pghatch's introspection system:

```python
from pghatch.introspection.introspection import make_introspection_query

# Get introspection data
async with pool.acquire() as conn:
    introspection = await make_introspection_query(conn)

# Create query builder with introspection
qb = QueryBuilder(introspection=introspection)

# Access user-defined functions
user_functions = qb.functions.list_user_functions("public")
print(f"Available functions: {user_functions}")

# Use user-defined function
my_func = qb.functions.get_user_function("calculate_score", "public")
if my_func:
    query = qb.select("*").from_("games").where(my_func("player_id").gt(100))
```

## Type Safety

The query builder provides type safety through:

1. **Expression Type Checking**: Column expressions validate operations
2. **Function Signatures**: Built-in functions have proper type signatures
3. **Introspection Integration**: User-defined functions and types are validated
4. **Pydantic Integration**: Results can be automatically converted to typed models

## Security

- **Parameterized Queries**: All values are properly parameterized
- **SQL Injection Prevention**: AST-based generation prevents injection
- **Schema Validation**: Integration with introspection validates table/column names
- **Type Validation**: PostgreSQL types are properly handled

## Performance

- **Optimized AST Generation**: Efficient query building
- **Connection Pooling**: Full support for AsyncPG connection pools
- **Prepared Statements**: Parameterized queries for better performance
- **Query Caching**: Built queries can be cached and reused

## Examples

See `examples/query_builder_examples.py` for comprehensive usage examples including:

- Basic SELECT queries
- Complex JOINs
- Aggregate functions
- Window functions
- JSON operations
- Array functions
- Pagination patterns
- Conditional expressions

## Testing

Run the test suite:

```bash
pytest tests/unit/query_builder/
```

## API Reference

### QueryBuilder Methods

- `select(*columns)` - Add columns to SELECT clause
- `select_all()` - SELECT *
- `distinct(bool)` - Enable/disable DISTINCT
- `from_(table, schema=None, alias=None)` - Set FROM clause
- `join(table, on=None, join_type="INNER", ...)` - Add JOIN
- `left_join(table, on=None, ...)` - Add LEFT JOIN
- `right_join(table, on=None, ...)` - Add RIGHT JOIN
- `inner_join(table, on=None, ...)` - Add INNER JOIN
- `full_join(table, on=None, ...)` - Add FULL JOIN
- `cross_join(table, ...)` - Add CROSS JOIN
- `where(condition)` - Add WHERE condition
- `group_by(*columns)` - Add GROUP BY columns
- `having(condition)` - Add HAVING condition
- `order_by(column, direction="ASC")` - Add ORDER BY
- `limit(count)` - Set LIMIT
- `offset(count)` - Set OFFSET
- `build()` - Build SQL and parameters
- `execute(pool, connection=None, model_class=None)` - Execute query
- `execute_one(pool, connection=None, model_class=None)` - Execute and get first result

### Expression Functions

- `col(name, table_alias=None)` - Create column reference
- `literal(value)` - Create literal value
- `and_(*expressions)` - Combine with AND
- `or_(*expressions)` - Combine with OR
- `not_(expression)` - Negate expression

### Function Categories

- **Aggregate**: count, sum, avg, max, min
- **String**: upper, lower, concat, substring, trim, replace
- **Date/Time**: now, date_trunc, extract, age, to_char
- **JSON**: json_extract_path, jsonb_array_length, etc.
- **Window**: row_number, rank, lag, lead, first_value
- **Array**: array_length, array_append, unnest
- **Conditional**: case, coalesce, nullif, greatest, least

## Contributing

When adding new features:

1. Add comprehensive tests
2. Update documentation
3. Follow existing code patterns
4. Ensure type safety
5. Add examples for complex features

## License

This module is part of the pghatch project and follows the same license terms.
