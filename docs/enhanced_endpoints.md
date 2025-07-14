# Enhanced REST Endpoints

This document describes the enhanced REST endpoints functionality implemented in pghatch, providing comprehensive CRUD operations with a powerful JSON filter format.

## Overview

The enhanced endpoints provide a complete REST API interface for PostgreSQL tables and views with the following features:

- **Multiple HTTP Methods**: GET, POST, PUT, DELETE with specific semantics
- **JSON Filter Format**: Powerful query language for complex filtering
- **Standardized Responses**: Consistent `{results, total, pagination}` format
- **Type Safety**: Full validation with Pydantic models
- **Security**: Primary key and unique constraint validation

## HTTP Method Mapping

### GET - Retrieve Data
- **Purpose**: Basic data retrieval with simple filtering
- **Parameters**: Query parameters for basic filtering and pagination
- **Response**: StandardResponse format

```http
GET /public/users?limit=10&offset=0&select_fields=id,name,email
```

### POST - Complex Queries & Updates
- **Purpose**: Complex queries using JSON filter format OR updates by primary key
- **Body**: FilterRequest (for queries) or UpdateRequest (for updates)
- **Response**: StandardResponse (queries) or updated record (updates)

```http
POST /public/users
Content-Type: application/json

{
  "select": {...},
  "where": {...},
  "pagination": {...}
}
```

### PUT - Create Records
- **Purpose**: Create new records (single or batch)
- **Body**: CreateRequest with data
- **Response**: Created record(s)

```http
PUT /public/users
Content-Type: application/json

{
  "data": {
    "name": "John Doe",
    "email": "john@example.com"
  }
}
```

### DELETE - Remove Records
- **Purpose**: Delete records by primary key or unique constraint
- **Body**: PrimaryKeyRequest with key values
- **Response**: Deletion confirmation

```http
DELETE /public/users
Content-Type: application/json

{
  "values": {
    "id": 123
  }
}
```

## JSON Filter Format

### Complete Structure

```json
{
  "select": {
    "fields": ["field1", "field2"],
    "joinable_table": {
      "fields": ["field1", "field2"]
    }
  },
  "where": {
    "type": "logical",
    "operator": "and",
    "conditions": [
      {
        "type": "comparison",
        "field": "status",
        "operator": "neq",
        "value": "Obsolete"
      }
    ]
  },
  "pagination": {
    "limit": 100,
    "offset": 0
  }
}
```

### Select Clause

Controls which fields to return and supports nested table selection:

```json
{
  "select": {
    "fields": ["id", "name", "email"],
    "profile": {
      "fields": ["age", "location"]
    },
    "orders": {
      "fields": ["id", "total", "created_at"]
    }
  }
}
```

### Where Clause

Supports complex logical and comparison operations:

#### Comparison Operators
- `eq` - Equal
- `neq` - Not equal  
- `gt` - Greater than
- `gte` - Greater than or equal
- `lt` - Less than
- `lte` - Less than or equal
- `like` - Pattern matching (case-sensitive)
- `ilike` - Pattern matching (case-insensitive)
- `in` - Value in list
- `not_in` - Value not in list
- `is_null` - Field is NULL
- `is_not_null` - Field is not NULL

#### Logical Operators
- `and` - All conditions must be true
- `or` - At least one condition must be true
- `not` - Negate the condition

#### Example Complex Where Clause

```json
{
  "where": {
    "type": "logical",
    "operator": "and",
    "conditions": [
      {
        "type": "comparison",
        "field": "active",
        "operator": "eq",
        "value": true
      },
      {
        "type": "logical",
        "operator": "or",
        "conditions": [
          {
            "type": "comparison",
            "field": "role",
            "operator": "in",
            "value": ["admin", "moderator"]
          },
          {
            "type": "comparison",
            "field": "created_at",
            "operator": "gte",
            "value": "2023-01-01"
          }
        ]
      }
    ]
  }
}
```

### Pagination

Supports limit/offset pagination with metadata:

```json
{
  "pagination": {
    "limit": 50,
    "offset": 100,
    "cursor": "optional_cursor_value"
  }
}
```

## Response Formats

### Standard Data Response

All data retrieval operations return this format:

```json
{
  "results": [
    {
      "id": 1,
      "name": "John Doe",
      "email": "john@example.com",
      "active": true
    }
  ],
  "total": 150,
  "pagination": {
    "limit": 10,
    "offset": 0,
    "total": 150,
    "has_more": true
  }
}
```

### Create Response

PUT operations return the created record(s):

```json
{
  "id": 123,
  "name": "John Doe",
  "email": "john@example.com",
  "created_at": "2023-12-01T15:30:00Z"
}
```

### Update Response

POST update operations return the updated record:

```json
{
  "id": 123,
  "name": "Updated Name",
  "email": "updated@example.com",
  "updated_at": "2023-12-01T16:00:00Z"
}
```

### Delete Response

DELETE operations return confirmation:

```json
{
  "deleted": 1,
  "message": "Deleted 1 record(s)"
}
```

## Request Models

### FilterRequest

```python
class FilterRequest(BaseModel):
    select: Optional[SelectClause] = None
    where: Optional[WhereClause] = None
    pagination: Optional[PaginationParams] = None
```

### UpdateRequest

```python
class UpdateRequest(BaseModel):
    key: PrimaryKeyRequest  # Primary key or unique constraint
    data: Dict[str, Any]    # Fields to update
```

### CreateRequest

```python
class CreateRequest(BaseModel):
    data: Union[Dict[str, Any], List[Dict[str, Any]]]  # Single or batch
```

### PrimaryKeyRequest

```python
class PrimaryKeyRequest(BaseModel):
    values: Dict[str, Any]  # Key-value pairs for PK or unique constraint
```

## Security Considerations

### Key Validation

All modification operations (UPDATE, DELETE) validate that the provided keys match either:
- The table's primary key columns exactly, OR
- A unique constraint's columns exactly

This prevents accidental modification of multiple records.

### SQL Injection Prevention

All values are properly parameterized and passed through the query builder's type-safe parameter system.

### Type Validation

All request bodies are validated using Pydantic models, ensuring type safety and proper data formats.

## Examples

### Simple Queries

```bash
# Get all users with pagination
curl "http://localhost:8000/public/users?limit=10&offset=0"

# Get specific fields
curl "http://localhost:8000/public/users?select_fields=id,name,email&limit=5"
```

### Complex Filtering

```bash
curl -X POST http://localhost:8000/public/users \
  -H "Content-Type: application/json" \
  -d '{
    "where": {
      "type": "logical",
      "operator": "and",
      "conditions": [
        {
          "type": "comparison",
          "field": "active",
          "operator": "eq",
          "value": true
        },
        {
          "type": "comparison",
          "field": "name",
          "operator": "ilike",
          "value": "%john%"
        }
      ]
    },
    "pagination": {
      "limit": 20,
      "offset": 0
    }
  }'
```

### Creating Records

```bash
# Single record
curl -X PUT http://localhost:8000/public/users \
  -H "Content-Type: application/json" \
  -d '{
    "data": {
      "name": "John Doe",
      "email": "john@example.com",
      "active": true
    }
  }'

# Multiple records
curl -X PUT http://localhost:8000/public/users \
  -H "Content-Type: application/json" \
  -d '{
    "data": [
      {
        "name": "Alice Smith",
        "email": "alice@example.com",
        "active": true
      },
      {
        "name": "Bob Johnson",
        "email": "bob@example.com",
        "active": true
      }
    ]
  }'
```

### Updating Records

```bash
curl -X POST http://localhost:8000/public/users \
  -H "Content-Type: application/json" \
  -d '{
    "key": {
      "values": {
        "id": 123
      }
    },
    "data": {
      "name": "Updated Name",
      "email": "updated@example.com"
    }
  }'
```

### Deleting Records

```bash
curl -X DELETE http://localhost:8000/public/users \
  -H "Content-Type: application/json" \
  -d '{
    "values": {
      "id": 123
    }
  }'
```

## Error Handling

The API returns appropriate HTTP status codes:

- `200 OK` - Successful operation
- `400 Bad Request` - Invalid request format or constraint violation
- `404 Not Found` - Record not found (for updates/deletes)
- `500 Internal Server Error` - Database or server error

Error responses include descriptive messages:

```json
{
  "detail": "Provided keys must match a primary key or unique constraint"
}
```

## Performance Considerations

1. **Pagination**: Always use pagination for large datasets
2. **Field Selection**: Use the `select` clause to limit returned fields
3. **Indexing**: Ensure filtered fields are properly indexed
4. **Batch Operations**: Use batch creates for multiple records

## See Also

- [Query Builder Documentation](../pghatch/query_builder/README.md)
- [Examples](../examples/enhanced_endpoints_examples.py)
- [Test Suite](../tests/test_enhanced_endpoints.py)