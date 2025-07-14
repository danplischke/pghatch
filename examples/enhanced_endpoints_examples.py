"""
Example usage of the enhanced REST endpoints.

This example demonstrates how to use the new JSON filter format and CRUD operations.
"""

import asyncio
import json
from typing import Dict, Any


# Example JSON filter requests

# 1. Simple GET with query parameters
def example_get_request():
    """Example GET request using query parameters."""
    # GET /public/users?limit=10&offset=0&select_fields=id,name,email
    return {
        "method": "GET",
        "url": "/public/users",
        "params": {
            "limit": 10,
            "offset": 0,
            "select_fields": "id,name,email"
        }
    }


# 2. Complex POST query with JSON filter format
def example_complex_query():
    """Example POST request with complex JSON filter."""
    filter_request = {
        "select": {
            "fields": ["id", "name", "email", "created_at"],
            "profile": {
                "fields": ["age", "location"]
            }
        },
        "where": {
            "type": "logical",
            "operator": "and",
            "conditions": [
                {
                    "type": "comparison",
                    "field": "active",
                    "operator": "eq",
                    "value": True
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
        },
        "pagination": {
            "limit": 50,
            "offset": 0
        }
    }
    
    return {
        "method": "POST",
        "url": "/public/users",
        "json": filter_request
    }


# 3. Update by primary key
def example_update_request():
    """Example POST request to update a record by primary key."""
    update_request = {
        "key": {
            "values": {
                "id": 123
            }
        },
        "data": {
            "name": "Updated Name",
            "email": "updated@example.com",
            "active": True
        }
    }
    
    return {
        "method": "POST",
        "url": "/public/users",
        "json": update_request
    }


# 4. Create new records
def example_create_request():
    """Example PUT request to create new records."""
    # Single record
    create_single = {
        "data": {
            "name": "John Doe",
            "email": "john@example.com",
            "active": True,
            "role": "user"
        }
    }
    
    # Multiple records
    create_multiple = {
        "data": [
            {
                "name": "Alice Smith",
                "email": "alice@example.com",
                "active": True,
                "role": "admin"
            },
            {
                "name": "Bob Johnson",
                "email": "bob@example.com", 
                "active": True,
                "role": "user"
            }
        ]
    }
    
    return [
        {
            "method": "PUT",
            "url": "/public/users",
            "json": create_single
        },
        {
            "method": "PUT",
            "url": "/public/users",
            "json": create_multiple
        }
    ]


# 5. Delete by primary key
def example_delete_request():
    """Example DELETE request to remove a record."""
    delete_request = {
        "values": {
            "id": 123
        }
    }
    
    return {
        "method": "DELETE",
        "url": "/public/users",
        "json": delete_request
    }


# 6. Advanced filtering examples
def example_advanced_filters():
    """Examples of advanced filtering capabilities."""
    
    # Text search with ILIKE
    text_search = {
        "where": {
            "type": "logical",
            "operator": "or",
            "conditions": [
                {
                    "type": "comparison",
                    "field": "name",
                    "operator": "ilike",
                    "value": "%john%"
                },
                {
                    "type": "comparison",
                    "field": "email",
                    "operator": "ilike",
                    "value": "%john%"
                }
            ]
        }
    }
    
    # Date range filtering
    date_range = {
        "where": {
            "type": "logical",
            "operator": "and",
            "conditions": [
                {
                    "type": "comparison",
                    "field": "created_at",
                    "operator": "gte",
                    "value": "2023-01-01"
                },
                {
                    "type": "comparison",
                    "field": "created_at",
                    "operator": "lt",
                    "value": "2024-01-01"
                }
            ]
        }
    }
    
    # Null value filtering
    null_check = {
        "where": {
            "type": "logical",
            "operator": "and",
            "conditions": [
                {
                    "type": "comparison",
                    "field": "deleted_at",
                    "operator": "is_null"
                },
                {
                    "type": "comparison",
                    "field": "email",
                    "operator": "is_not_null"
                }
            ]
        }
    }
    
    # Complex nested conditions
    complex_nested = {
        "where": {
            "type": "logical",
            "operator": "and",
            "conditions": [
                {
                    "type": "comparison",
                    "field": "active",
                    "operator": "eq",
                    "value": True
                },
                {
                    "type": "logical",
                    "operator": "or",
                    "conditions": [
                        {
                            "type": "logical",
                            "operator": "and",
                            "conditions": [
                                {
                                    "type": "comparison",
                                    "field": "role",
                                    "operator": "eq",
                                    "value": "admin"
                                },
                                {
                                    "type": "comparison",
                                    "field": "permissions",
                                    "operator": "in",
                                    "value": ["read", "write", "delete"]
                                }
                            ]
                        },
                        {
                            "type": "comparison",
                            "field": "is_superuser",
                            "operator": "eq",
                            "value": True
                        }
                    ]
                }
            ]
        }
    }
    
    return {
        "text_search": text_search,
        "date_range": date_range,
        "null_check": null_check,
        "complex_nested": complex_nested
    }


# Expected response formats
def example_response_formats():
    """Examples of expected response formats."""
    
    # Standard data retrieval response
    data_response = {
        "results": [
            {
                "id": 1,
                "name": "John Doe",
                "email": "john@example.com",
                "active": True,
                "created_at": "2023-01-15T10:30:00Z"
            },
            {
                "id": 2,
                "name": "Alice Smith", 
                "email": "alice@example.com",
                "active": True,
                "created_at": "2023-02-01T14:20:00Z"
            }
        ],
        "total": 150,
        "pagination": {
            "limit": 10,
            "offset": 0,
            "total": 150,
            "has_more": True
        }
    }
    
    # Create response (single record)
    create_response_single = {
        "id": 123,
        "name": "John Doe",
        "email": "john@example.com",
        "active": True,
        "created_at": "2023-12-01T15:30:00Z"
    }
    
    # Create response (multiple records)
    create_response_multiple = [
        {
            "id": 124,
            "name": "Alice Smith",
            "email": "alice@example.com",
            "active": True,
            "created_at": "2023-12-01T15:31:00Z"
        },
        {
            "id": 125,
            "name": "Bob Johnson",
            "email": "bob@example.com",
            "active": True,
            "created_at": "2023-12-01T15:32:00Z"
        }
    ]
    
    # Update response
    update_response = {
        "id": 123,
        "name": "Updated Name",
        "email": "updated@example.com",
        "active": True,
        "updated_at": "2023-12-01T16:00:00Z"
    }
    
    # Delete response
    delete_response = {
        "deleted": 1,
        "message": "Deleted 1 record(s)"
    }
    
    return {
        "data_response": data_response,
        "create_response_single": create_response_single,
        "create_response_multiple": create_response_multiple,
        "update_response": update_response,
        "delete_response": delete_response
    }


if __name__ == "__main__":
    print("=== Enhanced REST Endpoints Usage Examples ===\n")
    
    print("1. Simple GET Request:")
    print(json.dumps(example_get_request(), indent=2))
    print()
    
    print("2. Complex Query (POST):")
    print(json.dumps(example_complex_query(), indent=2))
    print()
    
    print("3. Update Request (POST):")
    print(json.dumps(example_update_request(), indent=2))
    print()
    
    print("4. Create Requests (PUT):")
    for i, req in enumerate(example_create_request(), 1):
        print(f"4.{i}:")
        print(json.dumps(req, indent=2))
        print()
    
    print("5. Delete Request (DELETE):")
    print(json.dumps(example_delete_request(), indent=2))
    print()
    
    print("6. Advanced Filter Examples:")
    filters = example_advanced_filters()
    for name, filter_def in filters.items():
        print(f"   {name}:")
        print(json.dumps(filter_def, indent=4))
        print()
    
    print("7. Expected Response Formats:")
    responses = example_response_formats()
    for name, response in responses.items():
        print(f"   {name}:")
        print(json.dumps(response, indent=4))
        print()