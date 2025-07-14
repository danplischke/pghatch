"""
Models for the JSON filter format as specified in the issue.
"""
from typing import Any, Dict, List, Optional, Union, Literal
from pydantic import BaseModel, Field
from enum import Enum


class ComparisonOperator(str, Enum):
    """Supported comparison operators for filters."""
    EQ = "eq"
    NEQ = "neq"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    LIKE = "like"
    ILIKE = "ilike"
    IN = "in"
    NOT_IN = "not_in"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"


class LogicalOperator(str, Enum):
    """Supported logical operators for combining conditions."""
    AND = "and"
    OR = "or"
    NOT = "not"


class ComparisonCondition(BaseModel):
    """A comparison condition for filtering."""
    type: Literal["comparison"] = "comparison"
    field: str = Field(..., description="Field name to compare against")
    operator: ComparisonOperator = Field(..., description="Comparison operator")
    value: Optional[Any] = Field(None, description="Value to compare with")


class LogicalCondition(BaseModel):
    """A logical condition for combining multiple conditions."""
    type: Literal["logical"] = "logical"
    operator: LogicalOperator = Field(..., description="Logical operator")
    conditions: List[Union["ComparisonCondition", "LogicalCondition"]] = Field(
        ..., description="List of conditions to combine"
    )


class JoinableTable(BaseModel):
    """Configuration for joinable tables in select."""
    fields: List[str] = Field(..., description="Fields to select from joined table")
    
    # Allow additional nested joinable tables
    model_config = {"extra": "allow"}


class SelectClause(BaseModel):
    """Select clause configuration."""
    fields: List[str] = Field(..., description="Fields to select from main table")
    
    # Allow additional joinable tables as extra fields
    model_config = {"extra": "allow"}


class WhereClause(BaseModel):
    """Where clause with support for logical and comparison conditions."""
    type: Literal["logical"] = "logical"
    operator: LogicalOperator = Field(..., description="Root logical operator")
    conditions: List[Union[ComparisonCondition, LogicalCondition]] = Field(
        ..., description="List of conditions"
    )


class PaginationParams(BaseModel):
    """Pagination parameters."""
    limit: Optional[int] = Field(None, ge=1, le=10000, description="Maximum number of rows to return")
    offset: Optional[int] = Field(None, ge=0, description="Number of rows to skip")
    cursor: Optional[str] = Field(None, description="Cursor for cursor-based pagination")


class FilterRequest(BaseModel):
    """Complete filter request structure."""
    select: Optional[SelectClause] = Field(None, description="Fields to select")
    where: Optional[WhereClause] = Field(None, description="Filter conditions")
    pagination: Optional[PaginationParams] = Field(None, description="Pagination parameters")


class StandardResponse(BaseModel):
    """Standardized response format."""
    results: List[Dict[str, Any]] = Field(..., description="Query results")
    total: int = Field(..., description="Total number of matching records")
    pagination: Optional[Dict[str, Any]] = Field(None, description="Pagination metadata")


class PrimaryKeyRequest(BaseModel):
    """Request for operations by primary key or unique constraints."""
    values: Dict[str, Any] = Field(..., description="Key-value pairs for primary key or unique constraint")


class UpdateRequest(BaseModel):
    """Request for updating records."""
    key: PrimaryKeyRequest = Field(..., description="Primary key or unique constraint to identify record")
    data: Dict[str, Any] = Field(..., description="Data to update")


class CreateRequest(BaseModel):
    """Request for creating new records."""
    data: Union[Dict[str, Any], List[Dict[str, Any]]] = Field(..., description="Data to create")