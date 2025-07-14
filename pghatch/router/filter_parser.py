"""
Parser for converting JSON filter format to SQL queries using the query builder.
"""
from typing import Any, Dict, List, Optional, Tuple
from pghatch.query_builder.builder import Query, select
from pghatch.query_builder.expressions import eq, neq, gt, gte, lt, lte, like, ilike, in_, not_in, is_null, is_not_null, and_, or_, not_
from pghatch.router.filter_models import (
    FilterRequest, ComparisonCondition, LogicalCondition, 
    ComparisonOperator, LogicalOperator, SelectClause, WhereClause,
    PaginationParams
)
from pghatch.introspection.introspection import Introspection


class FilterParser:
    """Converts JSON filter format to SQL queries."""

    def __init__(self, introspection: Introspection):
        self.introspection = introspection

    def parse_filter_request(self, filter_request: FilterRequest, table_name: str, schema: str = "public") -> Tuple[Query, int]:
        """
        Parse a FilterRequest into a Query object and get total count.
        
        Returns:
            Tuple of (main_query, total_count_query)
        """
        # Start with base query
        query = select()
        query = query.from_(table_name, schema)

        # Handle SELECT clause
        if filter_request.select:
            query = self._apply_select_clause(query, filter_request.select, table_name, schema)
        else:
            # Default to all fields
            query = query.select("*")

        # Handle WHERE clause
        if filter_request.where:
            where_expr = self._parse_where_clause(filter_request.where)
            if where_expr:
                query = query.where(where_expr)

        # Create count query for total
        count_query = select("COUNT(*) as total").from_(table_name, schema)
        if filter_request.where:
            where_expr = self._parse_where_clause(filter_request.where)
            if where_expr:
                count_query = count_query.where(where_expr)

        # Handle pagination
        if filter_request.pagination:
            query = self._apply_pagination(query, filter_request.pagination)

        return query, count_query

    def _apply_select_clause(self, query: Query, select_clause: SelectClause, table_name: str, schema: str) -> Query:
        """Apply SELECT clause with field selection and joins."""
        # Add main table fields
        for field in select_clause.fields:
            query = query.select(field)

        # Handle joins - for now, we'll implement basic join support
        # In a full implementation, we'd need to parse the dynamic joinable tables
        # from the select_clause.__dict__ or use a more sophisticated approach
        
        return query

    def _parse_where_clause(self, where_clause: WhereClause):
        """Parse WHERE clause into expression tree."""
        return self._parse_logical_condition(where_clause)

    def _parse_logical_condition(self, condition: LogicalCondition):
        """Parse a logical condition."""
        expressions = []
        
        for sub_condition in condition.conditions:
            if sub_condition.type == "comparison":
                expr = self._parse_comparison_condition(sub_condition)
                if expr:
                    expressions.append(expr)
            elif sub_condition.type == "logical":
                expr = self._parse_logical_condition(sub_condition)
                if expr:
                    expressions.append(expr)

        if not expressions:
            return None

        # Combine expressions based on operator
        if condition.operator == LogicalOperator.AND:
            result = expressions[0]
            for expr in expressions[1:]:
                result = and_(result, expr)
            return result
        elif condition.operator == LogicalOperator.OR:
            result = expressions[0]
            for expr in expressions[1:]:
                result = or_(result, expr)
            return result
        elif condition.operator == LogicalOperator.NOT:
            # NOT should have exactly one expression
            if expressions:
                return not_(expressions[0])
            return None

        return None

    def _parse_comparison_condition(self, condition: ComparisonCondition):
        """Parse a comparison condition."""
        field = condition.field
        operator = condition.operator
        value = condition.value

        # Map operators to query builder functions
        if operator == ComparisonOperator.EQ:
            return eq(field, value)
        elif operator == ComparisonOperator.NEQ:
            return neq(field, value)
        elif operator == ComparisonOperator.GT:
            return gt(field, value)
        elif operator == ComparisonOperator.GTE:
            return gte(field, value)
        elif operator == ComparisonOperator.LT:
            return lt(field, value)
        elif operator == ComparisonOperator.LTE:
            return lte(field, value)
        elif operator == ComparisonOperator.LIKE:
            return like(field, value)
        elif operator == ComparisonOperator.ILIKE:
            return ilike(field, value)
        elif operator == ComparisonOperator.IN:
            if isinstance(value, list):
                return in_(field, value)
            else:
                # Single value, treat as equals
                return eq(field, value)
        elif operator == ComparisonOperator.NOT_IN:
            if isinstance(value, list):
                return not_in(field, value)
            else:
                # Single value, treat as not equals
                return neq(field, value)
        elif operator == ComparisonOperator.IS_NULL:
            return is_null(field)
        elif operator == ComparisonOperator.IS_NOT_NULL:
            return is_not_null(field)

        return None

    def _apply_pagination(self, query: Query, pagination: PaginationParams) -> Query:
        """Apply pagination to query."""
        if pagination.limit is not None:
            query = query.limit(pagination.limit)
        
        if pagination.offset is not None:
            query = query.offset(pagination.offset)

        # TODO: Implement cursor-based pagination when needed
        if pagination.cursor:
            # For now, we'll skip cursor implementation
            pass

        return query

    def get_primary_key_columns(self, table_oid: str) -> List[str]:
        """Get primary key column names for a table."""
        constraints = self.introspection.get_constraints(table_oid)
        
        for constraint in constraints:
            if constraint.contype == 'p':  # Primary key constraint
                attributes = constraint.get_attributes(self.introspection)
                if attributes:
                    return [attr.attname for attr in attributes]
        
        return []

    def get_unique_constraints(self, table_oid: str) -> List[List[str]]:
        """Get unique constraint column combinations for a table."""
        constraints = self.introspection.get_constraints(table_oid)
        unique_constraints = []
        
        for constraint in constraints:
            if constraint.contype == 'u':  # Unique constraint
                attributes = constraint.get_attributes(self.introspection)
                if attributes:
                    unique_constraints.append([attr.attname for attr in attributes])
        
        return unique_constraints

    def build_key_condition(self, key_values: Dict[str, Any]):
        """Build a condition for matching by key values."""
        conditions = []
        for field, value in key_values.items():
            conditions.append(eq(field, value))
        
        if len(conditions) == 1:
            return conditions[0]
        else:
            # Combine with AND
            result = conditions[0]
            for condition in conditions[1:]:
                result = and_(result, condition)
            return result