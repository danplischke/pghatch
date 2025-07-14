"""
Test the enhanced endpoints functionality.
"""
import pytest
from unittest.mock import Mock, AsyncMock
from pghatch.router.filter_models import (
    FilterRequest, SelectClause, WhereClause, LogicalCondition, 
    ComparisonCondition, LogicalOperator, ComparisonOperator,
    PaginationParams, StandardResponse
)
from pghatch.router.filter_parser import FilterParser
from pghatch.router.enhanced_table_resolver import EnhancedTableViewResolver


class TestFilterModels:
    """Test the filter model definitions."""
    
    def test_comparison_condition(self):
        """Test creating a comparison condition."""
        condition = ComparisonCondition(
            field="name",
            operator=ComparisonOperator.EQ,
            value="John"
        )
        assert condition.field == "name"
        assert condition.operator == ComparisonOperator.EQ
        assert condition.value == "John"
    
    def test_logical_condition(self):
        """Test creating a logical condition."""
        condition1 = ComparisonCondition(
            field="name",
            operator=ComparisonOperator.EQ,
            value="John"
        )
        condition2 = ComparisonCondition(
            field="age",
            operator=ComparisonOperator.GT,
            value=18
        )
        
        logical_condition = LogicalCondition(
            operator=LogicalOperator.AND,
            conditions=[condition1, condition2]
        )
        
        assert logical_condition.operator == LogicalOperator.AND
        assert len(logical_condition.conditions) == 2
    
    def test_filter_request(self):
        """Test creating a complete filter request."""
        select_clause = SelectClause(fields=["id", "name", "email"])
        
        where_clause = WhereClause(
            operator=LogicalOperator.AND,
            conditions=[
                ComparisonCondition(
                    field="active",
                    operator=ComparisonOperator.EQ,
                    value=True
                )
            ]
        )
        
        pagination = PaginationParams(limit=10, offset=0)
        
        filter_request = FilterRequest(
            select=select_clause,
            where=where_clause,
            pagination=pagination
        )
        
        assert filter_request.select.fields == ["id", "name", "email"]
        assert filter_request.where.operator == LogicalOperator.AND
        assert filter_request.pagination.limit == 10


class TestFilterParser:
    """Test the filter parser functionality."""
    
    @pytest.fixture
    def mock_introspection(self):
        """Create a mock introspection object."""
        introspection = Mock()
        return introspection
    
    @pytest.fixture
    def filter_parser(self, mock_introspection):
        """Create a filter parser with mock introspection."""
        return FilterParser(mock_introspection)
    
    def test_parse_simple_filter(self, filter_parser):
        """Test parsing a simple filter request."""
        filter_request = FilterRequest(
            select=SelectClause(fields=["id", "name"]),
            where=WhereClause(
                operator=LogicalOperator.AND,
                conditions=[
                    ComparisonCondition(
                        field="active",
                        operator=ComparisonOperator.EQ,
                        value=True
                    )
                ]
            ),
            pagination=PaginationParams(limit=10, offset=0)
        )
        
        query, count_query = filter_parser.parse_filter_request(
            filter_request, "users", "public"
        )
        
        # Test that queries are built without errors
        assert query is not None
        assert count_query is not None
        
        # Test SQL generation
        sql, params = query.build()
        assert "users" in sql
        assert "SELECT" in sql.upper()
        
        count_sql, count_params = count_query.build()
        assert "COUNT" in count_sql.upper()


class TestEnhancedTableResolver:
    """Test the enhanced table resolver."""
    
    @pytest.fixture
    def mock_introspection(self):
        """Create a mock introspection object."""
        introspection = Mock()
        
        # Mock class
        mock_class = Mock()
        mock_class.relname = "test_table"
        mock_class.relkind = "r"  # Regular table
        introspection.get_class.return_value = mock_class
        
        # Mock namespace
        mock_namespace = Mock()
        mock_namespace.nspname = "public"
        introspection.get_namespace.return_value = mock_namespace
        
        # Mock attributes
        mock_attr = Mock()
        mock_attr.attname = "id"
        mock_attr.attisdropped = False
        mock_attr.get_type.return_value = Mock()
        mock_attr.get_py_type.return_value = (int, int)
        introspection.get_attributes.return_value = [mock_attr]
        
        # Mock constraints
        introspection.get_constraints.return_value = []
        introspection.get_description.return_value = None
        
        return introspection
    
    def test_resolver_initialization(self, mock_introspection):
        """Test that the resolver initializes correctly."""
        resolver = EnhancedTableViewResolver("123", mock_introspection)
        
        assert resolver.name == "test_table"
        assert resolver.schema == "public"
        assert resolver.oid == "123"
        assert len(resolver.fields) == 1
        assert resolver.fields[0] == "id"


if __name__ == "__main__":
    # Run basic tests
    test_models = TestFilterModels()
    test_models.test_comparison_condition()
    test_models.test_logical_condition()
    test_models.test_filter_request()
    print("✓ Filter models tests passed")
    
    # Test filter parser with mock
    from unittest.mock import Mock
    introspection = Mock()
    parser = FilterParser(introspection)
    print("✓ Filter parser created successfully")
    
    print("All basic tests passed!")