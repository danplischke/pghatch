"""
Unit tests for pghatch.introspection.pgtypes module.
Tests PostgreSQL to Python type mapping functionality.
"""

import pytest
from unittest.mock import MagicMock, patch
from enum import Enum
from typing import List, Tuple
import datetime

from pghatch.introspection.pgtypes import (
    get_py_type,
    _get_nullable_type,
    _get_array_type,
    _get_composite_type,
    _get_py_type_by_category,
    Interval,
    Point,
    Line,
    LineSegment,
    Box,
    Path,
    Polygon,
    Circle,
)
from pghatch.introspection.tables import PgType, PgAttribute
from pghatch.introspection.introspection import Introspection


class TestCustomTypes:
    """Test custom PostgreSQL type classes."""

    def test_interval_class(self):
        """Test Interval class functionality."""
        interval = Interval(years=1, months=2, days=3, hours=4, minutes=5, seconds=6.5)

        assert interval.years == 1
        assert interval.months == 2
        assert interval.days == 3
        assert interval.hours == 4
        assert interval.minutes == 5
        assert interval.seconds == 6.5

        # Test string representation
        repr_str = repr(interval)
        assert "Interval(" in repr_str
        assert "years=1" in repr_str
        assert "seconds=6.5" in repr_str

    def test_point_class(self):
        """Test Point class functionality."""
        point = Point(x=1.5, y=2.5)

        assert point.x == 1.5
        assert point.y == 2.5

        repr_str = repr(point)
        assert "Point(x=1.5, y=2.5)" == repr_str

    def test_line_class(self):
        """Test Line class functionality."""
        point_a = Point(0, 0)
        point_b = Point(1, 1)
        line = Line(point_a, point_b)

        assert line.a == point_a
        assert line.b == point_b

        repr_str = repr(line)
        assert "Line(" in repr_str

    def test_line_segment_class(self):
        """Test LineSegment class functionality."""
        point_a = Point(0, 0)
        point_b = Point(1, 1)
        line_segment = LineSegment(point_a, point_b)

        assert line_segment.a == point_a
        assert line_segment.b == point_b

    def test_box_class(self):
        """Test Box class functionality."""
        point_a = Point(0, 0)
        point_b = Point(1, 1)
        box = Box(point_a, point_b)

        assert box.a == point_a
        assert box.b == point_b

    def test_path_class(self):
        """Test Path class functionality."""
        points = [Point(0, 0), Point(1, 1), Point(2, 0)]
        path = Path(points, is_open=True)

        assert path.points == points
        assert path.is_open is True

    def test_polygon_class(self):
        """Test Polygon class functionality."""
        points = [Point(0, 0), Point(1, 0), Point(1, 1), Point(0, 1)]
        polygon = Polygon(points)

        assert polygon.points == points

    def test_circle_class(self):
        """Test Circle class functionality."""
        center = Point(0, 0)
        circle = Circle(center, 5.0)

        assert circle.center == center
        assert circle.radius == 5.0


class TestNullableType:
    """Test nullable type handling."""

    def test_nullable_type_with_nullable_attribute(self):
        """Test nullable type creation with nullable attribute."""
        mock_attr = MagicMock()
        mock_attr.attnotnull = False

        result = _get_nullable_type(int, attr=mock_attr)

        # Check if it's a union type with None
        assert hasattr(result, '__args__')
        assert int in result.__args__
        assert type(None) in result.__args__

    def test_nullable_type_with_non_nullable_attribute(self):
        """Test nullable type creation with non-nullable attribute."""
        mock_attr = MagicMock()
        mock_attr.attnotnull = True

        result = _get_nullable_type(int, attr=mock_attr)

        assert result == int

    def test_nullable_type_with_nullable_pg_type(self):
        """Test nullable type creation with nullable PgType."""
        mock_type = MagicMock()
        mock_type.typnotnull = False

        result = _get_nullable_type(str, pg_type=mock_type)

        # Check if it's a union type with None
        assert hasattr(result, '__args__')
        assert str in result.__args__
        assert type(None) in result.__args__

    def test_nullable_type_with_non_nullable_pg_type(self):
        """Test nullable type creation with non-nullable PgType."""
        mock_type = MagicMock()
        mock_type.typnotnull = True

        result = _get_nullable_type(str, pg_type=mock_type)

        assert result == str


class TestArrayType:
    """Test array type handling."""

    def test_array_type_from_pg_type(self):
        """Test array type creation from PgType."""
        mock_introspection = MagicMock()
        mock_elem_type = MagicMock()
        mock_elem_type.typcategory = "N"  # Numeric
        mock_elem_type.typname = "int4"

        mock_type = MagicMock()
        mock_type.get_elem_type.return_value = mock_elem_type
        mock_type.typndims = 1

        # Mock get_py_type to return int for the element type
        with patch('pghatch.introspection.pgtypes.get_py_type', return_value=int):
            result = _get_array_type(mock_introspection, typ=mock_type)

        # Should be List[int]
        assert hasattr(result, '__origin__')
        assert result.__origin__ == list
        assert result.__args__[0] == int

    def test_array_type_from_attribute(self):
        """Test array type creation from PgAttribute."""
        mock_introspection = MagicMock()
        mock_elem_type = MagicMock()
        mock_elem_type.typcategory = "S"  # String
        mock_elem_type.typname = "text"

        mock_type = MagicMock()
        mock_type.get_elem_type.return_value = mock_elem_type

        mock_attr = MagicMock()
        mock_attr.get_type.return_value = mock_type
        mock_attr.attndims = 2  # 2D array

        # Mock get_py_type to return str for the element type
        with patch('pghatch.introspection.pgtypes.get_py_type', return_value=str):
            result = _get_array_type(mock_introspection, attr=mock_attr)

        # Should be List[List[str]]
        assert hasattr(result, '__origin__')
        assert result.__origin__ == list
        assert hasattr(result.__args__[0], '__origin__')
        assert result.__args__[0].__origin__ == list
        assert result.__args__[0].__args__[0] == str

    def test_array_type_default_dimensions(self):
        """Test array type with default dimensions."""
        mock_introspection = MagicMock()
        mock_elem_type = MagicMock()
        mock_elem_type.typcategory = "B"  # Boolean
        mock_elem_type.typname = "bool"

        mock_type = MagicMock()
        mock_type.get_elem_type.return_value = mock_elem_type
        mock_type.typndims = 0  # No dimensions specified

        # Mock get_py_type to return bool for the element type
        with patch('pghatch.introspection.pgtypes.get_py_type', return_value=bool):
            result = _get_array_type(mock_introspection, typ=mock_type)

        # Should default to 1D array: List[bool]
        assert hasattr(result, '__origin__')
        assert result.__origin__ == list
        assert result.__args__[0] == bool


class TestCompositeType:
    """Test composite type handling."""

    def test_composite_type_creation(self):
        """Test composite type creation from relation."""
        mock_introspection = MagicMock()

        # Mock relation (table/composite type)
        mock_relation = MagicMock()
        mock_relation.relname = "address"

        # Mock attributes
        mock_attr1 = MagicMock()
        mock_attr1.attname = "street"
        mock_attr1.attisdropped = False
        mock_attr1.get_description.return_value = "Street address"

        mock_attr2 = MagicMock()
        mock_attr2.attname = "city"
        mock_attr2.attisdropped = False
        mock_attr2.get_description.return_value = "City name"

        mock_relation.get_attributes.return_value = [mock_attr1, mock_attr2]

        mock_type = MagicMock()
        mock_type.typrelid = "12345"

        mock_introspection.get_class.return_value = mock_relation

        # Mock get_py_type to return appropriate types
        with patch('pghatch.introspection.pgtypes.get_py_type', side_effect=[str, str]):
            result = _get_composite_type(mock_introspection, typ=mock_type)

        # Should return a Pydantic model class
        assert hasattr(result, 'model_fields')
        assert 'street' in result.model_fields
        assert 'city' in result.model_fields

    def test_composite_type_with_dropped_attributes(self):
        """Test composite type creation ignoring dropped attributes."""
        mock_introspection = MagicMock()

        mock_relation = MagicMock()
        mock_relation.relname = "test_type"

        # Mock attributes with one dropped
        mock_attr1 = MagicMock()
        mock_attr1.attname = "active_field"
        mock_attr1.attisdropped = False
        mock_attr1.get_description.return_value = "Active field"

        mock_attr2 = MagicMock()
        mock_attr2.attname = "dropped_field"
        mock_attr2.attisdropped = True  # This should be ignored

        mock_relation.get_attributes.return_value = [mock_attr1, mock_attr2]

        mock_type = MagicMock()
        mock_type.typrelid = "12345"

        mock_introspection.get_class.return_value = mock_relation

        with patch('pghatch.introspection.pgtypes.get_py_type', return_value=str):
            result = _get_composite_type(mock_introspection, typ=mock_type)

        # Should only have the active field
        assert hasattr(result, 'model_fields')
        assert 'active_field' in result.model_fields
        assert 'dropped_field' not in result.model_fields

    def test_composite_type_missing_relation(self):
        """Test composite type creation with missing relation."""
        mock_introspection = MagicMock()
        mock_introspection.get_class.return_value = None

        mock_type = MagicMock()
        mock_type.typname = "missing_type"
        mock_type.typrelid = "99999"

        with pytest.raises(ValueError, match="Relation for type missing_type not found"):
            _get_composite_type(mock_introspection, typ=mock_type)


class TestTypeCategoryMapping:
    """Test type category to Python type mapping."""

    def test_array_category(self):
        """Test array type category mapping."""
        mock_introspection = MagicMock()
        mock_type = MagicMock()
        mock_type.typcategory = "A"

        with patch('pghatch.introspection.pgtypes._get_array_py_type', return_value=List[int]) as mock_func:
            result = _get_py_type_by_category(mock_introspection, typ=mock_type)
            mock_func.assert_called_once_with(mock_introspection, mock_type, None)
            assert result == List[int]

    def test_boolean_category(self):
        """Test boolean type category mapping."""
        mock_introspection = MagicMock()
        mock_type = MagicMock()
        mock_type.typcategory = "B"

        with patch('pghatch.introspection.pgtypes._get_boolean_py_type', return_value=bool) as mock_func:
            result = _get_py_type_by_category(mock_introspection, typ=mock_type)
            mock_func.assert_called_once_with(mock_introspection, mock_type, None)
            assert result == bool

    def test_composite_category(self):
        """Test composite type category mapping."""
        mock_introspection = MagicMock()
        mock_type = MagicMock()
        mock_type.typcategory = "C"

        mock_composite_type = MagicMock()
        with patch('pghatch.introspection.pgtypes._get_composite_py_type', return_value=mock_composite_type) as mock_func:
            result = _get_py_type_by_category(mock_introspection, typ=mock_type)
            mock_func.assert_called_once_with(mock_introspection, mock_type, None)
            assert result == mock_composite_type

    def test_datetime_category(self):
        """Test date/time type category mapping."""
        mock_introspection = MagicMock()
        mock_type = MagicMock()
        mock_type.typcategory = "D"

        with patch('pghatch.introspection.pgtypes._get_datetime_type', return_value=datetime.datetime) as mock_func:
            result = _get_py_type_by_category(mock_introspection, typ=mock_type)
            mock_func.assert_called_once_with(mock_introspection, mock_type, None)
            assert result == datetime.datetime

    def test_enum_category(self):
        """Test enum type category mapping."""
        mock_introspection = MagicMock()
        mock_type = MagicMock()
        mock_type.typcategory = "E"

        mock_enum_type = MagicMock()
        with patch('pghatch.introspection.pgtypes._get_enum_py_type', return_value=mock_enum_type) as mock_func:
            result = _get_py_type_by_category(mock_introspection, typ=mock_type)
            mock_func.assert_called_once_with(mock_introspection, mock_type, None)
            assert result == mock_enum_type

    def test_geometric_category(self):
        """Test geometric type category mapping."""
        mock_introspection = MagicMock()
        mock_type = MagicMock()
        mock_type.typcategory = "G"

        with patch('pghatch.introspection.pgtypes._get_geometrics_py_type', return_value=Point) as mock_func:
            result = _get_py_type_by_category(mock_introspection, typ=mock_type)
            mock_func.assert_called_once_with(mock_introspection, mock_type, None)
            assert result == Point

    def test_network_category(self):
        """Test network address type category mapping."""
        mock_introspection = MagicMock()
        mock_type = MagicMock()
        mock_type.typcategory = "I"

        with patch('pghatch.introspection.pgtypes._get_network_py_type', return_value=str) as mock_func:
            result = _get_py_type_by_category(mock_introspection, typ=mock_type)
            mock_func.assert_called_once_with(mock_introspection, mock_type, None)
            assert result == str

    def test_numeric_category(self):
        """Test numeric type category mapping."""
        mock_introspection = MagicMock()
        mock_type = MagicMock()
        mock_type.typcategory = "N"

        with patch('pghatch.introspection.pgtypes._get_numeric_py_type', return_value=int) as mock_func:
            result = _get_py_type_by_category(mock_introspection, typ=mock_type)
            mock_func.assert_called_once_with(mock_introspection, mock_type, None)
            assert result == int

    def test_pseudo_category(self):
        """Test pseudo type category mapping."""
        mock_introspection = MagicMock()
        mock_type = MagicMock()
        mock_type.typcategory = "P"

        with patch('pghatch.introspection.pgtypes._get_pseudo_py_type', return_value=str) as mock_func:
            result = _get_py_type_by_category(mock_introspection, typ=mock_type)
            mock_func.assert_called_once_with(mock_introspection, mock_type, None)
            assert result == str

    def test_range_category(self):
        """Test range type category mapping."""
        mock_introspection = MagicMock()
        mock_type = MagicMock()
        mock_type.typcategory = "R"

        with patch('pghatch.introspection.pgtypes._get_range_py_type', return_value=Tuple[int]) as mock_func:
            result = _get_py_type_by_category(mock_introspection, typ=mock_type)
            mock_func.assert_called_once_with(mock_introspection, mock_type, None)
            assert result == Tuple[int]

    def test_string_category(self):
        """Test string type category mapping."""
        mock_introspection = MagicMock()
        mock_type = MagicMock()
        mock_type.typcategory = "S"

        with patch('pghatch.introspection.pgtypes._get_string_py_type', return_value=str) as mock_func:
            result = _get_py_type_by_category(mock_introspection, typ=mock_type)
            mock_func.assert_called_once_with(mock_introspection, mock_type, None)
            assert result == str

    def test_timespan_category(self):
        """Test timespan type category mapping."""
        mock_introspection = MagicMock()
        mock_type = MagicMock()
        mock_type.typcategory = "T"

        with patch('pghatch.introspection.pgtypes._get_timespan_py_type', return_value=Interval) as mock_func:
            result = _get_py_type_by_category(mock_introspection, typ=mock_type)
            mock_func.assert_called_once_with(mock_introspection, mock_type, None)
            assert result == Interval

    def test_user_defined_category(self):
        """Test user-defined type category mapping."""
        mock_introspection = MagicMock()
        mock_type = MagicMock()
        mock_type.typcategory = "U"

        with patch('pghatch.introspection.pgtypes._get_user_defined_py_type', return_value=dict) as mock_func:
            result = _get_py_type_by_category(mock_introspection, typ=mock_type)
            mock_func.assert_called_once_with(mock_introspection, mock_type, None)
            assert result == dict

    def test_bit_string_category(self):
        """Test bit-string type category mapping."""
        mock_introspection = MagicMock()
        mock_type = MagicMock()
        mock_type.typcategory = "V"

        with patch('pghatch.introspection.pgtypes._get_bitstring_py_type', return_value=str) as mock_func:
            result = _get_py_type_by_category(mock_introspection, typ=mock_type)
            mock_func.assert_called_once_with(mock_introspection, mock_type, None)
            assert result == str

    def test_unknown_category(self):
        """Test unknown type category mapping."""
        mock_introspection = MagicMock()
        mock_type = MagicMock()
        mock_type.typcategory = "X"

        with patch('pghatch.introspection.pgtypes._get_unknown_py_type', return_value=str) as mock_func:
            result = _get_py_type_by_category(mock_introspection, typ=mock_type)
            mock_func.assert_called_once_with(mock_introspection, mock_type, None)
            assert result == str

    def test_unsupported_category(self):
        """Test unsupported type category raises error."""
        mock_introspection = MagicMock()
        mock_type = MagicMock()
        mock_type.typcategory = "Z"  # Non-existent category
        mock_type.typname = "unknown_type"

        with pytest.raises(TypeError, match="Unsupported type: unknown_type"):
            _get_py_type_by_category(mock_introspection, typ=mock_type)


class TestSpecificTypeHandlers:
    """Test specific type handler functions."""

    def test_geometric_types(self):
        """Test geometric type mapping."""
        from pghatch.introspection.pgtypes import _get_geometrics_py_type

        mock_introspection = MagicMock()

        # Test point type
        mock_type = MagicMock()
        mock_type.typname = "point"
        result = _get_geometrics_py_type(mock_introspection, typ=mock_type)
        assert result == Point

        # Test line type
        mock_type.typname = "line"
        result = _get_geometrics_py_type(mock_introspection, typ=mock_type)
        assert result == Line

        # Test line segment type
        mock_type.typname = "lseg"
        result = _get_geometrics_py_type(mock_introspection, typ=mock_type)
        assert result == LineSegment

        # Test box type
        mock_type.typname = "box"
        result = _get_geometrics_py_type(mock_introspection, typ=mock_type)
        assert result == Box

        # Test path type
        mock_type.typname = "path"
        result = _get_geometrics_py_type(mock_introspection, typ=mock_type)
        assert result == Path

        # Test polygon type
        mock_type.typname = "polygon"
        result = _get_geometrics_py_type(mock_introspection, typ=mock_type)
        assert result == Polygon

        # Test circle type
        mock_type.typname = "circle"
        result = _get_geometrics_py_type(mock_introspection, typ=mock_type)
        assert result == Circle

        # Test unsupported geometric type
        mock_type.typname = "unsupported_geo"
        with pytest.raises(TypeError, match="Unsupported geometric type: unsupported_geo"):
            _get_geometrics_py_type(mock_introspection, typ=mock_type)

    def test_numeric_types(self):
        """Test numeric type mapping."""
        from pghatch.introspection.pgtypes import _get_numeric_py_type

        mock_introspection = MagicMock()

        # Test integer types
        integer_types = ["integer", "int", "int2", "int4", "int8", "smallint", "bigint"]
        for type_name in integer_types:
            mock_type = MagicMock()
            mock_type.typname = type_name
            result = _get_numeric_py_type(mock_introspection, typ=mock_type)
            assert result == int

        # Test float types
        float_types = ["float", "float4", "float8", "double precision", "real"]
        for type_name in float_types:
            mock_type = MagicMock()
            mock_type.typname = type_name
            result = _get_numeric_py_type(mock_introspection, typ=mock_type)
            assert result == float

        # Test decimal types
        decimal_types = ["numeric", "decimal"]
        for type_name in decimal_types:
            mock_type = MagicMock()
            mock_type.typname = type_name
            result = _get_numeric_py_type(mock_introspection, typ=mock_type)
            assert result == float

        # Test unsupported numeric type
        mock_type = MagicMock()
        mock_type.typname = "unsupported_numeric"
        with pytest.raises(TypeError, match="Unsupported geometric type: unsupported_numeric"):
            _get_numeric_py_type(mock_introspection, typ=mock_type)

    def test_string_types(self):
        """Test string type mapping."""
        from pghatch.introspection.pgtypes import _get_string_py_type

        mock_introspection = MagicMock()

        # Test string types
        string_types = ["text", "varchar", "char", "character varying", "character"]
        for type_name in string_types:
            mock_type = MagicMock()
            mock_type.typname = type_name
            result = _get_string_py_type(mock_introspection, typ=mock_type)
            assert result == str

        # Test bytea type
        mock_type = MagicMock()
        mock_type.typname = "bytea"
        result = _get_string_py_type(mock_introspection, typ=mock_type)
        assert result == bytes

    def test_timespan_types(self):
        """Test timespan type mapping."""
        from pghatch.introspection.pgtypes import _get_timespan_py_type

        mock_introspection = MagicMock()

        # Test interval type
        mock_type = MagicMock()
        mock_type.typname = "interval"
        result = _get_timespan_py_type(mock_introspection, typ=mock_type)
        assert result == Interval

        # Test datetime types
        datetime_types = [
            "date", "timestamp", "timestamp without time zone",
            "timestamp with time zone", "time", "time without time zone",
            "time with time zone"
        ]
        for type_name in datetime_types:
            mock_type = MagicMock()
            mock_type.typname = type_name
            result = _get_timespan_py_type(mock_introspection, typ=mock_type)
            assert result == datetime.datetime

        # Test unsupported timespan type
        mock_type = MagicMock()
        mock_type.typname = "unsupported_time"
        with pytest.raises(TypeError, match="Unsupported timespan type: unsupported_time"):
            _get_timespan_py_type(mock_introspection, typ=mock_type)

    def test_user_defined_types(self):
        """Test user-defined type mapping."""
        from pghatch.introspection.pgtypes import _get_user_defined_py_type

        mock_introspection = MagicMock()

        # Test JSON types
        json_types = ["json", "jsonb"]
        for type_name in json_types:
            mock_type = MagicMock()
            mock_type.typname = type_name
            result = _get_user_defined_py_type(mock_introspection, typ=mock_type)
            # Should return Json type from pydantic
            assert hasattr(result, '__name__')

    def test_enum_type_creation(self):
        """Test enum type creation."""
        from pghatch.introspection.pgtypes import _get_enum_py_type

        mock_introspection = MagicMock()

        # Mock enum values
        mock_enum1 = MagicMock()
        mock_enum1.enumlabel = "active"
        mock_enum2 = MagicMock()
        mock_enum2.enumlabel = "inactive"
        mock_enum3 = MagicMock()
        mock_enum3.enumlabel = "pending"

        mock_type = MagicMock()
        mock_type.typname = "status"
        mock_type.get_enum_values.return_value = [mock_enum1, mock_enum2, mock_enum3]

        mock_attr = MagicMock()
        mock_attr.attname = "user_status"

        result = _get_enum_py_type(mock_introspection, typ=mock_type, attr=mock_attr)

        # Should return an Enum class
        assert issubclass(result, Enum)
        assert hasattr(result, 'active')
        assert hasattr(result, 'inactive')
        assert hasattr(result, 'pending')


class TestGetPyType:
    """Test the main get_py_type function."""

    def test_get_py_type_with_type_only(self):
        """Test get_py_type with only type parameter."""
        mock_introspection = MagicMock()
        mock_type = MagicMock()
        mock_type.typcategory = "N"
        mock_type.typname = "int4"
        mock_type.typnotnull = True

        with patch('pghatch.introspection.pgtypes._get_py_type_by_category', return_value=int):
            result = get_py_type(mock_introspection, typ=mock_type)
            assert result == int

    def test_get_py_type_with_attribute_only(self):
        """Test get_py_type with only attribute parameter."""
        mock_introspection = MagicMock()
        mock_type = MagicMock()
        mock_type.typcategory = "S"
        mock_type.typname = "text"
        mock_type.typnotnull = False

        mock_attr = MagicMock()
        mock_attr.get_type.return_value = mock_type
        mock_attr.attnotnull = False

        with patch('pghatch.introspection.pgtypes._get_py_type_by_category', return_value=str):
            result = get_py_type(mock_introspection, attr=mock_attr)
            # Should be nullable string type
            assert hasattr(result, '__args__')
            assert str in result.__args__
            assert type(None) in result.__args__

    def test_get_py_type_with_both_parameters(self):
        """Test get_py_type with both type and attribute parameters."""
        mock_introspection = MagicMock()
        mock_type = MagicMock()
        mock_type.typcategory = "N"
        mock_type.typname = "int4"
        mock_type.typnotnull = True

        mock_attr = MagicMock()
        mock_attr.attnotnull = True

        with patch('pghatch.introspection.pgtypes._get_py_type_by_category', return_value=int):
            result = get_py_type(mock_introspection, typ=mock_type, attr=mock_attr)
            assert result == int

    def test_get_py_type_nullable_override(self):
        """Test that attribute nullability overrides type nullability."""
        mock_introspection = MagicMock()
        mock_type = MagicMock()
        mock_type.typcategory = "N"
        mock_type.typname = "int4"
        mock_type.typnotnull = True  # Type is not null

        mock_attr = MagicMock()
        mock_attr.attnotnull = False  # But attribute allows null

        with patch('pghatch.introspection.pgtypes._get_py_type_by_category', return_value=int):
            result = get_py_type(mock_introspection, typ=mock_type, attr=mock_attr)
            # Should be nullable because attribute allows null
            assert hasattr(result, '__args__')
            assert int in result.__args__
            assert type(None) in result.__args__


@pytest.mark.integration
class TestPgTypesIntegration:
    """Integration tests for PostgreSQL type mapping with real database."""

    @pytest.mark.asyncio
    async def test_real_type_mapping(self, introspection):
        """Test type mapping with real introspection data."""
        # Find some common types
        int_type = None
        text_type = None
        bool_type = None

        for typ in introspection.types:
            if typ.typname == "int4":
                int_type = typ
            elif typ.typname == "text":
                text_type = typ
            elif typ.typname == "bool":
                bool_type = typ

        # Test integer type mapping
        if int_type:
            result = get_py_type(introspection, typ=int_type)
            assert result == int

        # Test text type mapping
        if text_type:
            result = get_py_type(introspection, typ=text_type)
            assert result == str

        # Test boolean type mapping
        if bool_type:
            result = get_py_type(introspection, typ=bool_type)
            assert result == bool

    @pytest.mark.asyncio
    async def test_array_type_mapping(self, introspection):
        """Test array type mapping with real introspection data."""
        # Find an array type
        array_type = None
        for typ in introspection.types:
            if typ.typname.endswith("[]") or typ.typcategory == "A":
                array_type = typ
                break

        if array_type:
            result = get_py_type(introspection, typ=array_type)
            # Should be a List type
            assert hasattr(result, '__origin__')
            assert result.__origin__ == list

    @pytest.mark.asyncio
    async def test_enum_type_mapping(self, introspection):
        """Test enum type mapping with real introspection data."""
        # Find the test enum type
        enum_type = None
        for typ in introspection.types:
            if typ.typname == "user_status" and typ.typcategory == "E":
                enum_type = typ
                break

        if enum_type:
            result = get_py_type(introspection, typ=enum_type)
            # Should be an Enum class
            assert issubclass(result, Enum)
            # Should have the expected enum values
            enum_values = [e.name for e in result]
            expected_values = ["pending", "active", "suspended", "deleted"]
            for expected in expected_values:
                assert expected in enum_values

    @pytest.mark.asyncio
    async def test_composite_type_mapping(self, introspection):
        """Test composite type mapping with real introspection data."""
        # Find the test composite type
        composite_type = None
        for typ in introspection.types:
            if typ.typname == "address" and typ.typcategory == "C":
                composite_type = typ
                break

        if composite_type:
            result = get_py_type(introspection, typ=composite_type)
            # Should be a Pydantic model
            assert hasattr(result, 'model_fields')
            # Should have the expected fields
            expected_fields = ["street", "city", "state", "zip_code"]
            for field in expected_fields:
                assert field in result.model_fields
