# Static mapping of PostgreSQL types to Python types, with custom classes for complex types.
from typing import Any, Dict, Optional, Tuple, List, Union

# --- Custom Classes for Complex Types ---

class Interval:
    def __init__(self, years: int = 0, months: int = 0, days: int = 0, hours: int = 0, minutes: int = 0, seconds: float = 0.0):
        self.years = years
        self.months = months
        self.days = days
        self.hours = hours
        self.minutes = minutes
        self.seconds = seconds

    def __repr__(self):
        return f"Interval(years={self.years}, months={self.months}, days={self.days}, hours={self.hours}, minutes={self.minutes}, seconds={self.seconds})"

class Point:
    def __init__(self, x: float, y: float):
        self.x = x
        self.y = y

    def __repr__(self):
        return f"Point(x={self.x}, y={self.y})"

class Line:
    def __init__(self, a: 'Point', b: 'Point'):
        self.a = a
        self.b = b

    def __repr__(self):
        return f"Line(a={self.a}, b={self.b})"

class LineSegment:
    def __init__(self, a: 'Point', b: 'Point'):
        self.a = a
        self.b = b

    def __repr__(self):
        return f"LineSegment(a={self.a}, b={self.b})"

class Box:
    def __init__(self, a: 'Point', b: 'Point'):
        self.a = a
        self.b = b

    def __repr__(self):
        return f"Box(a={self.a}, b={self.b})"

class Path:
    def __init__(self, points: List['Point'], is_open: Optional[bool] = None):
        self.points = points
        self.is_open = is_open

    def __repr__(self):
        return f"Path(points={self.points}, is_open={self.is_open})"

class Polygon:
    def __init__(self, points: List['Point']):
        self.points = points

    def __repr__(self):
        return f"Polygon(points={self.points})"

class Circle:
    def __init__(self, center: 'Point', radius: float):
        self.center = center
        self.radius = radius

    def __repr__(self):
        return f"Circle(center={self.center}, radius={self.radius})"

# --- Type Mapping ---

PG_TO_PYTHON_TYPE = {
    # Basic types
    "integer": int,
    "int": int,
    "int2": int,
    "int4": int,
    "int8": int,
    "smallint": int,
    "bigint": int,
    "serial": int,
    "bigserial": int,
    "float": float,
    "float4": float,
    "float8": float,
    "double precision": float,
    "real": float,
    "numeric": float,
    "decimal": float,
    "money": float,
    "bool": bool,
    "boolean": bool,
    "text": str,
    "varchar": str,
    "char": str,
    "character varying": str,
    "character": str,
    "uuid": str,
    "date": str,
    "timestamp": str,
    "timestamp without time zone": str,
    "timestamp with time zone": str,
    "time": str,
    "time without time zone": str,
    "time with time zone": str,
    "interval": Interval,
    "json": dict,
    "jsonb": dict,
    "bytea": bytes,
    "bit": str,
    "bit varying": str,
    "xml": str,
    "cidr": str,
    "inet": str,
    "macaddr": str,
    "macaddr8": str,
    "tsvector": str,
    "tsquery": str,
    "uuid": str,
    "hstore": dict,  # Key-value store, can be dict[str, str|None]
    # Geometry types
    "point": Point,
    "line": Line,
    "lseg": LineSegment,
    "box": Box,
    "path": Path,
    "polygon": Polygon,
    "circle": Circle,
    # OID types
    "regproc": int,
    "regprocedure": int,
    "regoper": int,
    "regoperator": int,
    "regclass": int,
    "regtype": int,
    "regrole": int,
    "regnamespace": int,
    "regconfig": int,
    "regdictionary": int,
    # Custom
    "base64encodedbinary": bytes,
    "keyvaluehash": dict,
}

def pgtype_to_pytype(pg_type: str):
    """
    Returns the Python type or class for a given PostgreSQL type name.
    """
    return PG_TO_PYTHON_TYPE.get(pg_type.lower(), str)  # Default to str

