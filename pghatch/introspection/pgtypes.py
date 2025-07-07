import datetime
from types import UnionType
from typing import Optional, Tuple, List, Callable, cast

from pydantic import Field, create_model, model_validator
from pydantic.alias_generators import to_camel
from pghatch.introspection.introspection import Introspection
from pghatch.introspection.tables import PgType, PgAttribute

__all__ = (
    "get_py_type",
    "Interval",
    "Point",
    "Line",
    "LineSegment",
    "Box",
    "Path",
    "Polygon",
    "Circle",
)


# --- Custom Classes for Complex Types ---
class Interval:
    def __init__(
        self,
        years: int = 0,
        months: int = 0,
        days: int = 0,
        hours: int = 0,
        minutes: int = 0,
        seconds: float = 0.0,
    ):
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
    def __init__(self, a: "Point", b: "Point"):
        self.a = a
        self.b = b

    def __repr__(self):
        return f"Line(a={self.a}, b={self.b})"


class LineSegment:
    def __init__(self, a: "Point", b: "Point"):
        self.a = a
        self.b = b

    def __repr__(self):
        return f"LineSegment(a={self.a}, b={self.b})"


class Box:
    def __init__(self, a: "Point", b: "Point"):
        self.a = a
        self.b = b

    def __repr__(self):
        return f"Box(a={self.a}, b={self.b})"


class Path:
    def __init__(self, points: List["Point"], is_open: Optional[bool] = None):
        self.points = points
        self.is_open = is_open

    def __repr__(self):
        return f"Path(points={self.points}, is_open={self.is_open})"


class Polygon:
    def __init__(self, points: List["Point"]):
        self.points = points

    def __repr__(self):
        return f"Polygon(points={self.points})"


class Circle:
    def __init__(self, center: "Point", radius: float):
        self.center = center
        self.radius = radius

    def __repr__(self):
        return f"Circle(center={self.center}, radius={self.radius})"


def _get_nullable_type(
    typ: type,
    pg_type: Optional["PgType"] = None,
    attr: Optional["PgAttribute"] | None = None,
) -> UnionType | type:
    if (attr is not None and not attr.attnotnull) or (
        pg_type is not None and not pg_type.typnotnull
    ):
        return typ | None
    return typ


def _get_array_type(
    introspection: "Introspection",
    typ: Optional["PgType"] = None,
    attr: Optional["PgAttribute"] | None = None,
) -> type:
    """
    Returns the Python type for a PostgreSQL array based on the element type.
    """
    if typ and not attr:
        elem_type = typ.get_elem_type(introspection)
        dims = typ.typndims
    else:
        typ = attr.get_type(introspection)
        elem_type = typ.get_elem_type(introspection)
        dims = attr.attndims

    py_type = get_py_type(introspection=introspection, typ=elem_type)

    dims = 1 if not dims else dims
    for idx in range(dims):
        py_type = List[py_type]

    return py_type


def _get_composite_type(
    introspection: "Introspection",
    typ: Optional["PgType"] = None,
    attr: Optional["PgAttribute"] | None = None,
) -> type:
    """
    Returns the Python type for a PostgreSQL composite type.
    """
    if attr is not None:
        typ = attr.get_type(introspection)

    relation = introspection.get_class(typ.typrelid)
    if relation is None:
        raise ValueError(
            f"Relation for type {typ.typname} not found in introspection data."
        )

    attrs = relation.get_attributes(introspection)
    field_definitions = {}
    for attr in attrs:
        if attr.attisdropped:
            continue
        py_type = get_py_type(
            introspection=introspection, typ=attr.atttypid, attr=attr
        )
        field_definitions[attr.attname] = (
            py_type,
            Field(description=attr.get_description(introspection)),
        )
    return create_model(
        to_camel(relation.relname),
        **field_definitions,
        __validators__={
            "from_record": cast(
                Callable, model_validator(mode="before")(lambda row: dict(row))
            )
        },
    )


# Array type
def _get_array_py_type(
    introspection: "Introspection",
    typ: Optional["PgType"] = None,
    attr: Optional["PgAttribute"] | None = None,
) -> type:
    return _get_array_type(introspection, typ, attr)


# Boolean type
def _get_boolean_py_type(
    introspection: "Introspection",
    typ: Optional["PgType"] = None,
    attr: Optional["PgAttribute"] | None = None,
) -> type:
    return bool


# Composite type
def _get_composite_py_type(
    introspection: "Introspection",
    typ: Optional["PgType"] = None,
    attr: Optional["PgAttribute"] | None = None,
) -> type:
    return _get_composite_type(introspection, typ, attr)


# Date/Time type
def _get_datetime_type(
    introspection: "Introspection",
    typ: Optional["PgType"] = None,
    attr: Optional["PgAttribute"] | None = None,
) -> type:
    return datetime.datetime


# Enum type
def _get_enum_py_type(
    introspection: "Introspection",
    typ: Optional["PgType"] = None,
    attr: Optional["PgAttribute"] | None = None,
) -> type:
    from enum import Enum

    enums = typ.get_enum_values(introspection)
    labels = [en.enumlabel for en in enums]
    return Enum(attr.attname if attr else typ.typname, labels)


# Geometric type
def _get_geometrics_py_type(
    introspection: "Introspection",
    typ: Optional["PgType"] = None,
    attr: Optional["PgAttribute"] | None = None,
) -> type:
    if typ is None:
        typ = attr.get_type(introspection)

    if typ.typname == "point":
        return Point
    elif typ.typname == "line":
        return Line
    elif typ.typname == "lseg":
        return LineSegment
    elif typ.typname == "box":
        return Box
    elif typ.typname == "path":
        return Path
    elif typ.typname == "polygon":
        return Polygon
    elif typ.typname == "circle":
        return Circle
    else:
        raise TypeError(f"Unsupported geometric type: {typ.typname}")


# Network address type
def _get_network_py_type(
    introspection: "Introspection",
    typ: Optional["PgType"] = None,
    attr: Optional["PgAttribute"] | None = None,
) -> type:
    return str


# Numeric type
def _get_numeric_py_type(
    introspection: "Introspection",
    typ: Optional["PgType"] = None,
    attr: Optional["PgAttribute"] | None = None,
) -> type:
    if typ is None:
        typ = attr.get_type(introspection)

    match typ.typname:
        case "integer" | "int" | "int2" | "int4" | "int8" | "smallint" | "bigint":
            return int
        case "float" | "float4" | "float8" | "double precision" | "real":
            return float
        case "numeric" | "decimal":
            return float
    raise TypeError(f"Unsupported geometric type: {typ.typname}")


# Pseudo type
def _get_pseudo_py_type(
    introspection: "Introspection",
    typ: Optional["PgType"] = None,
    attr: Optional["PgAttribute"] | None = None,
) -> type:
    return str


# Range type
def _get_range_py_type(
    introspection: "Introspection",
    typ: Optional["PgType"] = None,
    attr: Optional["PgAttribute"] | None = None,
) -> type:
    if typ is None:
        typ = attr.get_type(introspection)

    # For range types, we can return a tuple of the element type
    elem_type = typ.get_elem_type(introspection)
    return get_py_type(introspection=introspection, typ=elem_type)


# String type
def _get_string_py_type(
    introspection: "Introspection",
    typ: Optional["PgType"] = None,
    attr: Optional["PgAttribute"] | None = None,
) -> type:
    if typ is None:
        typ = attr.get_type(introspection)

    match typ.typname:
        case "text" | "varchar" | "char" | "character varying" | "character":
            return str
        case "bytea":
            return bytes
        case _:
            return str
    raise TypeError(f"Unsupported string type: {typ.typname}")


# Timespan types
def _get_timespan_py_type(
    introspection: "Introspection",
    typ: Optional["PgType"] = None,
    attr: Optional["PgAttribute"] | None = None,
) -> type:
    if typ is None:
        typ = attr.get_type(introspection)

    if typ.typname == "interval":
        return Interval
    elif typ.typname in (
        "date",
        "timestamp",
        "timestamp without time zone",
        "timestamp with time zone",
        "time",
        "time without time zone",
        "time with time zone",
    ):
        return datetime.datetime
    raise TypeError(f"Unsupported timespan type: {typ.typname}")


# User-defined type
def _get_user_defined_py_type(
    introspection: "Introspection",
    typ: Optional["PgType"] = None,
    attr: Optional["PgAttribute"] | None = None,
) -> type:
    from pydantic import Json

    if typ is None:
        typ = attr.get_type(introspection)

    if typ.typname == "json" or typ.typname == "jsonb":
        return Json

    if typ.typname == "domain":
        # For domains, we can return the underlying type
        base_type = typ.get_base_type(introspection)
        return get_py_type(introspection=introspection, typ=base_type)

    return _get_composite_type(introspection, typ, attr)


# Bit-string types
def _get_bitstring_py_type(
    introspection: "Introspection",
    typ: Optional["PgType"] = None,
    attr: Optional["PgAttribute"] | None = None,
) -> type:
    if typ is None:
        typ = attr.get_type(introspection)

    if typ.typname in ("bit", "bit varying"):
        return str
    raise TypeError(f"Unsupported bit-string type: {typ.typname}")


# Unknown type
def _get_unknown_py_type(
    introspection: "Introspection",
    typ: Optional["PgType"] = None,
    attr: Optional["PgAttribute"] | None = None,
) -> type:
    return str


# Custom types
def _get_custom_py_type(
    introspection: "Introspection",
    typ: Optional["PgType"] = None,
    attr: Optional["PgAttribute"] | None = None,
) -> type:
    if typ is None:
        typ = attr.get_type(introspection)

    if typ.typname == "base64encodedbinary":
        return bytes
    elif typ.typname == "keyvaluehash":
        return dict
    else:
        raise TypeError(f"Unsupported custom type: {typ.typname}")


# Internal use types
def _get_internal_py_type(
    introspection: "Introspection",
    typ: Optional["PgType"] = None,
    attr: Optional["PgAttribute"] | None = None,
) -> type:
    if typ is None:
        typ = attr.get_type(introspection)

    if typ.typname in (
        "regproc",
        "regprocedure",
        "regoper",
        "regoperator",
        "regclass",
        "regtype",
        "regrole",
        "regnamespace",
        "regconfig",
        "regdictionary",
    ):
        return int
    else:
        raise TypeError(f"Unsupported internal use type: {typ.typname}")


def _get_py_type_by_category(
    introspection: "Introspection",
    typ: Optional["PgType"] = None,
    attr: Optional["PgAttribute"] | None = None,
) -> type:
    """
    Returns the Python type based on the PostgreSQL type category.
    This function is used to dispatch the correct type handler.
    """
    if attr:
        typ = attr.get_type(introspection)

    # ARRAY = 'A'           # Array types
    # BOOLEAN = 'B'         # Boolean types
    # COMPOSITE = 'C'       # Composite types
    # DATE_TIME = 'D'       # Date/time types
    # ENUM = 'E'            # Enum types
    # GEOMETRIC = 'G'       # Geometric types
    # NETWORK = 'I'         # Network address types
    # NUMERIC = 'N'         # Numeric types
    # PSEUDO = 'P'          # Pseudo-types
    # RANGE = 'R'           # Range types
    # STRING = 'S'          # String types
    # TIMES = 'T'           # Timespan types
    # USER_DEFINED = 'U'    # User-defined types (including json/jsonb)
    # BIT_STRING = 'V'      # Bit-string types
    # UNKNOWN = 'X'         # Unknown types

    match typ.typcategory:
        case "A":
            return _get_array_py_type(introspection, typ, attr)
        case "B":
            return _get_boolean_py_type(introspection, typ, attr)
        case "C":
            return _get_composite_py_type(introspection, typ, attr)
        case "D":
            return _get_datetime_type(introspection, typ, attr)
        case "E":
            return _get_enum_py_type(introspection, typ, attr)
        case "G":
            return _get_geometrics_py_type(introspection, typ, attr)
        case "I":
            return _get_network_py_type(introspection, typ, attr)
        case "N":
            return _get_numeric_py_type(introspection, typ, attr)
        case "P":
            return _get_pseudo_py_type(introspection, typ, attr)
        case "R":
            return _get_range_py_type(introspection, typ, attr)
        case "S":
            return _get_string_py_type(introspection, typ, attr)
        case "T":
            return _get_timespan_py_type(introspection, typ, attr)
        case "U":
            return _get_user_defined_py_type(introspection, typ, attr)
        case "V":
            return _get_bitstring_py_type(introspection, typ, attr)
        case "X":
            return _get_unknown_py_type(introspection, typ, attr)

    raise TypeError(f"Unsupported type: {typ.typname}")


def get_py_type(
    introspection: "Introspection",
    typ: Optional["PgType"] = None,
    attr: Optional["PgAttribute"] | None = None,
) -> (type, UnionType | type):
    return _get_nullable_type(_get_py_type_by_category(introspection, typ, attr), typ, attr)