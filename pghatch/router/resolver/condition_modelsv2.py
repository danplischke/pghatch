from typing import List, Union, Annotated, Literal

from pydantic import BaseModel, create_model, Field
from pydantic.alias_generators import to_pascal

from pghatch.introspection.introspection import Introspection
from pghatch.introspection.tables import PgAttribute
from pghatch.utils.model_registry import create_model


def create_range_condition_model(
        table_name: str, field: str, py_type: type
) -> type[BaseModel]:
    return create_model(
        f"{table_name}{to_pascal(field)}RangeCondition",
        condition_type=Literal["RangeCondition"],
        field=Literal[field],
        lower_bound=(py_type, None),
        upper_bound=(py_type, None),
        include_lower=(bool, True),
        include_upper=(bool, True),
    )


def create_not_range_condition_model(
        table_name: str, field: str, py_type: type
) -> type[BaseModel]:
    return create_model(
        f"{table_name}{to_pascal(field)}NotRangeCondition",
        condition_type=Literal["NotRangeCondition"],
        field=Literal[field],
        lower_bound=(py_type, None),
        upper_bound=(py_type, None),
        include_lower=(bool, True),
        include_upper=(bool, True),
    )


def create_exists_condition_model(
        table_name: str, field: str, py_type: type
) -> type[BaseModel]:
    return create_model(
        f"{table_name}{to_pascal(field)}ExistsCondition",
        condition_type=Literal["ExistsCondition"],
        field=Literal[field],
        value=(py_type, ...),
    )


def create_equal_condition_model(
        table_name: str, field: str, py_type: type
) -> type[BaseModel]:
    return create_model(
        f"{table_name}{to_pascal(field)}EqualCondition",
        condition_type=Literal["EqualCondition"],
        field=Literal[field],
        value=py_type | None,
    )


def create_not_equal_condition_model(
        table_name: str, field: str, py_type: type
) -> type[BaseModel]:
    return create_model(
        f"{table_name}{to_pascal(field)}NotEqualCondition",
        condition_type=Literal["NotEqualCondition"],
        field=Literal[field],
        value=py_type | None,
    )


def create_less_than_condition_model(
        table_name: str, field: str, py_type: type
) -> type[BaseModel]:
    return create_model(
        f"{table_name}{to_pascal(field)}LessThanCondition",
        condition_type=Literal["LessThanCondition"],
        field=Literal[field],
        value=(py_type, ...),
    )


def create_greater_than_condition_model(
        table_name: str, field: str, py_type: type
) -> type[BaseModel]:
    return create_model(
        f"{table_name}{to_pascal(field)}GreaterThanCondition",
        condition_type=Literal["GreaterThanCondition"],
        field=Literal[field],
        value=(py_type, ...),
    )


def create_like_condition_model(
        table_name: str, field: str, py_type: type
) -> type[BaseModel]:
    return create_model(
        f"{table_name}{to_pascal(field)}LikeCondition",
        condition_type=Literal["LikeCondition"],
        field=Literal[field],
        value=(py_type, ...),
    )


def create_ilike_condition_model(
        table_name: str, field: str, py_type: type
) -> type[BaseModel]:
    return create_model(
        f"{table_name}{to_pascal(field)}ILikeCondition",
        condition_type=Literal["ILikeCondition"],
        field=Literal[field],
        value=(py_type, ...),
    )


def create_in_condition_model(
        table_name: str, field: str, py_type: type
) -> type[BaseModel]:
    return create_model(
        f"{table_name}{to_pascal(field)}InCondition",
        condition_type=Literal["InCondition"],
        field=Literal[field],
        values=(List[py_type], ...),
    )


def create_not_in_condition_model(
        table_name: str, field: str, py_type: type
) -> type[BaseModel]:
    return create_model(
        f"{table_name}{to_pascal(field)}NotInCondition",
        condition_type=Literal["NotInCondition"],
        field=Literal[field],
        values=(List[py_type], ...),
    )


def create_is_null_condition_model(
        table_name: str, field: str
) -> type[BaseModel]:
    return create_model(
        f"{table_name}{to_pascal(field)}IsNullCondition",
        condition_type=Literal["IsNullCondition"],
        field=Literal[field]
    )


def create_is_not_null_condition_model(
        table_name: str, field: str
) -> type[BaseModel]:
    return create_model(
        f"{table_name}{to_pascal(field)}IsNotNullCondition",
        field=Literal[field],
        condition_type=Literal["IsNotNullCondition"],
    )


def get_numeric_condition_models(table_view_name: str, py_type: type, field_name: str, is_nullable: bool) -> List[
    type[BaseModel]]:
    return [
        create_equal_condition_model(table_view_name, field_name, py_type),
        create_not_equal_condition_model(table_view_name, field_name, py_type),
        create_less_than_condition_model(table_view_name, field_name, py_type),
        create_greater_than_condition_model(table_view_name, field_name, py_type),
        create_like_condition_model(table_view_name, field_name, py_type),
        create_ilike_condition_model(table_view_name, field_name, py_type),
        create_in_condition_model(table_view_name, field_name, py_type),
        create_not_in_condition_model(table_view_name, field_name, py_type)
    ]


def get_array_condition_models(table_view_name: str, py_type: type, field_name: str, is_nullable: bool) -> List[
    type[BaseModel]]:
    return [
        create_equal_condition_model(table_view_name, field_name, py_type),
        create_not_equal_condition_model(table_view_name, field_name, py_type),
        create_in_condition_model(table_view_name, field_name, py_type),
        create_not_in_condition_model(table_view_name, field_name, py_type),
    ]


def get_boolean_condition_models(table_view_name: str, py_type: type, field_name: str, is_nullable: bool) -> List[
    type[BaseModel]]:
    return [
        create_equal_condition_model(table_view_name, field_name, py_type),
        create_not_equal_condition_model(table_view_name, field_name, py_type),
    ]


def get_datetime_condition_models(table_view_name: str, py_type: type, field_name: str, is_nullable: bool) -> List[
    type[BaseModel]]:
    return [
        create_equal_condition_model(table_view_name, field_name, py_type),
        create_not_equal_condition_model(table_view_name, field_name, py_type),
        create_less_than_condition_model(table_view_name, field_name, py_type),
        create_greater_than_condition_model(table_view_name, field_name, py_type),
        create_like_condition_model(table_view_name, field_name, py_type),
        create_ilike_condition_model(table_view_name, field_name, py_type),
        create_in_condition_model(table_view_name, field_name, py_type),
        create_not_in_condition_model(table_view_name, field_name, py_type),
    ]


def get_enum_condition_models(table_view_name: str, py_type: type, field_name: str, is_nullable: bool) -> List[
    type[BaseModel]]:
    return [
        create_equal_condition_model(table_view_name, field_name, py_type),
        create_not_equal_condition_model(table_view_name, field_name, py_type),
        create_in_condition_model(table_view_name, field_name, py_type),
        create_not_in_condition_model(table_view_name, field_name, py_type),
    ]


def get_geometrics_condition_models(table_view_name: str, py_type: type, field_name: str, is_nullable: bool) -> List[
    type[BaseModel]]:
    return [
        create_equal_condition_model(table_view_name, field_name, py_type),
        create_not_equal_condition_model(table_view_name, field_name, py_type),
        create_in_condition_model(table_view_name, field_name, py_type),
        create_not_in_condition_model(table_view_name, field_name, py_type),
    ]


def get_network_condition_models(table_view_name: str, py_type: type, field_name: str, is_nullable: bool) -> List[
    type[BaseModel]]:
    return [
        create_equal_condition_model(table_view_name, field_name, py_type),
        create_not_equal_condition_model(table_view_name, field_name, py_type),
        create_in_condition_model(table_view_name, field_name, py_type),
        create_not_in_condition_model(table_view_name, field_name, py_type),
    ]


def get_pseudo_condition_models(table_view_name: str, py_type: type, field_name: str, is_nullable: bool) -> List[
    type[BaseModel]]:
    return [
        create_equal_condition_model(table_view_name, field_name, py_type),
        create_not_equal_condition_model(field_name, py_type),
        create_in_condition_model(table_view_name, field_name, py_type),
        create_not_in_condition_model(table_view_name, field_name, py_type),
    ]


def get_range_condition_models(table_view_name: str, py_type: type, field_name: str, is_nullable: bool) -> List[
    type[BaseModel]]:
    return [
        create_range_condition_model(table_view_name, field_name, py_type),
        create_not_range_condition_model(table_view_name, field_name, py_type),
        create_exists_condition_model(table_view_name, field_name, py_type),
    ]


def get_string_condition_models(table_view_name: str, py_type: type, field_name: str, is_nullable: bool) -> List[
    type[BaseModel]]:
    return [
        create_equal_condition_model(table_view_name, field_name, py_type),
        create_not_equal_condition_model(table_view_name, field_name, py_type),
        create_like_condition_model(table_view_name, field_name, py_type),
        create_ilike_condition_model(table_view_name, field_name, py_type),
        create_in_condition_model(table_view_name, field_name, py_type),
        create_not_in_condition_model(table_view_name, field_name, py_type),
    ]


def get_timespan_condition_models(table_view_name: str, py_type: type, field_name: str, is_nullable: bool) -> List[
    type[BaseModel]]:
    return [
        create_equal_condition_model(table_view_name, field_name, py_type),
        create_not_equal_condition_model(table_view_name, field_name, py_type),
        create_less_than_condition_model(table_view_name, field_name, py_type),
        create_greater_than_condition_model(table_view_name, field_name, py_type),
        create_like_condition_model(table_view_name, field_name, py_type),
        create_ilike_condition_model(table_view_name, field_name, py_type),
        create_in_condition_model(table_view_name, field_name, py_type),
        create_not_in_condition_model(table_view_name, field_name, py_type),
    ]


def get_bitstring_condition_models(table_view_name: str, py_type: type, field_name: str, is_nullable: bool) -> List[
    type[BaseModel]]:
    return [
        create_equal_condition_model(table_view_name, field_name, py_type),
        create_not_equal_condition_model(table_view_name, field_name, py_type),
        create_in_condition_model(table_view_name, field_name, py_type),
        create_not_in_condition_model(table_view_name, field_name, py_type),
    ]


def get_and_or_condition_models(table_view_name: str, condition_models: List[type[BaseModel]]) -> tuple[
    type[BaseModel], type[BaseModel]]:
    """
    Create a model that allows combining multiple conditions with AND/OR logic.
    """

    and_model_name = f"{table_view_name}AndCondition"
    and_model = create_model(
        and_model_name,
        conditions=(Union[*condition_models, and_model_name], ...),
    )

    or_model_name = f"{table_view_name}OrCondition"
    or_model = create_model(
        or_model_name,
        conditions=(Union[*condition_models, or_model_name], ...),
    )

    # Update forward references
    and_model_rebuild = and_model.model_rebuild()
    or_model_rebuild = or_model.model_rebuild()

    if not all([and_model_rebuild or and_model_rebuild is None, or_model_rebuild or or_model_rebuild is None]):
        raise ValueError("Failed to rebuild AND/OR condition models.")

    return and_model, or_model


def get_conditions_for_attribute(table_view_name: str, attr: PgAttribute, introspection: Introspection) -> list[type[
    BaseModel]] | None:
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
    typ = attr.get_py_type_not_nullable(introspection)
    field_name = attr.attname
    is_nullable = attr.is_nullable()
    print(attr.attname)
    match attr.get_type(introspection).typcategory:
        case "A":
            return get_array_condition_models(table_view_name, typ, field_name, is_nullable)
        case "B":
            return get_boolean_condition_models(table_view_name, typ, field_name, is_nullable)
        case "C":
            return []
        case "D":
            return get_datetime_condition_models(table_view_name, typ, field_name, is_nullable)
        case "E":
            return get_enum_condition_models(table_view_name, typ, field_name, is_nullable)
        case "G":
            return get_geometrics_condition_models(table_view_name, typ, field_name, is_nullable)
        case "I":
            return get_network_condition_models(table_view_name, typ, field_name, is_nullable)
        case "N":
            return get_numeric_condition_models(table_view_name, typ, field_name, is_nullable)
        case "P":
            raise NotImplementedError()
        case "R":
            return get_range_condition_models(table_view_name, typ, field_name, is_nullable)
        case "S":
            return get_string_condition_models(table_view_name, typ, field_name, is_nullable)
        case "T":
            return get_timespan_condition_models(table_view_name, typ, field_name, is_nullable)
        case "U":
            raise NotImplementedError()
        case "V":
            return get_bitstring_condition_models(table_view_name, typ, field_name, is_nullable)
        case "X":
            raise NotImplementedError()
    return list()


def create_field_condition_models(
        table_view_name: str,
        attr: PgAttribute,
        introspection: Introspection
):
    discriminator = Field(discriminator='operator')
    conditions = get_conditions_for_attribute(table_view_name, attr, introspection)

    return Annotated[Union[*conditions], discriminator]


def create_table_view_condition_model(table_view_oid: str, introspection: Introspection) -> type[BaseModel] | None:
    table_view_name = introspection.get_class(table_view_oid).relname
    table_view_name = to_pascal(table_view_name)

    conditions = list()
    for attr in introspection.get_attributes(table_view_oid):
        if attr.attisdropped or attr.attnum <= 0:
            continue
        conditions.extend(get_conditions_for_attribute(table_view_name, attr, introspection))

    if len(conditions) == 0:
        return None

    and_model, or_model = get_and_or_condition_models(table_view_name, conditions)
    return create_model(
        f"{table_view_name}Condition",
        where=Union[*conditions, and_model, or_model],
        order_by=(str, None),
        limit=(int, None),
        offset=(int, None),
    )
