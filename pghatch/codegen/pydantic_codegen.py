import ast
import typing
from typing import Callable, Any, Literal, Pattern

from pydantic import BaseModel, AliasChoices, AliasPath
from pydantic.fields import _Unset, PydanticUndefined
import annotated_types
from pydantic.config import JsonDict
from pydantic.fields import FieldInfo, Deprecated
from pydantic.types import Discriminator

from pghatch.codegen.utils import class_def, pydantic_field, ann_assign, name_, tuple_, str_


class FieldParams(BaseModel):
    default: str | None = None
    required: bool = True
    default: Any = PydanticUndefined
    default_factory: Callable[[], Any] | Callable[[dict[str, Any]], Any] | None = _Unset
    alias: str | None = _Unset
    alias_priority: int | None = _Unset
    validation_alias: str | AliasPath | AliasChoices | None = _Unset
    serialization_alias: str | None = _Unset
    title: str | None = _Unset
    field_title_generator: Callable[[str, FieldInfo], str] | None = _Unset
    description: str | None = _Unset
    examples: list[Any] | None = _Unset
    exclude: bool | None = _Unset
    discriminator: str | Discriminator | None = _Unset
    deprecated: Deprecated | str | bool | None = _Unset
    json_schema_extra: JsonDict | Callable[[JsonDict], None] | None = _Unset
    frozen: bool | None = _Unset
    validate_default: bool | None = _Unset
    repr: bool = _Unset
    init: bool | None = _Unset
    init_var: bool | None = _Unset
    kw_only: bool | None = _Unset
    pattern: str | Pattern[str] | None = _Unset
    strict: bool | None = _Unset
    coerce_numbers_to_str: bool | None = _Unset
    gt: annotated_types.SupportsGt | None = _Unset
    ge: annotated_types.SupportsGe | None = _Unset
    lt: annotated_types.SupportsLt | None = _Unset
    le: annotated_types.SupportsLe | None = _Unset
    multiple_of: float | None = _Unset
    allow_inf_nan: bool | None = _Unset
    max_digits: int | None = _Unset
    decimal_places: int | None = _Unset
    min_length: int | None = _Unset
    max_length: int | None = _Unset
    union_mode: Literal['smart', 'left_to_right'] = _Unset
    fail_fast: bool | None = _Unset


class FieldDefinition(BaseModel):
    name: str
    type: typing.Type
    params: FieldParams | None


def generate_pydantic_model(name: str, fields: list[FieldDefinition]) -> ast.ClassDef:
    cls = class_def(name)

    for field in fields:
        cls.body.append(ann_assign(name_(field.name), annotation=field.type,
                                   value=pydantic_field(**field.params.model_dump()) if field.params else None))

    return cls


if __name__ == '__main__':
    print(ast.unparse(ann_assign(name_("hello"), tuple_(str_()))))
