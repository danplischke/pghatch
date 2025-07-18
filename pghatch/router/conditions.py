from enum import StrEnum


class ConditionOperator(StrEnum):
    EQUAL = "="
    NOT_EQUAL = "!="
    LIKE = "LIKE"
    ILIKE = "ILIKE"
    

class ExpressionOperator(StrEnum):
    AND = "AND"
    OR = "OR"
    NOT = "NOT"

class LogicalExpression:
    operator: ExpressionOperator


class ConditionExpression:
    operator: ConditionOperator
    value: str | int | float | bool | None | list[str | int | float | bool | None]
    field: str
