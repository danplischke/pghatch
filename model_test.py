from typing import Union, Literal, Annotated

from pydantic import BaseModel, Field


class BaseCondition(BaseModel):
    field: str

class NameEQCondition(BaseCondition):
    field: Literal['name'] = 'name'
    operator: Literal['='] = '='
    value: str

class NameNECondition(BaseCondition):
    field: Literal['name'] = 'name'
    operator: Literal['!='] = '!='
    value: str

class AgeGTCondition(BaseCondition):
    field: Literal['age'] = 'age'
    operator: Literal['>'] = '>'
    value: int


NameCondition = Annotated[Union[NameEQCondition, NameNECondition], Field(discriminator='operator')]
AgeCondition = Annotated[Union[AgeGTCondition], Field(discriminator='operator')]
Condition = Annotated[Union[NameCondition, AgeCondition], Field(discriminator='field')]

class Query(BaseModel):
    where: list[Condition] = []
    limit: Union[int, None] = None
    offset: Union[int, None] = None

if __name__ == '__main__':
    print(Query.model_validate(dict(where=[dict(field='friend', operator='=', value='John')]),))


