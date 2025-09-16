from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import (
    AnyUrl,
    BaseModel,
    EmailStr,
    Field,
    PositiveFloat,
    conint,
    constr,
)


class Swagger(Enum):
    field_2_0 = '2.0'


class Contact(BaseModel):
    class Config:
        extra = "forbid"

    name: Optional[str] = Field(
        None, description='The identifying name of the contact person/organization.'
    )
    url: Optional[AnyUrl] = Field(
        None, description='The URL pointing to the contact information.'
    )
    email: Optional[EmailStr] = Field(
        None, description='The email address of the contact person/organization.'
    )


class License(BaseModel):
    class Config:
        extra = "forbid"

    name: str = Field(
        ...,
        description="The name of the license type. It's encouraged to use an OSI compatible license.",
    )
    url: Optional[AnyUrl] = Field(None, description='The URL pointing to the license.')


class ExternalDocs(BaseModel):
    class Config:
        extra = "forbid"

    description: Optional[str] = None
    url: AnyUrl


class Examples(BaseModel):
    pass

    class Config:
        extra = "allow"


class MimeType(BaseModel):
    __root__: str = Field(..., description='The MIME type of the HTTP message.')


class Type(Enum):
    string = 'string'
    number = 'number'
    integer = 'integer'
    boolean = 'boolean'
    array = 'array'


class VendorExtension(BaseModel):
    class Config:
        extra = "allow"

    __root__: Any = Field(..., description='Any property starting with x- is valid.')


class In(Enum):
    body = 'body'


class In1(Enum):
    header = 'header'


class Type1(Enum):
    string = 'string'
    number = 'number'
    boolean = 'boolean'
    integer = 'integer'
    array = 'array'


class In2(Enum):
    query = 'query'


class In3(Enum):
    formData = 'formData'


class Type3(Enum):
    string = 'string'
    number = 'number'
    boolean = 'boolean'
    integer = 'integer'
    array = 'array'
    file = 'file'


class Required(Enum):
    boolean_True = True


class In4(Enum):
    path = 'path'


class Type4(Enum):
    string = 'string'
    number = 'number'
    boolean = 'boolean'
    integer = 'integer'
    array = 'array'


class PositiveInteger(BaseModel):
    __root__: conint(ge=0)


class PositiveIntegerDefault0(BaseModel):
    pass


class SimpleTypes(Enum):
    array = 'array'
    boolean = 'boolean'
    integer = 'integer'
    null = 'null'
    number = 'number'
    object = 'object'
    string = 'string'


class StringArray(BaseModel):
    __root__: List[str] = Field(..., min_items=1, unique_items=True)


class Type5(Enum):
    file = 'file'


class Type6(Enum):
    string = 'string'
    number = 'number'
    integer = 'integer'
    boolean = 'boolean'
    array = 'array'


class SecurityRequirement(BaseModel):
    __root__: Optional[Dict[str, List[str]]] = Field(None, unique_items=True)


class Xml(BaseModel):
    class Config:
        extra = "forbid"

    name: Optional[str] = None
    namespace: Optional[str] = None
    prefix: Optional[str] = None
    attribute: Optional[bool] = False
    wrapped: Optional[bool] = False


class Tag(BaseModel):
    class Config:
        extra = "forbid"

    name: str
    description: Optional[str] = None
    externalDocs: Optional[ExternalDocs] = None


class Type7(Enum):
    basic = 'basic'


class BasicAuthenticationSecurity(BaseModel):
    class Config:
        extra = "forbid"

    type: Type7
    description: Optional[str] = None


class Type8(Enum):
    apiKey = 'apiKey'


class In5(Enum):
    header = 'header'
    query = 'query'


class ApiKeySecurity(BaseModel):
    class Config:
        extra = "forbid"

    type: Type8
    name: str
    in_: In5 = Field(..., alias='in')
    description: Optional[str] = None


class Type9(Enum):
    oauth2 = 'oauth2'


class Flow(Enum):
    implicit = 'implicit'


class Flow1(Enum):
    password = 'password'


class Flow2(Enum):
    application = 'application'


class Flow3(Enum):
    accessCode = 'accessCode'


class Oauth2Scopes(BaseModel):
    __root__: Optional[Dict[str, str]] = None


class MediaTypeList(BaseModel):
    __root__: List[MimeType] = Field(..., unique_items=True)


class SchemesListEnum(Enum):
    http = 'http'
    https = 'https'
    ws = 'ws'
    wss = 'wss'


class SchemesList(BaseModel):
    __root__: List[SchemesListEnum] = Field(
        ..., description='The transfer protocol of the API.', unique_items=True
    )


class CollectionFormat(Enum):
    csv = 'csv'
    ssv = 'ssv'
    tsv = 'tsv'
    pipes = 'pipes'


class CollectionFormatWithMulti(Enum):
    csv = 'csv'
    ssv = 'ssv'
    tsv = 'tsv'
    pipes = 'pipes'
    multi = 'multi'


class JsonReference(BaseModel):
    class Config:
        extra = "forbid"

    field_ref: str = Field(..., alias='$ref')


class Info(BaseModel):
    class Config:
        extra = "forbid"

    title: str = Field(..., description='A unique and precise title of the API.')
    version: str = Field(..., description='A semantic version number of the API.')
    description: Optional[str] = Field(
        None,
        description='A longer description of the API. Should be different from the title.  GitHub Flavored Markdown is allowed.',
    )
    termsOfService: Optional[str] = Field(
        None, description='The terms of service for the API.'
    )
    contact: Optional[Contact] = None
    license: Optional[License] = None


class Security(BaseModel):
    __root__: List[SecurityRequirement] = Field(..., unique_items=True)


class Oauth2ImplicitSecurity(BaseModel):
    class Config:
        extra = "forbid"

    type: Type9
    flow: Flow
    scopes: Optional[Oauth2Scopes] = None
    authorizationUrl: AnyUrl
    description: Optional[str] = None


class Oauth2PasswordSecurity(BaseModel):
    class Config:
        extra = "forbid"

    type: Type9
    flow: Flow1
    scopes: Optional[Oauth2Scopes] = None
    tokenUrl: AnyUrl
    description: Optional[str] = None


class Oauth2ApplicationSecurity(BaseModel):
    class Config:
        extra = "forbid"

    type: Type9
    flow: Flow2
    scopes: Optional[Oauth2Scopes] = None
    tokenUrl: AnyUrl
    description: Optional[str] = None


class Oauth2AccessCodeSecurity(BaseModel):
    class Config:
        extra = "forbid"

    type: Type9
    flow: Flow3
    scopes: Optional[Oauth2Scopes] = None
    authorizationUrl: AnyUrl
    tokenUrl: AnyUrl
    description: Optional[str] = None


class SecurityDefinitions(BaseModel):
    __root__: Optional[
        Dict[
            str,
            Union[
                BasicAuthenticationSecurity,
                ApiKeySecurity,
                Oauth2ImplicitSecurity,
                Oauth2PasswordSecurity,
                Oauth2ApplicationSecurity,
                Oauth2AccessCodeSecurity,
            ],
        ]
    ] = None


class AJsonSchemaForSwagger20Api(BaseModel):
    class Config:
        extra = "forbid"

    swagger: Swagger = Field(..., description='The Swagger version of this document.')
    info: Info
    host: Optional[constr(pattern=r'^[^{}/ :\\]+(?::\d+)?$')] = Field(
        None, description="The host (name or ip) of the API. Example: 'swagger.io'"
    )
    basePath: Optional[constr(pattern=r'^/')] = Field(
        None, description="The base path to the API. Example: '/api'."
    )
    schemes: Optional[SchemesList] = None
    consumes: Optional[MediaTypeList] = Field(
        None, description='A list of MIME types accepted by the API.'
    )
    produces: Optional[MediaTypeList] = Field(
        None, description='A list of MIME types the API can produce.'
    )
    paths: Paths
    definitions: Optional[Definitions] = None
    parameters: Optional[ParameterDefinitions] = None
    responses: Optional[ResponseDefinitions] = None
    security: Optional[Security] = None
    securityDefinitions: Optional[SecurityDefinitions] = None
    tags: Optional[List[Tag]] = Field(None, unique_items=True)
    externalDocs: Optional[ExternalDocs] = None


class Paths(BaseModel):
    class Config:
        extra = "forbid"

    __root__: Union[
        Dict[constr(pattern=r'^x-'), VendorExtension], Dict[constr(pattern=r'^/'), PathItem]
    ] = Field(
        ...,
        description="Relative paths to the individual endpoints. They must be relative to the 'basePath'.",
    )


class Definitions(BaseModel):
    __root__: Optional[Dict[str, Schema]] = None


class ParameterDefinitions(BaseModel):
    __root__: Optional[Dict[str, Parameter]] = None


class ResponseDefinitions(BaseModel):
    __root__: Optional[Dict[str, Response]] = None


class Operation(BaseModel):
    class Config:
        extra = "forbid"

    tags: Optional[List[str]] = Field(None, unique_items=True)
    summary: Optional[str] = Field(
        None, description='A brief summary of the operation.'
    )
    description: Optional[str] = Field(
        None,
        description='A longer description of the operation, GitHub Flavored Markdown is allowed.',
    )
    externalDocs: Optional[ExternalDocs] = None
    operationId: Optional[str] = Field(
        None, description='A unique identifier of the operation.'
    )
    produces: Optional[MediaTypeList] = Field(
        None, description='A list of MIME types the API can produce.'
    )
    consumes: Optional[MediaTypeList] = Field(
        None, description='A list of MIME types the API can consume.'
    )
    parameters: Optional[ParametersList] = None
    responses: Responses
    schemes: Optional[SchemesList] = None
    deprecated: Optional[bool] = False
    security: Optional[Security] = None


class PathItem(BaseModel):
    class Config:
        extra = "forbid"

    field_ref: Optional[str] = Field(None, alias='$ref')
    get: Optional[Operation] = None
    put: Optional[Operation] = None
    post: Optional[Operation] = None
    delete: Optional[Operation] = None
    options: Optional[Operation] = None
    head: Optional[Operation] = None
    patch: Optional[Operation] = None
    parameters: Optional[ParametersList] = None


class Responses(BaseModel):
    class Config:
        extra = "forbid"

    __root__: Union[
        Dict[constr(pattern=r'^([0-9]{3})$|^(default)$'), ResponseValue],
        Dict[constr(pattern=r'^x-'), VendorExtension],
    ] = Field(
        ...,
        description="Response objects names can either be any valid HTTP status code or 'default'.",
    )


class ResponseValue(BaseModel):
    __root__: Union[Response, JsonReference]


class Response(BaseModel):
    class Config:
        extra = "forbid"

    description: str
    schema_: Optional[Union[Schema, FileSchema]] = Field(None, alias='schema')
    headers: Optional[Headers] = None
    examples: Optional[Examples] = None


class Headers(BaseModel):
    __root__: Optional[Dict[str, Header]] = None


class Header(BaseModel):
    class Config:
        extra = "forbid"

    type: Type
    format: Optional[str] = None
    items: Optional[PrimitivesItems] = None
    collectionFormat: Optional[CollectionFormat] = 'csv'
    default: Optional[Default] = None
    maximum: Optional[Maximum] = None
    exclusiveMaximum: Optional[ExclusiveMaximum] = None
    minimum: Optional[Minimum] = None
    exclusiveMinimum: Optional[ExclusiveMinimum] = None
    maxLength: Optional[MaxLength] = None
    minLength: Optional[MinLength] = None
    pattern: Optional[Pattern] = None
    maxItems: Optional[MaxItems] = None
    minItems: Optional[MinItems] = None
    uniqueItems: Optional[UniqueItems] = None
    enum: Optional[EnumModel] = None
    multipleOf: Optional[MultipleOf] = None
    description: Optional[str] = None


class BodyParameter(BaseModel):
    class Config:
        extra = "forbid"

    description: Optional[str] = Field(
        None,
        description='A brief description of the parameter. This could contain examples of use.  GitHub Flavored Markdown is allowed.',
    )
    name: str = Field(..., description='The name of the parameter.')
    in_: In = Field(
        ..., alias='in', description='Determines the location of the parameter.'
    )
    required: Optional[bool] = Field(
        False,
        description='Determines whether or not this parameter is required or optional.',
    )
    schema_: Schema = Field(..., alias='schema')


class HeaderParameterSubSchema(BaseModel):
    class Config:
        extra = "forbid"

    required: Optional[bool] = Field(
        False,
        description='Determines whether or not this parameter is required or optional.',
    )
    in_: Optional[In1] = Field(
        None, alias='in', description='Determines the location of the parameter.'
    )
    description: Optional[str] = Field(
        None,
        description='A brief description of the parameter. This could contain examples of use.  GitHub Flavored Markdown is allowed.',
    )
    name: Optional[str] = Field(None, description='The name of the parameter.')
    type: Optional[Type1] = None
    format: Optional[str] = None
    items: Optional[PrimitivesItems] = None
    collectionFormat: Optional[CollectionFormat] = 'csv'
    default: Optional[Default] = None
    maximum: Optional[Maximum] = None
    exclusiveMaximum: Optional[ExclusiveMaximum] = None
    minimum: Optional[Minimum] = None
    exclusiveMinimum: Optional[ExclusiveMinimum] = None
    maxLength: Optional[MaxLength] = None
    minLength: Optional[MinLength] = None
    pattern: Optional[Pattern] = None
    maxItems: Optional[MaxItems] = None
    minItems: Optional[MinItems] = None
    uniqueItems: Optional[UniqueItems] = None
    enum: Optional[EnumModel] = None
    multipleOf: Optional[MultipleOf] = None


class QueryParameterSubSchema(BaseModel):
    class Config:
        extra = "forbid"

    required: Optional[bool] = Field(
        False,
        description='Determines whether or not this parameter is required or optional.',
    )
    in_: Optional[In2] = Field(
        None, alias='in', description='Determines the location of the parameter.'
    )
    description: Optional[str] = Field(
        None,
        description='A brief description of the parameter. This could contain examples of use.  GitHub Flavored Markdown is allowed.',
    )
    name: Optional[str] = Field(None, description='The name of the parameter.')
    allowEmptyValue: Optional[bool] = Field(
        False,
        description='allows sending a parameter by name only or with an empty value.',
    )
    type: Optional[Type1] = None
    format: Optional[str] = None
    items: Optional[PrimitivesItems] = None
    collectionFormat: Optional[CollectionFormatWithMulti] = 'csv'
    default: Optional[Default] = None
    maximum: Optional[Maximum] = None
    exclusiveMaximum: Optional[ExclusiveMaximum] = None
    minimum: Optional[Minimum] = None
    exclusiveMinimum: Optional[ExclusiveMinimum] = None
    maxLength: Optional[MaxLength] = None
    minLength: Optional[MinLength] = None
    pattern: Optional[Pattern] = None
    maxItems: Optional[MaxItems] = None
    minItems: Optional[MinItems] = None
    uniqueItems: Optional[UniqueItems] = None
    enum: Optional[EnumModel] = None
    multipleOf: Optional[MultipleOf] = None


class FormDataParameterSubSchema(BaseModel):
    class Config:
        extra = "forbid"

    required: Optional[bool] = Field(
        False,
        description='Determines whether or not this parameter is required or optional.',
    )
    in_: Optional[In3] = Field(
        None, alias='in', description='Determines the location of the parameter.'
    )
    description: Optional[str] = Field(
        None,
        description='A brief description of the parameter. This could contain examples of use.  GitHub Flavored Markdown is allowed.',
    )
    name: Optional[str] = Field(None, description='The name of the parameter.')
    allowEmptyValue: Optional[bool] = Field(
        False,
        description='allows sending a parameter by name only or with an empty value.',
    )
    type: Optional[Type3] = None
    format: Optional[str] = None
    items: Optional[PrimitivesItems] = None
    collectionFormat: Optional[CollectionFormatWithMulti] = 'csv'
    default: Optional[Default] = None
    maximum: Optional[Maximum] = None
    exclusiveMaximum: Optional[ExclusiveMaximum] = None
    minimum: Optional[Minimum] = None
    exclusiveMinimum: Optional[ExclusiveMinimum] = None
    maxLength: Optional[MaxLength] = None
    minLength: Optional[MinLength] = None
    pattern: Optional[Pattern] = None
    maxItems: Optional[MaxItems] = None
    minItems: Optional[MinItems] = None
    uniqueItems: Optional[UniqueItems] = None
    enum: Optional[EnumModel] = None
    multipleOf: Optional[MultipleOf] = None


class PathParameterSubSchema(BaseModel):
    class Config:
        extra = "forbid"

    required: Required = Field(
        ...,
        description='Determines whether or not this parameter is required or optional.',
    )
    in_: Optional[In4] = Field(
        None, alias='in', description='Determines the location of the parameter.'
    )
    description: Optional[str] = Field(
        None,
        description='A brief description of the parameter. This could contain examples of use.  GitHub Flavored Markdown is allowed.',
    )
    name: Optional[str] = Field(None, description='The name of the parameter.')
    type: Optional[Type4] = None
    format: Optional[str] = None
    items: Optional[PrimitivesItems] = None
    collectionFormat: Optional[CollectionFormat] = 'csv'
    default: Optional[Default] = None
    maximum: Optional[Maximum] = None
    exclusiveMaximum: Optional[ExclusiveMaximum] = None
    minimum: Optional[Minimum] = None
    exclusiveMinimum: Optional[ExclusiveMinimum] = None
    maxLength: Optional[MaxLength] = None
    minLength: Optional[MinLength] = None
    pattern: Optional[Pattern] = None
    maxItems: Optional[MaxItems] = None
    minItems: Optional[MinItems] = None
    uniqueItems: Optional[UniqueItems] = None
    enum: Optional[EnumModel] = None
    multipleOf: Optional[MultipleOf] = None


class NonBodyParameter(BaseModel):
    __root__: Union[
        HeaderParameterSubSchema,
        FormDataParameterSubSchema,
        QueryParameterSubSchema,
        PathParameterSubSchema,
    ]


class Parameter(BaseModel):
    __root__: Union[BodyParameter, NonBodyParameter]


class Schema(BaseModel):
    class Config:
        extra = "forbid"

    field_ref: Optional[str] = Field(None, alias='$ref')
    format: Optional[str] = None
    title: Optional[Title] = None
    description: Optional[Description] = None
    default: Optional[Default] = None
    multipleOf: Optional[MultipleOf] = None
    maximum: Optional[Maximum] = None
    exclusiveMaximum: Optional[ExclusiveMaximum] = None
    minimum: Optional[Minimum] = None
    exclusiveMinimum: Optional[ExclusiveMinimum] = None
    maxLength: Optional[PositiveIntegerModel] = None
    minLength: Optional[PositiveIntegerDefault0Model] = None
    pattern: Optional[Pattern] = None
    maxItems: Optional[PositiveIntegerModel] = None
    minItems: Optional[PositiveIntegerDefault0Model] = None
    uniqueItems: Optional[UniqueItems] = None
    maxProperties: Optional[PositiveIntegerModel] = None
    minProperties: Optional[PositiveIntegerDefault0Model] = None
    required: Optional[StringArrayModel] = None
    enum: Optional[EnumModel] = None
    additionalProperties: Optional[Union[Schema, bool]] = {}
    type: Optional[TypeModel] = None
    items: Optional[Union[Schema, List[Schema]]] = {}
    allOf: Optional[List[Schema]] = None
    properties: Optional[Dict[str, Schema]] = {}
    discriminator: Optional[str] = None
    readOnly: Optional[bool] = False
    xml: Optional[Xml] = None
    externalDocs: Optional[ExternalDocs] = None
    example: Optional[Any] = None


class Title(BaseModel):
    id: Optional[str] = None
    field_schema: Optional[str] = Field(None, alias='$schema')
    title: Optional[str] = None
    description: Optional[str] = None
    default: Optional[Any] = None
    multipleOf: Optional[PositiveFloat] = None
    maximum: Optional[float] = None
    exclusiveMaximum: Optional[bool] = False
    minimum: Optional[float] = None
    exclusiveMinimum: Optional[bool] = False
    maxLength: Optional[PositiveInteger] = None
    minLength: Optional[PositiveIntegerDefault0] = None
    pattern: Optional[str] = None
    additionalItems: Optional[Union[bool, Title]] = {}
    items: Optional[Union[Title, SchemaArray]] = {}
    maxItems: Optional[PositiveInteger] = None
    minItems: Optional[PositiveIntegerDefault0] = None
    uniqueItems: Optional[bool] = False
    maxProperties: Optional[PositiveInteger] = None
    minProperties: Optional[PositiveIntegerDefault0] = None
    required: Optional[StringArray] = None
    additionalProperties: Optional[Union[bool, Title]] = {}
    definitions: Optional[Dict[str, Title]] = {}
    properties: Optional[Dict[str, Title]] = {}
    patternProperties: Optional[Dict[str, Title]] = {}
    dependencies: Optional[Dict[str, Union[Title, StringArray]]] = None
    enum: Optional[List] = Field(None, min_items=1, unique_items=True)
    type: Optional[Union[SimpleTypes, List[SimpleTypes]]] = None
    format: Optional[str] = None
    allOf: Optional[SchemaArray] = None
    anyOf: Optional[SchemaArray] = None
    oneOf: Optional[SchemaArray] = None
    not_: Optional[Title] = Field(None, alias='not')


class SchemaArray(BaseModel):
    __root__: List[Title] = Field(..., min_items=1)


class Description(BaseModel):
    id: Optional[str] = None
    field_schema: Optional[str] = Field(None, alias='$schema')
    title: Optional[str] = None
    description: Optional[str] = None
    default: Optional[Any] = None
    multipleOf: Optional[PositiveFloat] = None
    maximum: Optional[float] = None
    exclusiveMaximum: Optional[bool] = False
    minimum: Optional[float] = None
    exclusiveMinimum: Optional[bool] = False
    maxLength: Optional[PositiveInteger] = None
    minLength: Optional[PositiveIntegerDefault0] = None
    pattern: Optional[str] = None
    additionalItems: Optional[Union[bool, Description]] = {}
    items: Optional[Union[Description, SchemaArrayModel]] = {}
    maxItems: Optional[PositiveInteger] = None
    minItems: Optional[PositiveIntegerDefault0] = None
    uniqueItems: Optional[bool] = False
    maxProperties: Optional[PositiveInteger] = None
    minProperties: Optional[PositiveIntegerDefault0] = None
    required: Optional[StringArray] = None
    additionalProperties: Optional[Union[bool, Description]] = {}
    definitions: Optional[Dict[str, Description]] = {}
    properties: Optional[Dict[str, Description]] = {}
    patternProperties: Optional[Dict[str, Description]] = {}
    dependencies: Optional[Dict[str, Union[Description, StringArray]]] = None
    enum: Optional[List] = Field(None, min_items=1, unique_items=True)
    type: Optional[Union[SimpleTypes, List[SimpleTypes]]] = None
    format: Optional[str] = None
    allOf: Optional[SchemaArrayModel] = None
    anyOf: Optional[SchemaArrayModel] = None
    oneOf: Optional[SchemaArrayModel] = None
    not_: Optional[Description] = Field(None, alias='not')


class SchemaArrayModel(BaseModel):
    __root__: List[Description] = Field(..., min_items=1)


class Default(BaseModel):
    id: Optional[str] = None
    field_schema: Optional[str] = Field(None, alias='$schema')
    title: Optional[str] = None
    description: Optional[str] = None
    default: Optional[Any] = None
    multipleOf: Optional[PositiveFloat] = None
    maximum: Optional[float] = None
    exclusiveMaximum: Optional[bool] = False
    minimum: Optional[float] = None
    exclusiveMinimum: Optional[bool] = False
    maxLength: Optional[PositiveInteger] = None
    minLength: Optional[PositiveIntegerDefault0] = None
    pattern: Optional[str] = None
    additionalItems: Optional[Union[bool, Default]] = {}
    items: Optional[Union[Default, SchemaArrayModel1]] = {}
    maxItems: Optional[PositiveInteger] = None
    minItems: Optional[PositiveIntegerDefault0] = None
    uniqueItems: Optional[bool] = False
    maxProperties: Optional[PositiveInteger] = None
    minProperties: Optional[PositiveIntegerDefault0] = None
    required: Optional[StringArray] = None
    additionalProperties: Optional[Union[bool, Default]] = {}
    definitions: Optional[Dict[str, Default]] = {}
    properties: Optional[Dict[str, Default]] = {}
    patternProperties: Optional[Dict[str, Default]] = {}
    dependencies: Optional[Dict[str, Union[Default, StringArray]]] = None
    enum: Optional[List] = Field(None, min_items=1, unique_items=True)
    type: Optional[Union[SimpleTypes, List[SimpleTypes]]] = None
    format: Optional[str] = None
    allOf: Optional[SchemaArrayModel1] = None
    anyOf: Optional[SchemaArrayModel1] = None
    oneOf: Optional[SchemaArrayModel1] = None
    not_: Optional[Default] = Field(None, alias='not')


class SchemaArrayModel1(BaseModel):
    __root__: List[Default] = Field(..., min_items=1)


class MultipleOf(BaseModel):
    id: Optional[str] = None
    field_schema: Optional[str] = Field(None, alias='$schema')
    title: Optional[str] = None
    description: Optional[str] = None
    default: Optional[Any] = None
    multipleOf: Optional[PositiveFloat] = None
    maximum: Optional[float] = None
    exclusiveMaximum: Optional[bool] = False
    minimum: Optional[float] = None
    exclusiveMinimum: Optional[bool] = False
    maxLength: Optional[PositiveInteger] = None
    minLength: Optional[PositiveIntegerDefault0] = None
    pattern: Optional[str] = None
    additionalItems: Optional[Union[bool, MultipleOf]] = {}
    items: Optional[Union[MultipleOf, SchemaArrayModel2]] = {}
    maxItems: Optional[PositiveInteger] = None
    minItems: Optional[PositiveIntegerDefault0] = None
    uniqueItems: Optional[bool] = False
    maxProperties: Optional[PositiveInteger] = None
    minProperties: Optional[PositiveIntegerDefault0] = None
    required: Optional[StringArray] = None
    additionalProperties: Optional[Union[bool, MultipleOf]] = {}
    definitions: Optional[Dict[str, MultipleOf]] = {}
    properties: Optional[Dict[str, MultipleOf]] = {}
    patternProperties: Optional[Dict[str, MultipleOf]] = {}
    dependencies: Optional[Dict[str, Union[MultipleOf, StringArray]]] = None
    enum: Optional[List] = Field(None, min_items=1, unique_items=True)
    type: Optional[Union[SimpleTypes, List[SimpleTypes]]] = None
    format: Optional[str] = None
    allOf: Optional[SchemaArrayModel2] = None
    anyOf: Optional[SchemaArrayModel2] = None
    oneOf: Optional[SchemaArrayModel2] = None
    not_: Optional[MultipleOf] = Field(None, alias='not')


class SchemaArrayModel2(BaseModel):
    __root__: List[MultipleOf] = Field(..., min_items=1)


class Maximum(BaseModel):
    id: Optional[str] = None
    field_schema: Optional[str] = Field(None, alias='$schema')
    title: Optional[str] = None
    description: Optional[str] = None
    default: Optional[Any] = None
    multipleOf: Optional[PositiveFloat] = None
    maximum: Optional[float] = None
    exclusiveMaximum: Optional[bool] = False
    minimum: Optional[float] = None
    exclusiveMinimum: Optional[bool] = False
    maxLength: Optional[PositiveInteger] = None
    minLength: Optional[PositiveIntegerDefault0] = None
    pattern: Optional[str] = None
    additionalItems: Optional[Union[bool, Maximum]] = {}
    items: Optional[Union[Maximum, SchemaArrayModel3]] = {}
    maxItems: Optional[PositiveInteger] = None
    minItems: Optional[PositiveIntegerDefault0] = None
    uniqueItems: Optional[bool] = False
    maxProperties: Optional[PositiveInteger] = None
    minProperties: Optional[PositiveIntegerDefault0] = None
    required: Optional[StringArray] = None
    additionalProperties: Optional[Union[bool, Maximum]] = {}
    definitions: Optional[Dict[str, Maximum]] = {}
    properties: Optional[Dict[str, Maximum]] = {}
    patternProperties: Optional[Dict[str, Maximum]] = {}
    dependencies: Optional[Dict[str, Union[Maximum, StringArray]]] = None
    enum: Optional[List] = Field(None, min_items=1, unique_items=True)
    type: Optional[Union[SimpleTypes, List[SimpleTypes]]] = None
    format: Optional[str] = None
    allOf: Optional[SchemaArrayModel3] = None
    anyOf: Optional[SchemaArrayModel3] = None
    oneOf: Optional[SchemaArrayModel3] = None
    not_: Optional[Maximum] = Field(None, alias='not')


class SchemaArrayModel3(BaseModel):
    __root__: List[Maximum] = Field(..., min_items=1)


class ExclusiveMaximum(BaseModel):
    id: Optional[str] = None
    field_schema: Optional[str] = Field(None, alias='$schema')
    title: Optional[str] = None
    description: Optional[str] = None
    default: Optional[Any] = None
    multipleOf: Optional[PositiveFloat] = None
    maximum: Optional[float] = None
    exclusiveMaximum: Optional[bool] = False
    minimum: Optional[float] = None
    exclusiveMinimum: Optional[bool] = False
    maxLength: Optional[PositiveInteger] = None
    minLength: Optional[PositiveIntegerDefault0] = None
    pattern: Optional[str] = None
    additionalItems: Optional[Union[bool, ExclusiveMaximum]] = {}
    items: Optional[Union[ExclusiveMaximum, SchemaArrayModel4]] = {}
    maxItems: Optional[PositiveInteger] = None
    minItems: Optional[PositiveIntegerDefault0] = None
    uniqueItems: Optional[bool] = False
    maxProperties: Optional[PositiveInteger] = None
    minProperties: Optional[PositiveIntegerDefault0] = None
    required: Optional[StringArray] = None
    additionalProperties: Optional[Union[bool, ExclusiveMaximum]] = {}
    definitions: Optional[Dict[str, ExclusiveMaximum]] = {}
    properties: Optional[Dict[str, ExclusiveMaximum]] = {}
    patternProperties: Optional[Dict[str, ExclusiveMaximum]] = {}
    dependencies: Optional[Dict[str, Union[ExclusiveMaximum, StringArray]]] = None
    enum: Optional[List] = Field(None, min_items=1, unique_items=True)
    type: Optional[Union[SimpleTypes, List[SimpleTypes]]] = None
    format: Optional[str] = None
    allOf: Optional[SchemaArrayModel4] = None
    anyOf: Optional[SchemaArrayModel4] = None
    oneOf: Optional[SchemaArrayModel4] = None
    not_: Optional[ExclusiveMaximum] = Field(None, alias='not')


class SchemaArrayModel4(BaseModel):
    __root__: List[ExclusiveMaximum] = Field(..., min_items=1)


class Minimum(BaseModel):
    id: Optional[str] = None
    field_schema: Optional[str] = Field(None, alias='$schema')
    title: Optional[str] = None
    description: Optional[str] = None
    default: Optional[Any] = None
    multipleOf: Optional[PositiveFloat] = None
    maximum: Optional[float] = None
    exclusiveMaximum: Optional[bool] = False
    minimum: Optional[float] = None
    exclusiveMinimum: Optional[bool] = False
    maxLength: Optional[PositiveInteger] = None
    minLength: Optional[PositiveIntegerDefault0] = None
    pattern: Optional[str] = None
    additionalItems: Optional[Union[bool, Minimum]] = {}
    items: Optional[Union[Minimum, SchemaArrayModel5]] = {}
    maxItems: Optional[PositiveInteger] = None
    minItems: Optional[PositiveIntegerDefault0] = None
    uniqueItems: Optional[bool] = False
    maxProperties: Optional[PositiveInteger] = None
    minProperties: Optional[PositiveIntegerDefault0] = None
    required: Optional[StringArray] = None
    additionalProperties: Optional[Union[bool, Minimum]] = {}
    definitions: Optional[Dict[str, Minimum]] = {}
    properties: Optional[Dict[str, Minimum]] = {}
    patternProperties: Optional[Dict[str, Minimum]] = {}
    dependencies: Optional[Dict[str, Union[Minimum, StringArray]]] = None
    enum: Optional[List] = Field(None, min_items=1, unique_items=True)
    type: Optional[Union[SimpleTypes, List[SimpleTypes]]] = None
    format: Optional[str] = None
    allOf: Optional[SchemaArrayModel5] = None
    anyOf: Optional[SchemaArrayModel5] = None
    oneOf: Optional[SchemaArrayModel5] = None
    not_: Optional[Minimum] = Field(None, alias='not')


class SchemaArrayModel5(BaseModel):
    __root__: List[Minimum] = Field(..., min_items=1)


class ExclusiveMinimum(BaseModel):
    id: Optional[str] = None
    field_schema: Optional[str] = Field(None, alias='$schema')
    title: Optional[str] = None
    description: Optional[str] = None
    default: Optional[Any] = None
    multipleOf: Optional[PositiveFloat] = None
    maximum: Optional[float] = None
    exclusiveMaximum: Optional[bool] = False
    minimum: Optional[float] = None
    exclusiveMinimum: Optional[bool] = False
    maxLength: Optional[PositiveInteger] = None
    minLength: Optional[PositiveIntegerDefault0] = None
    pattern: Optional[str] = None
    additionalItems: Optional[Union[bool, ExclusiveMinimum]] = {}
    items: Optional[Union[ExclusiveMinimum, SchemaArrayModel6]] = {}
    maxItems: Optional[PositiveInteger] = None
    minItems: Optional[PositiveIntegerDefault0] = None
    uniqueItems: Optional[bool] = False
    maxProperties: Optional[PositiveInteger] = None
    minProperties: Optional[PositiveIntegerDefault0] = None
    required: Optional[StringArray] = None
    additionalProperties: Optional[Union[bool, ExclusiveMinimum]] = {}
    definitions: Optional[Dict[str, ExclusiveMinimum]] = {}
    properties: Optional[Dict[str, ExclusiveMinimum]] = {}
    patternProperties: Optional[Dict[str, ExclusiveMinimum]] = {}
    dependencies: Optional[Dict[str, Union[ExclusiveMinimum, StringArray]]] = None
    enum: Optional[List] = Field(None, min_items=1, unique_items=True)
    type: Optional[Union[SimpleTypes, List[SimpleTypes]]] = None
    format: Optional[str] = None
    allOf: Optional[SchemaArrayModel6] = None
    anyOf: Optional[SchemaArrayModel6] = None
    oneOf: Optional[SchemaArrayModel6] = None
    not_: Optional[ExclusiveMinimum] = Field(None, alias='not')


class SchemaArrayModel6(BaseModel):
    __root__: List[ExclusiveMinimum] = Field(..., min_items=1)


class PositiveIntegerModel(BaseModel):
    id: Optional[str] = None
    field_schema: Optional[str] = Field(None, alias='$schema')
    title: Optional[str] = None
    description: Optional[str] = None
    default: Optional[Any] = None
    multipleOf: Optional[PositiveFloat] = None
    maximum: Optional[float] = None
    exclusiveMaximum: Optional[bool] = False
    minimum: Optional[float] = None
    exclusiveMinimum: Optional[bool] = False
    maxLength: Optional[PositiveInteger] = None
    minLength: Optional[PositiveIntegerDefault0] = None
    pattern: Optional[str] = None
    additionalItems: Optional[Union[bool, PositiveIntegerModel]] = {}
    items: Optional[Union[PositiveIntegerModel, SchemaArrayModel7]] = {}
    maxItems: Optional[PositiveInteger] = None
    minItems: Optional[PositiveIntegerDefault0] = None
    uniqueItems: Optional[bool] = False
    maxProperties: Optional[PositiveInteger] = None
    minProperties: Optional[PositiveIntegerDefault0] = None
    required: Optional[StringArray] = None
    additionalProperties: Optional[Union[bool, PositiveIntegerModel]] = {}
    definitions: Optional[Dict[str, PositiveIntegerModel]] = {}
    properties: Optional[Dict[str, PositiveIntegerModel]] = {}
    patternProperties: Optional[Dict[str, PositiveIntegerModel]] = {}
    dependencies: Optional[Dict[str, Union[PositiveIntegerModel, StringArray]]] = None
    enum: Optional[List] = Field(None, min_items=1, unique_items=True)
    type: Optional[Union[SimpleTypes, List[SimpleTypes]]] = None
    format: Optional[str] = None
    allOf: Optional[SchemaArrayModel7] = None
    anyOf: Optional[SchemaArrayModel7] = None
    oneOf: Optional[SchemaArrayModel7] = None
    not_: Optional[PositiveIntegerModel] = Field(None, alias='not')


class SchemaArrayModel7(BaseModel):
    __root__: List[PositiveIntegerModel] = Field(..., min_items=1)


class PositiveIntegerDefault0Model(BaseModel):
    id: Optional[str] = None
    field_schema: Optional[str] = Field(None, alias='$schema')
    title: Optional[str] = None
    description: Optional[str] = None
    default: Optional[Any] = None
    multipleOf: Optional[PositiveFloat] = None
    maximum: Optional[float] = None
    exclusiveMaximum: Optional[bool] = False
    minimum: Optional[float] = None
    exclusiveMinimum: Optional[bool] = False
    maxLength: Optional[PositiveInteger] = None
    minLength: Optional[PositiveIntegerDefault0] = None
    pattern: Optional[str] = None
    additionalItems: Optional[Union[bool, PositiveIntegerDefault0Model]] = {}
    items: Optional[Union[PositiveIntegerDefault0Model, SchemaArrayModel8]] = {}
    maxItems: Optional[PositiveInteger] = None
    minItems: Optional[PositiveIntegerDefault0] = None
    uniqueItems: Optional[bool] = False
    maxProperties: Optional[PositiveInteger] = None
    minProperties: Optional[PositiveIntegerDefault0] = None
    required: Optional[StringArray] = None
    additionalProperties: Optional[Union[bool, PositiveIntegerDefault0Model]] = {}
    definitions: Optional[Dict[str, PositiveIntegerDefault0Model]] = {}
    properties: Optional[Dict[str, PositiveIntegerDefault0Model]] = {}
    patternProperties: Optional[Dict[str, PositiveIntegerDefault0Model]] = {}
    dependencies: Optional[
        Dict[str, Union[PositiveIntegerDefault0Model, StringArray]]
    ] = None
    enum: Optional[List] = Field(None, min_items=1, unique_items=True)
    type: Optional[Union[SimpleTypes, List[SimpleTypes]]] = None
    format: Optional[str] = None
    allOf: Optional[SchemaArrayModel8] = None
    anyOf: Optional[SchemaArrayModel8] = None
    oneOf: Optional[SchemaArrayModel8] = None
    not_: Optional[PositiveIntegerDefault0Model] = Field(None, alias='not')


class SchemaArrayModel8(BaseModel):
    __root__: List[PositiveIntegerDefault0Model] = Field(..., min_items=1)


class Pattern(BaseModel):
    id: Optional[str] = None
    field_schema: Optional[str] = Field(None, alias='$schema')
    title: Optional[str] = None
    description: Optional[str] = None
    default: Optional[Any] = None
    multipleOf: Optional[PositiveFloat] = None
    maximum: Optional[float] = None
    exclusiveMaximum: Optional[bool] = False
    minimum: Optional[float] = None
    exclusiveMinimum: Optional[bool] = False
    maxLength: Optional[PositiveInteger] = None
    minLength: Optional[PositiveIntegerDefault0] = None
    pattern: Optional[str] = None
    additionalItems: Optional[Union[bool, Pattern]] = {}
    items: Optional[Union[Pattern, SchemaArrayModel9]] = {}
    maxItems: Optional[PositiveInteger] = None
    minItems: Optional[PositiveIntegerDefault0] = None
    uniqueItems: Optional[bool] = False
    maxProperties: Optional[PositiveInteger] = None
    minProperties: Optional[PositiveIntegerDefault0] = None
    required: Optional[StringArray] = None
    additionalProperties: Optional[Union[bool, Pattern]] = {}
    definitions: Optional[Dict[str, Pattern]] = {}
    properties: Optional[Dict[str, Pattern]] = {}
    patternProperties: Optional[Dict[str, Pattern]] = {}
    dependencies: Optional[Dict[str, Union[Pattern, StringArray]]] = None
    enum: Optional[List] = Field(None, min_items=1, unique_items=True)
    type: Optional[Union[SimpleTypes, List[SimpleTypes]]] = None
    format: Optional[str] = None
    allOf: Optional[SchemaArrayModel9] = None
    anyOf: Optional[SchemaArrayModel9] = None
    oneOf: Optional[SchemaArrayModel9] = None
    not_: Optional[Pattern] = Field(None, alias='not')


class SchemaArrayModel9(BaseModel):
    __root__: List[Pattern] = Field(..., min_items=1)


class UniqueItems(BaseModel):
    id: Optional[str] = None
    field_schema: Optional[str] = Field(None, alias='$schema')
    title: Optional[str] = None
    description: Optional[str] = None
    default: Optional[Any] = None
    multipleOf: Optional[PositiveFloat] = None
    maximum: Optional[float] = None
    exclusiveMaximum: Optional[bool] = False
    minimum: Optional[float] = None
    exclusiveMinimum: Optional[bool] = False
    maxLength: Optional[PositiveInteger] = None
    minLength: Optional[PositiveIntegerDefault0] = None
    pattern: Optional[str] = None
    additionalItems: Optional[Union[bool, UniqueItems]] = {}
    items: Optional[Union[UniqueItems, SchemaArrayModel10]] = {}
    maxItems: Optional[PositiveInteger] = None
    minItems: Optional[PositiveIntegerDefault0] = None
    uniqueItems: Optional[bool] = False
    maxProperties: Optional[PositiveInteger] = None
    minProperties: Optional[PositiveIntegerDefault0] = None
    required: Optional[StringArray] = None
    additionalProperties: Optional[Union[bool, UniqueItems]] = {}
    definitions: Optional[Dict[str, UniqueItems]] = {}
    properties: Optional[Dict[str, UniqueItems]] = {}
    patternProperties: Optional[Dict[str, UniqueItems]] = {}
    dependencies: Optional[Dict[str, Union[UniqueItems, StringArray]]] = None
    enum: Optional[List] = Field(None, min_items=1, unique_items=True)
    type: Optional[Union[SimpleTypes, List[SimpleTypes]]] = None
    format: Optional[str] = None
    allOf: Optional[SchemaArrayModel10] = None
    anyOf: Optional[SchemaArrayModel10] = None
    oneOf: Optional[SchemaArrayModel10] = None
    not_: Optional[UniqueItems] = Field(None, alias='not')


class SchemaArrayModel10(BaseModel):
    __root__: List[UniqueItems] = Field(..., min_items=1)


class StringArrayModel(BaseModel):
    id: Optional[str] = None
    field_schema: Optional[str] = Field(None, alias='$schema')
    title: Optional[str] = None
    description: Optional[str] = None
    default: Optional[Any] = None
    multipleOf: Optional[PositiveFloat] = None
    maximum: Optional[float] = None
    exclusiveMaximum: Optional[bool] = False
    minimum: Optional[float] = None
    exclusiveMinimum: Optional[bool] = False
    maxLength: Optional[PositiveInteger] = None
    minLength: Optional[PositiveIntegerDefault0] = None
    pattern: Optional[str] = None
    additionalItems: Optional[Union[bool, StringArrayModel]] = {}
    items: Optional[Union[StringArrayModel, SchemaArrayModel11]] = {}
    maxItems: Optional[PositiveInteger] = None
    minItems: Optional[PositiveIntegerDefault0] = None
    uniqueItems: Optional[bool] = False
    maxProperties: Optional[PositiveInteger] = None
    minProperties: Optional[PositiveIntegerDefault0] = None
    required: Optional[StringArray] = None
    additionalProperties: Optional[Union[bool, StringArrayModel]] = {}
    definitions: Optional[Dict[str, StringArrayModel]] = {}
    properties: Optional[Dict[str, StringArrayModel]] = {}
    patternProperties: Optional[Dict[str, StringArrayModel]] = {}
    dependencies: Optional[Dict[str, Union[StringArrayModel, StringArray]]] = None
    enum: Optional[List] = Field(None, min_items=1, unique_items=True)
    type: Optional[Union[SimpleTypes, List[SimpleTypes]]] = None
    format: Optional[str] = None
    allOf: Optional[SchemaArrayModel11] = None
    anyOf: Optional[SchemaArrayModel11] = None
    oneOf: Optional[SchemaArrayModel11] = None
    not_: Optional[StringArrayModel] = Field(None, alias='not')


class SchemaArrayModel11(BaseModel):
    __root__: List[StringArrayModel] = Field(..., min_items=1)


class EnumModel(BaseModel):
    id: Optional[str] = None
    field_schema: Optional[str] = Field(None, alias='$schema')
    title: Optional[str] = None
    description: Optional[str] = None
    default: Optional[Any] = None
    multipleOf: Optional[PositiveFloat] = None
    maximum: Optional[float] = None
    exclusiveMaximum: Optional[bool] = False
    minimum: Optional[float] = None
    exclusiveMinimum: Optional[bool] = False
    maxLength: Optional[PositiveInteger] = None
    minLength: Optional[PositiveIntegerDefault0] = None
    pattern: Optional[str] = None
    additionalItems: Optional[Union[bool, EnumModel]] = {}
    items: Optional[Union[EnumModel, SchemaArrayModel12]] = {}
    maxItems: Optional[PositiveInteger] = None
    minItems: Optional[PositiveIntegerDefault0] = None
    uniqueItems: Optional[bool] = False
    maxProperties: Optional[PositiveInteger] = None
    minProperties: Optional[PositiveIntegerDefault0] = None
    required: Optional[StringArray] = None
    additionalProperties: Optional[Union[bool, EnumModel]] = {}
    definitions: Optional[Dict[str, EnumModel]] = {}
    properties: Optional[Dict[str, EnumModel]] = {}
    patternProperties: Optional[Dict[str, EnumModel]] = {}
    dependencies: Optional[Dict[str, Union[EnumModel, StringArray]]] = None
    enum: Optional[List] = Field(None, min_items=1, unique_items=True)
    type: Optional[Union[SimpleTypes, List[SimpleTypes]]] = None
    format: Optional[str] = None
    allOf: Optional[SchemaArrayModel12] = None
    anyOf: Optional[SchemaArrayModel12] = None
    oneOf: Optional[SchemaArrayModel12] = None
    not_: Optional[EnumModel] = Field(None, alias='not')


class SchemaArrayModel12(BaseModel):
    __root__: List[EnumModel] = Field(..., min_items=1)


class TypeModel(BaseModel):
    id: Optional[str] = None
    field_schema: Optional[str] = Field(None, alias='$schema')
    title: Optional[str] = None
    description: Optional[str] = None
    default: Optional[Any] = None
    multipleOf: Optional[PositiveFloat] = None
    maximum: Optional[float] = None
    exclusiveMaximum: Optional[bool] = False
    minimum: Optional[float] = None
    exclusiveMinimum: Optional[bool] = False
    maxLength: Optional[PositiveInteger] = None
    minLength: Optional[PositiveIntegerDefault0] = None
    pattern: Optional[str] = None
    additionalItems: Optional[Union[bool, TypeModel]] = {}
    items: Optional[Union[TypeModel, SchemaArrayModel13]] = {}
    maxItems: Optional[PositiveInteger] = None
    minItems: Optional[PositiveIntegerDefault0] = None
    uniqueItems: Optional[bool] = False
    maxProperties: Optional[PositiveInteger] = None
    minProperties: Optional[PositiveIntegerDefault0] = None
    required: Optional[StringArray] = None
    additionalProperties: Optional[Union[bool, TypeModel]] = {}
    definitions: Optional[Dict[str, TypeModel]] = {}
    properties: Optional[Dict[str, TypeModel]] = {}
    patternProperties: Optional[Dict[str, TypeModel]] = {}
    dependencies: Optional[Dict[str, Union[TypeModel, StringArray]]] = None
    enum: Optional[List] = Field(None, min_items=1, unique_items=True)
    type: Optional[Union[SimpleTypes, List[SimpleTypes]]] = None
    format: Optional[str] = None
    allOf: Optional[SchemaArrayModel13] = None
    anyOf: Optional[SchemaArrayModel13] = None
    oneOf: Optional[SchemaArrayModel13] = None
    not_: Optional[TypeModel] = Field(None, alias='not')


class SchemaArrayModel13(BaseModel):
    __root__: List[TypeModel] = Field(..., min_items=1)


class FileSchema(BaseModel):
    class Config:
        extra = "forbid"

    format: Optional[str] = None
    title: Optional[Title] = None
    description: Optional[Description] = None
    default: Optional[Default] = None
    required: Optional[StringArrayModel] = None
    type: Type5
    readOnly: Optional[bool] = False
    externalDocs: Optional[ExternalDocs] = None
    example: Optional[Any] = None


class PrimitivesItems(BaseModel):
    class Config:
        extra = "forbid"

    type: Optional[Type6] = None
    format: Optional[str] = None
    items: Optional[PrimitivesItems] = None
    collectionFormat: Optional[CollectionFormat] = 'csv'
    default: Optional[Default] = None
    maximum: Optional[Maximum] = None
    exclusiveMaximum: Optional[ExclusiveMaximum] = None
    minimum: Optional[Minimum] = None
    exclusiveMinimum: Optional[ExclusiveMinimum] = None
    maxLength: Optional[MaxLength] = None
    minLength: Optional[MinLength] = None
    pattern: Optional[Pattern] = None
    maxItems: Optional[MaxItems] = None
    minItems: Optional[MinItems] = None
    uniqueItems: Optional[UniqueItems] = None
    enum: Optional[EnumModel] = None
    multipleOf: Optional[MultipleOf] = None


class ParametersList(BaseModel):
    __root__: List[Union[Parameter, JsonReference]] = Field(
        ...,
        description='The parameters needed to send a valid API call.',
        unique_items=True,
    )


class MaxLength(BaseModel):
    __root__: PositiveIntegerModel


class MinLength(BaseModel):
    __root__: PositiveIntegerDefault0Model


class MaxItems(BaseModel):
    __root__: PositiveIntegerModel


class MinItems(BaseModel):
    __root__: PositiveIntegerDefault0Model


AJsonSchemaForSwagger20Api.model_rebuild()
Paths.model_rebuild()
Definitions.model_rebuild()
ParameterDefinitions.model_rebuild()
ResponseDefinitions.model_rebuild()
Operation.model_rebuild()
PathItem.model_rebuild()
Responses.model_rebuild()
ResponseValue.model_rebuild()
Response.model_rebuild()
Headers.model_rebuild()
Header.model_rebuild()
BodyParameter.model_rebuild()
HeaderParameterSubSchema.model_rebuild()
QueryParameterSubSchema.model_rebuild()
FormDataParameterSubSchema.model_rebuild()
PathParameterSubSchema.model_rebuild()
Schema.model_rebuild()
Title.model_rebuild()
Description.model_rebuild()
Default.model_rebuild()
MultipleOf.model_rebuild()
Maximum.model_rebuild()
ExclusiveMaximum.model_rebuild()
Minimum.model_rebuild()
ExclusiveMinimum.model_rebuild()
PositiveIntegerModel.model_rebuild()
PositiveIntegerDefault0Model.model_rebuild()
Pattern.model_rebuild()
UniqueItems.model_rebuild()
StringArrayModel.model_rebuild()
EnumModel.model_rebuild()
TypeModel.model_rebuild()
PrimitivesItems.model_rebuild()
