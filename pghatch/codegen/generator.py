import abc
import ast
import sys
import typing
from abc import abstractmethod
from typing import Any, Dict, List, Optional, Union, cast, Tuple

import pydantic
from pydantic import validate_call

Annotation = Union[ast.Name, ast.Subscript]
TYPING_MODULE = "typing"

class Codegen(abc.ABC):

    def __init__(self) -> None:
        self.imports: Dict[str, set[str]] = {}

    def type_str(self):
        """Generate str annotation."""
        return ast.Name(id=str.__name__)

    def type_int(self):
        """Generate int annotation."""
        return ast.Name(id=int.__name__)

    def type_float(self):
        """Generate float annotation."""
        return ast.Name(id=float.__name__)

    def type_bool(self):
        """Generate bool annotation."""
        return ast.Name(id=bool.__name__)

    def type_Optional(self):
        """Generate optional annotation."""
        self._add_import(TYPING_MODULE, Optional.__name__)
        return ast.Name(id=Optional[str.__name__])

    def type_list(self, type_: Optional[Annotation] = None):
        """Generate list annotation."""
        self._add_import(TYPING_MODULE, List.__name__)
        if type_:
            return ast.Subscript(value=ast.Name(id=List.__name__), slice=type_)
        return ast.Name(id=list.__name__)

    def type_dict(self, key_type: Optional[Annotation] = None,
                  value_type: Optional[Annotation] = None) -> ast.Subscript:
        """Generate dict annotation."""
        self._add_import(TYPING_MODULE, Tuple.__name__)
        slice_ = ast.Tuple(elts=[key_type if key_type else ast.Name(id=str.__name__),
                                 value_type if value_type else ast.Name(id=Any.__name__)])
        return ast.Subscript(value=ast.Name(id=dict.__name__), slice=slice_)

    def type_tuple(self, type_: Optional[Annotation] = None):
        """Generate tuple annotation."""
        if type_:
            slice_ = type_
        else:
            slice_ = Any.__name__

        return ast.Subscript(value=ast.Name(id=tuple.__name__), slice=slice_)

    def type_set(self, type_: Optional[Annotation] = None):
        """Generate set annotation."""
        if type_:
            return ast.Subscript(value=ast.Name(id=set.__name__), slice=type_)
        return ast.Name(id=set.__name__)

    def import_from(self, names: List[str], from_: Optional[str] = None, level: int = 0) -> ast.ImportFrom:
        """Generate import from statement."""
        return ast.ImportFrom(module=from_, names=[ast.alias(n) for n in names], level=level)

    def nullable_annotation(self, slice_: Union[ast.Name, ast.Subscript]) -> ast.Subscript:
        """Generate optional annotation."""
        return ast.Subscript(value=ast.Name(id=typing.Optional.__class__.__name__), slice=slice_)

    def annotation_name(self, name, nullable: bool = True) -> Union[ast.Name, ast.Subscript]:
        """Generate annotation."""
        result = ast.Name(id=name)
        return result if not nullable else self.nullable_annotation(result)

    def list_annotation(self, slice_: Union[ast.Name, ast.Subscript], nullable: bool = True) -> ast.Subscript:
        """Generate list annotation."""
        result = ast.Subscript(value=ast.Name(id=List.__name__), slice=slice_)
        return result if not nullable else self.nullable_annotation(result)

    def arg(self, name: str, annotation: Optional[ast.expr] = None) -> ast.arg:
        """Generate arg."""
        return ast.arg(arg=name, annotation=annotation)

    def arguments(self, args: Optional[List[ast.arg]] = None, vararg: Optional[ast.arg] = None,
                  kwonlyargs: Optional[list[ast.arg]] = None, kw_defaults: Optional[list[Union[ast.expr, None]]] = None,
                  kwarg: Optional[ast.arg] = None, defaults: Optional[List[ast.expr]] = None) -> ast.arguments:
        """Generate arguments."""
        return ast.arguments(posonlyargs=[], args=args if args else [], vararg=vararg,
                             kwonlyargs=kwonlyargs if kwonlyargs else [],
                             kw_defaults=kw_defaults if kw_defaults else [],
                             kwarg=kwarg, defaults=defaults or [])

    def async_method_definition(self, name: str, arguments: ast.arguments, return_type: Union[ast.Name, ast.Subscript],
                                body: Optional[List[ast.stmt]] = None, lineno: int = 1,
                                decorator_list: Optional[List[ast.expr]] = None) -> ast.AsyncFunctionDef:
        """Generate async function."""
        params: Dict[str, Any] = {"name": name, "args": arguments, "body": body if body else [ast.Pass()],
                                  "decorator_list": decorator_list if decorator_list else [], "returns": return_type,
                                  "lineno": lineno}
        if sys.version_info >= (3, 12):
            params["type_params"] = []
        return ast.AsyncFunctionDef(**params)

    def class_def(self, name: str, base_names: Optional[List[str]] = None,
                  body: Optional[List[ast.stmt]] = None) -> ast.ClassDef:
        """Generate class definition."""
        bases = cast(List[ast.expr], [ast.Name(id=name) for name in base_names] if base_names else [])
        params: Dict[str, Any] = {"name": name, "bases": bases, "keywords": [], "body": body if body else [],
                                  "decorator_list": []}
        if sys.version_info >= (3, 12):
            params["type_params"] = []
        return ast.ClassDef(**params)

    def name_(self, name: str) -> ast.Name:
        """Generate name object."""
        return ast.Name(id=name)

    def joined_str(self, values: list[ast.expr]) -> ast.JoinedStr:
        """Generate joined str object."""
        return ast.JoinedStr(values)

    def constant(self, value: Any) -> ast.Constant:
        """Generate constant object."""
        return ast.Constant(value=value)

    def formatted_value(self, value: ast.expr, conversion: int = -1,
                        format_spec: Optional[ast.expr] = None) -> ast.FormattedValue:
        """Generate formatted value object."""
        return ast.FormattedValue(value=value, conversion=conversion, format_spec=format_spec)

    def assign(self, targets: List[str], value: Union[ast.expr, List[ast.expr]], lineno: int = 1) -> ast.Assign:
        """Generate assign object."""
        return ast.Assign(targets=[ast.Name(id=t) for t in targets], value=value, lineno=lineno)  # type:ignore

    def ann_assign(self, target: Union[ast.Name, ast.Attribute, ast.Subscript], annotation: Annotation,
                   value: Optional[ast.expr] = None, lineno: int = 1) -> ast.AnnAssign:
        """Generate ann assign object."""
        return ast.AnnAssign(target=target, annotation=annotation, value=value, simple=1, lineno=lineno)

    def union_annotation(self, types: List[ast.expr], nullable: bool = True) -> ast.Subscript:
        """Generate union annotation."""
        result = ast.Subscript(value=ast.Name(id=typing.Union.__class__.__name__), slice=ast.Tuple(elts=types))
        return result if not nullable else self.nullable_annotation(result)

    def dict(self, keys: Optional[List[Optional[ast.expr]]] = None,
             values: Optional[List[ast.expr]] = None) -> ast.Dict:
        """Generate dict object."""
        return ast.Dict(keys=keys if keys else [], values=values if values else [])

    def await_(self, value: ast.expr) -> ast.Await:
        """Generate await object."""
        return ast.Await(value=value)

    def call(self, func: ast.expr, args: Optional[List[Union[ast.expr, List[ast.expr]]]] = None,
             keywords: Optional[List[ast.keyword]] = None) -> ast.Call:
        """Generate call object."""
        return ast.Call(func=func, args=args if args else [], keywords=keywords if keywords else [])  # type:ignore

    def attribute(self, value: ast.expr, attr: str) -> ast.Attribute:
        """Generate attribute object."""
        return ast.Attribute(value=value, attr=attr)

    def keyword(self, value: ast.expr, arg: Optional[str] = None) -> ast.keyword:
        """Generate keyword object."""
        return ast.keyword(arg=arg, value=value)

    def return_(self, value: Optional[ast.expr] = None) -> ast.Return:
        """Generate return object."""
        return ast.Return(value=value)

    def method_call(self, object_name: str, method_name: str, args: Optional[List[ast.expr]] = None) -> ast.Call:
        """Generate object`s method call."""
        return ast.Call(func=ast.Attribute(value=ast.Name(id=object_name), attr=method_name), args=args or [],
                        keywords=[])

    def expr(self, value: ast.expr):
        """Generate expression object."""
        return ast.Expr(value=value)

    def trivial_lambda(self, name: str, argument_name: str) -> ast.Assign:
        """Generate lambda that returns given argument, eg. gql = lambda q: q."""
        return ast.Assign(
            targets=[ast.Name(id=name)],
            value=ast.Lambda(
                args=ast.arguments(
                    posonlyargs=[],
                    args=[ast.arg(arg=argument_name)],
                    kwonlyargs=[],
                    kw_defaults=[],
                    defaults=[],
                ),
                body=ast.Name(id=argument_name),
            ),
        )

    def list(self, elements: List[ast.expr]) -> ast.List:
        """Generate list object."""
        return ast.List(elts=elements)

    def list_comp(self, elt: ast.expr, generators: List[ast.comprehension]) -> ast.ListComp:
        """Generate list comprehension"""
        return ast.ListComp(elt=elt, generators=generators)

    def comp(self, target: str, iter_: str, ifs: Optional[List[ast.expr]] = None,
             is_async: int = 0) -> ast.comprehension:
        "Generate comprehension"
        return ast.comprehension(target=self.name_(target), iter=self.name_(iter_), ifs=ifs if ifs else [],
                                 is_async=is_async)

    def lambda_(self, body: ast.expr, args: Optional[ast.arguments] = None) -> ast.Lambda:
        """Generate lambda definition."""
        return ast.Lambda(args=args or self.arguments(), body=body)

    def pydantic_field(self, keywords: Dict[str, ast.expr]) -> ast.Call:
        """Generate pydantic field call."""
        return self.call(func=self.name_(pydantic.Field.__name__),
                         keywords=[self.keyword(value=value, arg=arg) for arg, value in keywords.items()])

    def module(self, body: List[ast.stmt]) -> ast.Module:
        """Generate module object."""
        return ast.Module(body=body, type_ignores=[])

    def subscript(self, value: ast.expr, slice_: ast.expr) -> ast.Subscript:
        """Generate subscript object."""
        return ast.Subscript(value=value, slice=slice_)

    def tuple(self, elts: List[ast.expr]) -> ast.Tuple:
        """Generate tuple object."""
        return ast.Tuple(elts=elts)

    def method_definition(self, name: str, arguments: ast.arguments, return_type: Union[ast.Name, ast.Subscript],
                          body: Optional[List[ast.stmt]] = None, lineno: int = 1,
                          decorator_list: Optional[List[ast.expr]] = None) -> ast.FunctionDef:
        """Generate function definition."""
        params: Dict[str, Any] = {"name": name, "args": arguments, "body": body if body else [ast.Pass()],
                                  "decorator_list": decorator_list if decorator_list else [], "returns": return_type,
                                  "lineno": lineno}
        if sys.version_info >= (3, 12):
            params["type_params"] = []
        return ast.FunctionDef(**params)

    def async_for(self, target: ast.expr, iter_: ast.expr, body: Optional[List[ast.stmt]] = None,
                  orelse: Optional[List[ast.stmt]] = None, lineno: int = 1) -> ast.AsyncFor:
        """Generate async for statement."""
        return ast.AsyncFor(target=target, iter=iter_, body=body or [ast.Pass()], orelse=orelse or [], lineno=lineno)

    def yield_(self, value: Optional[ast.expr] = None) -> ast.Yield:
        """Generate yield object."""
        return ast.Yield(value=value)

    def pass_(self) -> ast.Pass:  # add self
        return ast.Pass()

    def bit_or(self, left: ast.expr, right: ast.expr) -> ast.BinOp:
        return ast.BinOp(left=left, op=ast.BitOr(), right=right)

    def bit_and(self, left: ast.expr, right: ast.expr) -> ast.BinOp:
        return ast.BinOp(left=left, op=ast.BitAnd(), right=right)

    def validate_call_decorator(self) -> ast.expr:
        self._add_import("pydantic", "validate_call")
        return ast.Name(id="validate_call")

    def _add_import(self, module: str, name: str):
        if module not in self.imports:
            self.imports[module] = set()
        self.imports[module].add(name)

    def _generate_imports(self) -> List[ast.stmt]:
        imports: List[ast.stmt] = []
        for module, names in self.imports.items():
            imports.append(self.import_from(names=sorted(names), from_=module))
        return imports

    @abstractmethod
    def generate(self):
        body = self._generate_imports()
        return self.module(body=body)


    # def endpoint_method(self) -> ast.FunctionDef:
    #     """Generate: def endpoint(filter: str, limit: int | None = None) -> EndpointModel with httpx call body"""
    #     # Arguments
    #     filter_arg = self.arg("filter", self.type_str())
    #     limit_union = ast.BinOp(left=self.type_int(), op=ast.BitOr(), right=ast.Constant(value=None))
    #     limit_arg = self.arg("limit", limit_union)
    #     args = self.arguments(args=[filter_arg, limit_arg], defaults=[ast.Constant(value=None)])
    #     return_type = self.name_("EndpointModel")
    #     # response = httpx.request(...)
    #     params_dict = self.dict(keys=[self.constant("filter"), self.constant("limit")],
    #                             values=[self.name_("filter"), self.name_("limit")])
    #     httpx_request_call = self.call(
    #         func=self.attribute(self.name_("httpx"), "request"),
    #         keywords=[
    #             self.keyword(arg="method", value=self.constant("GET")),
    #             self.keyword(arg="url", value=self.constant("http://localhost:8000/api")),
    #             self.keyword(arg="params", value=params_dict),
    #         ],
    #     )
    #     assign_response = self.assign(["response"], httpx_request_call)
    #     # response.raise_for_status()
    #     raise_call = self.expr(self.call(func=self.attribute(self.name_("response"), "raise_for_status")))
    #     # return EndpointModel.model_validate(response.json())
    #     response_json_call = self.call(func=self.attribute(self.name_("response"), "json"))
    #     model_validate_call = self.call(
    #         func=self.attribute(self.name_("EndpointModel"), "model_validate"),
    #         args=[response_json_call],
    #     )
    #     return_stmt = self.return_(model_validate_call)
    #     body: List[ast.stmt] = [assign_response, raise_call, return_stmt]
    #     return self.method_definition(name="endpoint", arguments=args, return_type=return_type, body=body)



class OpenAPIGenerator(Codegen, abc.ABC):

    @abstractmethod
    def add_model(self, model_name: str, fields: Dict[str, Any]) -> None:
        # Placeholder for adding a model based on OpenAPI schema
        pass

    @abstractmethod
    def add_endpoint(self, path: str, method: str, operation: Dict[str, Any]) -> None:
        # Placeholder for adding an endpoint based on OpenAPI operation
        pass