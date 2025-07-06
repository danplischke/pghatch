from typing import List

from fastapi import APIRouter
from pydantic import BaseModel
from pydantic.alias_generators import to_camel
from pydantic.v1 import create_model

from pghatch.introspection.introspection import Introspection
from pghatch.introspection.pgtypes import get_py_type
from pghatch.router.resolver.resolver import Resolver


class ProcResolver(Resolver):
    """
    Resolver for proc-related operations.
    """

    def __init__(self, oid: str, introspection: Introspection):
        proc = introspection.get_proc(oid)
        if proc is None:
            raise ValueError(f"Procedure with OID {oid} not found in introspection data.")
        self.proc = proc
        self.name = proc.proname
        self.oid = oid
        self.schema = proc.get_namespace(introspection).nspname
        self.type = proc.prokind
        self.return_type, self.input_model = self._create_return_type(
            introspection
        )

    def _create_return_type(self, introspection: Introspection):
        args = self.proc.get_arguments(introspection)
        ret_type = self.proc.get_return_type(introspection)
        ret_type = get_py_type(introspection=introspection, typ=ret_type)

        arg_definitions = dict()
        for arg in args:
            # handle variadic
            if arg.is_in:
                arg_definitions[arg.name] = get_py_type(introspection=introspection, typ=arg.typ)
        if len(arg_definitions) == 0:
            input_model = None
        else:
            input_model = create_model(to_camel(f"{self.name}_input"),
                                       **arg_definitions)

        return ret_type, input_model

    async def resolver_function(self, inp: BaseModel):
        from pglast.ast import SelectStmt, A_Const, Integer, RangeVar
        from pglast.stream import RawStream
        import asyncpg

        select_stmt = SelectStmt(

        )

    def resolve(self):
        """
        Resolve the request for proc operations.
        """
        from typing import TypeVar

        inp = TypeVar("inp", bound=self.input_model)
        output = TypeVar("ret", bound=self.return_type)

        async def resolver_no_arg_function() -> output:
            return await self.resolver_function(None)

        async def resolver_function(inp: inp) -> output:
            return await self.resolver_function(inp)

        async def resolver_no_arg_procedure():
            return await self.resolver_function(None)

        async def resolver_procedure(inp: inp):
            return await self.resolver_function(inp)

        if self.type == "f":
            resolver_function = resolver_no_arg_procedure if self.input_model is None else resolver_procedure
        elif self.type == "p":
            resolver_function = resolver_no_arg_function if self.input_model is None else resolver_function

        return resolver_function

    def mount(self, router: APIRouter):
        router.add_api_route(
            f"/{self.schema}/{self.name}",
            self.resolve(),
            methods=["POST"],
            response_model=List[self.return_type],
            summary=f"Get data from {self.schema}.{self.name}",
            description=f"Fetches data from the table or view {self.schema}.{self.name}.",
        )


if __name__ == '__main__':
    from pglast.parser import parse_sql
    from pglast.stream import RawStream
    from pglast.ast import SelectStmt, A_Const, Integer, ParamRef, RangeVar, ResTarget, RangeFunction, FuncCall, String, \
        ColumnRef, A_Star

    stmt = parse_sql("SELECT * FROM public.test_function($1, $1, $1)")[0].stmt

    statement = SelectStmt(
        targetList=(
            ResTarget(val=ColumnRef(fields=[A_Star()])),
        ),
        fromClause=(
            RangeFunction(
                functions=(
                    (FuncCall(funcname=(String(sval='public'), String(sval='test_function')), args=[
                        ParamRef(number=1),
                        ParamRef(number=1),
                        ParamRef(number=1)]  # use ParamRef
                              ),
                     None
                     ),
                ),
            ),
        )
    )

    print(RawStream()(statement))
