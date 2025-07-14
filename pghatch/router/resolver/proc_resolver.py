from typing import List

from fastapi import APIRouter
from pydantic import BaseModel
from pydantic import create_model
from pydantic.alias_generators import to_camel

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
        self.introspection = introspection
        self.name = proc.proname
        self.oid = oid
        self.schema = proc.get_namespace(introspection).nspname
        self.type = proc.prokind
        self.return_type, self.input_model = self._create_return_type(
            introspection
        )
        self.router = None

    def _create_return_type(self, introspection: Introspection):
        args = self.proc.get_arguments(introspection)
        ret_type = self.proc.get_return_type(introspection)
        ret_type = get_py_type(introspection=introspection, typ=ret_type)

        if self.proc.proretset:
            # If the procedure returns a set, we need to handle it as a list of records
            ret_type = List[ret_type]

        ret_type = create_model(
            to_camel(f"{self.name}_return"),
            **{"result": ret_type}
        )

        arg_definitions = dict()
        for i, arg in enumerate(args):
            # handle variadic
            if arg.is_in and arg.name:
                arg_definitions[arg.name] = get_py_type(introspection=introspection, typ=arg.typ)
            elif arg.is_in and not arg.name:
                # Generate a name for unnamed arguments
                arg_definitions[f"arg_{i}"] = get_py_type(introspection=introspection, typ=arg.typ)

        if len(arg_definitions) == 0:
            input_model = None
        else:
            input_model = create_model(to_camel(f"{self.name}_input"), **arg_definitions)

        return ret_type, input_model

    async def resolver_function(self, inp: BaseModel | None = None):
        from pglast.ast import SelectStmt, ParamRef, RangeFunction, FuncCall, String, ResTarget, ColumnRef, A_Star
        from pglast.stream import RawStream

        # Get the arguments for the procedure
        args = self.proc.get_arguments(self.introspection)

        # Build parameter references for the function call
        param_refs = []
        param_values = []

        if inp is not None and self.input_model is not None:
            param_num = 1

            for i, arg in enumerate(args):
                if arg.is_in:
                    # Use the argument name if available, otherwise use generated name
                    arg_name = arg.name if arg.name else f"arg_{i}"
                    if arg_name in inp:
                        param_refs.append(ParamRef(number=param_num))
                        param_values.append(inp[arg_name])
                        param_num += 1

        # Build the function call
        func_call = FuncCall(
            funcname=(String(sval=self.schema), String(sval=self.name)),
            args=param_refs
        )

        # Build the SELECT statement
        select_stmt = SelectStmt(
            targetList=(
                ResTarget(val=ColumnRef(fields=[A_Star()])),
            ),
            fromClause=(
                RangeFunction(
                    functions=(
                        (func_call, None),
                    ),
                ),
            )
        )

        # Generate SQL
        sql = RawStream()(select_stmt)

        # Execute the query
        async with self.router._pool.acquire() as conn:
            if self.proc.proretset:
                result = await conn.fetch(sql, *param_values)
                return self.return_type(result=[dict(row) for row in result])
            else:
                result = await conn.fetchrow(sql, *param_values)

                if result is not None and len(result) == 1:
                    result = result[0]
                elif result is not None:
                    result = dict(result)
                return self.return_type(result=result)

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
        self.router = router

        router.add_api_route(
            f"/{self.schema}/{self.name}",
            self.resolve(),
            methods=["POST"],
            response_model=self.return_type,
            summary=f"Get data from {self.schema}.{self.name}",
            description=f"Fetches data from the function or procedure {self.schema}.{self.name}.",
        )


if __name__ == '__main__':
    from pglast.parser import parse_sql
    from pglast.stream import RawStream
    from pglast.ast import SelectStmt, ParamRef, ResTarget, RangeFunction, FuncCall, String, \
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
