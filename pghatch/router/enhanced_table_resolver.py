"""
Enhanced TableViewResolver with support for multiple HTTP methods and the JSON filter format.
"""
import asyncio
import logging
from typing import Any, Dict, List, Optional, Union

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from pghatch.introspection.introspection import Introspection
from pghatch.router.resolver.resolver import Resolver
from pghatch.router.filter_models import (
    FilterRequest, StandardResponse, PrimaryKeyRequest, 
    UpdateRequest, CreateRequest, PaginationParams, SelectClause
)
from pghatch.router.filter_parser import FilterParser


class EnhancedTableViewResolver(Resolver):
    """Enhanced resolver supporting multiple HTTP methods and JSON filter format."""

    def __init__(self, oid: str, introspection: Introspection):
        cls = introspection.get_class(oid)
        if cls is None:
            raise ValueError(f"Class with OID {oid} not found in introspection data.")
        
        self.cls = cls
        self.name = cls.relname
        self.oid = oid
        self.schema = introspection.get_namespace(cls.relnamespace).nspname
        self.introspection = introspection
        self.filter_parser = FilterParser(introspection)
        
        # Get table metadata
        self.type, self.fields, self.return_type = self._create_return_type(introspection)
        self.primary_key_columns = self.filter_parser.get_primary_key_columns(oid)
        self.unique_constraints = self.filter_parser.get_unique_constraints(oid)
        
        self.router = None

    def _create_return_type(self, introspection: Introspection) -> tuple[str, list[str], type]:
        """Create return type and field list for this table/view."""
        from pydantic import create_model
        from pydantic.alias_generators import to_camel
        
        field_definitions = {}
        fields = []
        
        for attr in introspection.get_attributes(self.oid):
            if attr.attisdropped:
                continue
                
            typ = attr.get_type(introspection)
            fields.append(attr.attname)
            
            attr_py_type = attr.get_py_type(introspection)
            field_definitions[attr.attname] = (
                attr_py_type,
                Field(introspection.get_description(introspection.PG_CLASS, typ.oid)),
            )
        
        return (
            self.cls.relkind,
            fields,
            create_model(
                to_camel(self.name),
                **field_definitions,
            ),
        )

    def mount(self, router: APIRouter):
        """Mount all endpoints for this table/view."""
        self.router = router
        
        base_path = f"/{self.schema}/{self.name}"
        
        # GET: Retrieve data with optional filters
        router.add_api_route(
            base_path,
            self.get_records,
            methods=["GET"],
            response_model=StandardResponse,
            summary=f"Get records from {self.schema}.{self.name}",
            description=f"Retrieve records from table/view {self.schema}.{self.name} with optional filtering and pagination.",
        )
        
        # POST: Complex queries and updates by primary key
        router.add_api_route(
            base_path,
            self.post_query_or_update,
            methods=["POST"],
            response_model=Union[StandardResponse, Dict[str, Any]],
            summary=f"Query or update records in {self.schema}.{self.name}",
            description=f"Execute complex queries or update records by primary key in {self.schema}.{self.name}.",
        )
        
        # PUT: Create new records
        router.add_api_route(
            base_path,
            self.put_create,
            methods=["PUT"],
            response_model=Union[List[Dict[str, Any]], Dict[str, Any]],
            summary=f"Create records in {self.schema}.{self.name}",
            description=f"Create new records in table {self.schema}.{self.name}.",
        )
        
        # DELETE: Delete records by primary key or unique constraint
        router.add_api_route(
            base_path,
            self.delete_records,
            methods=["DELETE"],
            response_model=Dict[str, Any],
            summary=f"Delete records from {self.schema}.{self.name}",
            description=f"Delete records from table {self.schema}.{self.name} by primary key or unique constraint.",
        )

    async def get_records(
        self,
        # For GET requests, we'll accept query parameters instead of body
        limit: Optional[int] = 100,
        offset: Optional[int] = 0,
        select_fields: Optional[str] = None,
        where_conditions: Optional[str] = None
    ) -> StandardResponse:
        """GET: Retrieve records with optional filtering."""
        try:
            # Convert query parameters to FilterRequest
            filter_request = FilterRequest(
                pagination=PaginationParams(limit=limit, offset=offset)
            )
            
            # Parse select fields if provided (comma-separated)
            if select_fields:
                fields = [field.strip() for field in select_fields.split(",")]
                filter_request.select = SelectClause(fields=fields)
            
            # For now, we'll skip complex where parsing from URL parameters
            # In a real implementation, you'd want a more sophisticated parser
            
            # Parse filter request into queries
            main_query, count_query = self.filter_parser.parse_filter_request(
                filter_request, self.name, self.schema
            )
            
            async with self.router._pool.acquire() as conn:
                # Get total count
                count_sql, count_params = count_query.build()
                count_result = await conn.fetchval(count_sql, *count_params)
                total = count_result or 0
                
                # Get main results
                main_sql, main_params = main_query.build()
                rows = await conn.fetch(main_sql, *main_params)
                
                results = [dict(row) for row in rows]
                
                # Build pagination metadata
                pagination_meta = None
                if filter_request.pagination:
                    pagination_meta = {
                        "limit": filter_request.pagination.limit,
                        "offset": filter_request.pagination.offset,
                        "total": total,
                        "has_more": (filter_request.pagination.offset or 0) + len(results) < total
                    }
                
                return StandardResponse(
                    results=results,
                    total=total,
                    pagination=pagination_meta
                )
                
        except Exception as e:
            logging.error(f"Error in get_records: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve records: {str(e)}"
            )

    async def post_query_or_update(
        self,
        request: Union[FilterRequest, UpdateRequest]
    ) -> Union[StandardResponse, Dict[str, Any]]:
        """POST: Handle complex queries or updates by primary key."""
        try:
            if isinstance(request, UpdateRequest):
                return await self._handle_update(request)
            else:
                # Handle as complex query (same as GET but with POST body)
                return await self.get_records(request)
                
        except Exception as e:
            logging.error(f"Error in post_query_or_update: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to process request: {str(e)}"
            )

    async def put_create(
        self,
        request: CreateRequest
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """PUT: Create new records."""
        try:
            if not self._is_table():
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Cannot create records in views"
                )
            
            # Handle both single record and batch creation
            data_to_insert = request.data if isinstance(request.data, list) else [request.data]
            
            created_records = []
            async with self.router._pool.acquire() as conn:
                for record_data in data_to_insert:
                    # Build INSERT query
                    columns = list(record_data.keys())
                    values = list(record_data.values())
                    
                    column_list = ", ".join(f'"{col}"' for col in columns)
                    value_placeholders = ", ".join(f"${i+1}" for i in range(len(values)))
                    
                    returning_clause = ", ".join(f'"{field}"' for field in self.fields)
                    
                    insert_sql = f'''
                        INSERT INTO "{self.schema}"."{self.name}" ({column_list})
                        VALUES ({value_placeholders})
                        RETURNING {returning_clause}
                    '''
                    
                    result = await conn.fetchrow(insert_sql, *values)
                    created_records.append(dict(result))
            
            return created_records if isinstance(request.data, list) else created_records[0]
            
        except Exception as e:
            logging.error(f"Error in put_create: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create records: {str(e)}"
            )

    async def delete_records(
        self,
        request: PrimaryKeyRequest
    ) -> Dict[str, Any]:
        """DELETE: Delete records by primary key or unique constraint."""
        try:
            if not self._is_table():
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Cannot delete records from views"
                )
            
            # Validate that the provided keys match a primary key or unique constraint
            provided_keys = set(request.values.keys())
            
            valid_key_combination = False
            if set(self.primary_key_columns) == provided_keys:
                valid_key_combination = True
            else:
                for unique_constraint in self.unique_constraints:
                    if set(unique_constraint) == provided_keys:
                        valid_key_combination = True
                        break
            
            if not valid_key_combination:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Provided keys must match a primary key or unique constraint"
                )
            
            # Build WHERE condition
            where_condition = self.filter_parser.build_key_condition(request.values)
            
            # Build DELETE query with the where condition
            from pghatch.query_builder.builder import Query
            from pglast.ast import DeleteStmt, RangeVar
            from pglast.stream import RawStream
            
            delete_stmt = DeleteStmt(
                relations=[RangeVar(
                    relname=self.name,
                    schemaname=self.schema,
                    inh=True
                )],
                whereClause=where_condition.node
            )
            
            delete_sql = RawStream()(delete_stmt)
            
            async with self.router._pool.acquire() as conn:
                result = await conn.execute(delete_sql)
                
                # Extract number of deleted rows from result string
                deleted_count = 0
                if result.startswith("DELETE "):
                    try:
                        deleted_count = int(result.split(" ")[1])
                    except (IndexError, ValueError):
                        deleted_count = 0
                
                return {
                    "deleted": deleted_count,
                    "message": f"Deleted {deleted_count} record(s)"
                }
                
        except Exception as e:
            logging.error(f"Error in delete_records: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to delete records: {str(e)}"
            )

    async def _handle_update(self, request: UpdateRequest) -> Dict[str, Any]:
        """Handle update operations by primary key or unique constraint."""
        if not self._is_table():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot update records in views"
            )
        
        # Validate key combination
        provided_keys = set(request.key.values.keys())
        
        valid_key_combination = False
        if set(self.primary_key_columns) == provided_keys:
            valid_key_combination = True
        else:
            for unique_constraint in self.unique_constraints:
                if set(unique_constraint) == provided_keys:
                    valid_key_combination = True
                    break
        
        if not valid_key_combination:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Provided keys must match a primary key or unique constraint"
            )
        
        # Build UPDATE query
        set_clauses = []
        values = []
        param_counter = 1
        
        for field, value in request.data.items():
            set_clauses.append(f'"{field}" = ${param_counter}')
            values.append(value)
            param_counter += 1
        
        where_clauses = []
        for field, value in request.key.values.items():
            where_clauses.append(f'"{field}" = ${param_counter}')
            values.append(value)
            param_counter += 1
        
        returning_clause = ", ".join(f'"{field}"' for field in self.fields)
        
        update_sql = f'''
            UPDATE "{self.schema}"."{self.name}"
            SET {", ".join(set_clauses)}
            WHERE {" AND ".join(where_clauses)}
            RETURNING {returning_clause}
        '''
        
        async with self.router._pool.acquire() as conn:
            result = await conn.fetchrow(update_sql, *values)
            
            if result is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Record not found"
                )
            
            return dict(result)

    def _is_table(self) -> bool:
        """Check if this is a table (not a view) that supports modifications."""
        return self.cls.relkind in ('r', 'p')  # Regular table or partitioned table