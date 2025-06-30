import logging

from pydantic import BaseModel
from psycopg import connect

from pgrestcue.introspection.tables import PgDatabase, PgNamespace, PgClass, PgAttribute, PgConstraint, PgProc, \
    PgAuthMembers, PgType, PgEnum, PgExtension, PgIndex, PgInherits, PgLanguage, PgPolicy, PgRange, PgDepend, \
    PgDescription, PgAm, PgRoles


class Introspection(BaseModel):
    database: 'PgDatabase'
    namespaces: list['PgNamespace']
    classes: list['PgClass']
    attributes: list['PgAttribute']
    constraints: list['PgConstraint']
    procs: list['PgProc']
    roles: list['PgRoles']
    auth_members: list['PgAuthMembers']
    types: list['PgType']
    enums: list['PgEnum']
    extensions: list['PgExtension']
    indexes: list['PgIndex']
    inherits: list['PgInherits']
    languages: list['PgLanguage']
    policies: list['PgPolicy']
    ranges: list['PgRange']
    depends: list['PgDepend']
    descriptions: list['PgDescription']
    am: list['PgAm']

    catalog_by_oid: dict[str, str]

    current_user: str
    pg_version: str
    introspection_version: int = 1

    @property
    def oid_by_catalog(self) -> dict[str, str]:
        """Return a mapping from catalog name to OID."""
        return {catalog: oid for oid, catalog in self.catalog_by_oid.items()}

    def get_role(self, oid: str | None = None) -> 'PgRoles | None':
        """Get a role by its OID."""
        for role in self.roles:
            if role.oid == oid:
                return role
        return None

    def get_namespace(self, id: str | None) -> 'PgNamespace | None':
        return next((ns for ns in self.namespaces if getattr(ns, 'oid', None) == id), None)

    def get_type(self, id: str | None) -> 'PgType | None':
        return next((t for t in self.types if getattr(t, 'oid', None) == id), None)

    def get_class(self, id: str | None) -> 'PgClass | None':
        return next((c for c in self.classes if getattr(c, 'oid', None) == id), None)

    def get_range(self, id: str | None) -> 'PgRange | None':
        return next((r for r in self.ranges if getattr(r, 'rngtypid', None) == id), None)

    def get_attributes(self, id: str | None) -> list['PgAttribute']:
        return sorted(
            [a for a in self.attributes if getattr(a, 'attrelid', None) == id],
            key=lambda a: getattr(a, 'attnum', 0)
        )

    def get_constraints(self, id: str | None) -> list['PgConstraint']:
        return sorted(
            [c for c in self.constraints if getattr(c, 'conrelid', None) == id],
            key=lambda c: getattr(c, 'conname', '')
        )

    def get_foreign_constraints(self, id: str | None) -> list['PgConstraint']:
        return [c for c in self.constraints if getattr(c, 'confrelid', None) == id]

    def get_enums(self, id: str | None) -> list['PgEnum']:
        return sorted(
            [e for e in self.enums if getattr(e, 'enumtypid', None) == id],
            key=lambda e: getattr(e, 'enumsortorder', 0)
        )

    def get_indexes(self, id: str | None) -> list['PgIndex']:
        return [i for i in self.indexes if getattr(i, 'indrelid', None) == id]

    def get_description(self, classoid: str, objoid: str, objsubid: int | None = None) -> str | None:
        if objsubid is None:
            desc = next((d for d in self.descriptions if d.classoid == classoid and d.objoid == objoid), None)
        else:
            desc = next((d for d in self.descriptions if
                         d.classoid == classoid and d.objoid == objoid and d.objsubid == objsubid), None)
        return getattr(desc, 'description', None) if desc else None

    def get_tags_and_description(self, classoid: str, objoid: str, objsubid: int | None = None,
                                 fallback: dict | None = None) -> dict:
        description = self.get_description(classoid, objoid, objsubid)
        if description is None and fallback:
            description = self.get_description(
                fallback.get('classoid'),
                fallback.get('objoid'),
                fallback.get('objsubid'),
            )
        # parseSmartComment is not defined; return as dict for now
        return {'description': description}

    def get_current_user(self):
        return next((r for r in self.roles if r.rolname == self.current_user), None)

    def get_namespace(self, by: dict) -> 'PgNamespace | None':
        if 'id' in by and by['id']:
            return self.get_namespace(by['id'])
        elif 'name' in by and by['name']:
            return next((entity for entity in self.namespaces if getattr(entity, 'nspname', None) == by['name']), None)
        return None

    def get_class(self, by: dict) -> 'PgClass | None':
        return self.get_class(by['oid']) if 'oid' in by else None

    def get_constraint(self, by: dict) -> 'PgConstraint | None':
        return next((c for c in self.constraints if getattr(c, 'oid', None) == by.get('oid')), None)

    def get_proc(self, by: dict) -> 'PgProc | None':
        return next((c for c in self.procs if getattr(c, 'oid', None) == by.get('oid')), None)

    def get_roles(self, by: dict) -> 'PgRoles | None':
        return next((c for c in self.roles if getattr(c, 'oid', None) == by.get('oid')), None)

    def get_enum(self, by: dict) -> 'PgEnum | None':
        return next((c for c in self.enums if getattr(c, 'oid', None) == by.get('oid')), None)

    def get_extension(self, by: dict) -> 'PgExtension | None':
        return next((c for c in self.extensions if getattr(c, '_id', None) == by.get('id')), None)

    def get_index(self, by: dict) -> 'PgIndex | None':
        return next((c for c in self.indexes if getattr(c, 'indexrelid', None) == by.get('id')), None)

    def get_language(self, by: dict) -> 'PgLanguage | None':
        return next((c for c in self.languages if getattr(c, '_id', None) == by.get('id')), None)


def make_introspection_query() -> Introspection:
    introspection_query = """
                          with database as (select *
                                            from pg_catalog.pg_database
                                            where datname = current_database()),

                               namespaces as (select *
                                              from pg_catalog.pg_namespace
                                              where nspname <> 'information_schema'),

                               classes as (select *,
                                                  pg_catalog.pg_relation_is_updatable(oid, true) ::bit(8)::int4 as "updatable_mask"
                                           from pg_catalog.pg_class
                                           where relnamespace in (select namespaces.oid
                                                                  from namespaces
                                                                  where nspname <> 'information_schema'
                                                                    and nspname not like 'pg\\_%')),

                               attributes as (select *
                                              from pg_catalog.pg_attribute
                                              where attrelid in (select classes.oid from classes)
                                                AND attnum > 0),

                               constraints as (select *
                                               from pg_catalog.pg_constraint
                                               where connamespace in (select namespaces.oid
                                                                      from namespaces
                                                                      where nspname <> 'information_schema'
                                                                        and nspname not like 'pg\\_%')),

                               procs as (select *
                                         from pg_catalog.pg_proc
                                         where pronamespace in (select namespaces.oid
                                                                from namespaces
                                                                where nspname <> 'information_schema'
                                                                  and nspname not like 'pg\\_%')
                                           and prorettype operator(pg_catalog.<>) 2279
                              ), roles as (
                          select *
                          from pg_catalog.pg_roles
                              ), auth_members as (
                          select *
                          from pg_catalog.pg_auth_members
                          where roleid in (select roles.oid from roles)
                              )
                              , types as (
                          select *
                          from pg_catalog.pg_type
                          where (typnamespace in (select namespaces.oid from namespaces where nspname <> 'information_schema'
                            and nspname not like 'pg\\_%'))
                             or (typnamespace = 'pg_catalog'::regnamespace)
                              )
                              , enums as (
                          select *
                          from pg_catalog.pg_enum
                          where enumtypid in (select types.oid from types)
                              )
                              , extensions as (
                          select *
                          from pg_catalog.pg_extension
                              ), indexes as (
                          select *
                          from pg_catalog.pg_index
                          where indrelid in (select classes.oid from classes)
                              )
                              , inherits as (
                          select *
                          from pg_catalog.pg_inherits
                          where inhrelid in (select classes.oid from classes)
                              )
                              , languages as (
                          select *
                          from pg_catalog.pg_language
                              ), policies as (
                          select *
                          from pg_catalog.pg_policy
                          where polrelid in (select classes.oid from classes)
                              )
                              , ranges as (
                          select *
                          from pg_catalog.pg_range
                          where rngtypid in (select types.oid from types)
                              )
                              , depends as (
                          select *
                          from pg_catalog.pg_depend
                          where deptype IN ('a'
                              , 'e')
                            and (
                              (classid = 'pg_catalog.pg_namespace'::regclass
                            and objid in (select namespaces.oid from namespaces))
                             or (classid = 'pg_catalog.pg_class'::regclass
                            and objid in (select classes.oid from classes))
                             or (classid = 'pg_catalog.pg_attribute'::regclass
                            and objid in (select classes.oid from classes)
                            and objsubid
                              > 0)
                             or (classid = 'pg_catalog.pg_constraint'::regclass
                            and objid in (select constraints.oid from constraints))
                             or (classid = 'pg_catalog.pg_proc'::regclass
                            and objid in (select procs.oid from procs))
                             or (classid = 'pg_catalog.pg_type'::regclass
                            and objid in (select types.oid from types))
                             or (classid = 'pg_catalog.pg_enum'::regclass
                            and objid in (select enums.oid from enums))
                             or (classid = 'pg_catalog.pg_extension'::regclass
                            and objid in (select extensions.oid from extensions))
                              )
                              )
                              , descriptions as (
                          select *
                          from pg_catalog.pg_description
                          where (
                              (classoid = 'pg_catalog.pg_namespace'::regclass
                            and objoid in (select namespaces.oid from namespaces))
                             or (classoid = 'pg_catalog.pg_class'::regclass
                            and objoid in (select classes.oid from classes))
                             or (classoid = 'pg_catalog.pg_attribute'::regclass
                            and objoid in (select classes.oid from classes)
                            and objsubid
                              > 0)
                             or (classoid = 'pg_catalog.pg_constraint'::regclass
                            and objoid in (select constraints.oid from constraints))
                             or (classoid = 'pg_catalog.pg_proc'::regclass
                            and objoid in (select procs.oid from procs))
                             or (classoid = 'pg_catalog.pg_type'::regclass
                            and objoid in (select types.oid from types))
                             or (classoid = 'pg_catalog.pg_enum'::regclass
                            and objoid in (select enums.oid from enums))
                             or (classoid = 'pg_catalog.pg_extension'::regclass
                            and objoid in (select extensions.oid from extensions))
                              )
                              )
                              , am as (
                          select *
                          from pg_catalog.pg_am
                          where true
                              )
                          select json_build_object(
                                         'database',
                                         (select row_to_json(database) from database),
                                         'namespaces',
                                         (select coalesce(
                                                         (select json_agg(row_to_json(namespaces) order by nspname) from namespaces),
                                                         '[]' ::json)),
                                         'classes',
                                         (select coalesce(
                                                         (select json_agg(row_to_json(classes) order by relnamespace, relname) from classes),
                                                         '[]' ::json)),
                                         'attributes',
                                         (select coalesce(
                                                         (select json_agg(row_to_json(attributes) order by attrelid, attnum) from attributes),
                                                         '[]' ::json)),
                                         'constraints',
                                         (select coalesce(
                                                         (select json_agg(row_to_json(constraints) order by connamespace, conrelid, conname) from constraints),
                                                         '[]' ::json)),
                                         'procs',
                                         (select coalesce((select json_agg(row_to_json(procs) order by pronamespace,
                                                                           proname,
                                                                           pg_get_function_identity_arguments(procs.oid))
                                                           from procs), '[]' ::json)),
                                         'roles',
                                         (select coalesce(
                                                         (select json_agg(row_to_json(roles) order by rolname) from roles),
                                                         '[]' ::json)),
                                         'auth_members',
                                         (select coalesce(
                                                         (select json_agg(row_to_json(auth_members) order by roleid, member, grantor)
                                                          from auth_members), '[]' ::json)),
                                         'types',
                                         (select coalesce(
                                                         (select json_agg(row_to_json(types) order by typnamespace, typname) from types),
                                                         '[]' ::json)),
                                         'enums',
                                         (select coalesce(
                                                         (select json_agg(row_to_json(enums) order by enumtypid, enumsortorder) from enums),
                                                         '[]' ::json)),
                                         'extensions',
                                         (select coalesce(
                                                         (select json_agg(row_to_json(extensions) order by extname) from extensions),
                                                         '[]' ::json)),
                                         'indexes',
                                         (select coalesce(
                                                         (select json_agg(row_to_json(indexes) order by indrelid, indexrelid) from indexes),
                                                         '[]' ::json)),
                                         'inherits',
                                         (select coalesce(
                                                         (select json_agg(row_to_json(inherits) order by inhrelid, inhseqno) from inherits),
                                                         '[]' ::json)),
                                         'languages',
                                         (select coalesce(
                                                         (select json_agg(row_to_json(languages) order by lanname) from languages),
                                                         '[]' ::json)),
                                         'policies',
                                         (select coalesce(
                                                         (select json_agg(row_to_json(policies) order by polrelid, polname) from policies),
                                                         '[]' ::json)),
                                         'ranges',
                                         (select coalesce(
                                                         (select json_agg(row_to_json(ranges) order by rngtypid) from ranges),
                                                         '[]' ::json)),
                                         'depends',
                                         (select coalesce((select json_agg(row_to_json(depends) order by classid, objid,
                                                                           objsubid, refclassid, refobjid, refobjsubid)
                                                           from depends), '[]' ::json)),
                                         'descriptions',
                                         (select coalesce(
                                                         (select json_agg(row_to_json(descriptions) order by objoid, classoid, objsubid)
                                                          from descriptions), '[]' ::json)),
                                         'am',
                                         (select coalesce((select json_agg(row_to_json(am) order by amname) from am), '[]'::json)),
                                         'catalog_by_oid',
                                         (select json_object_agg(oid::text, relname order by relname asc)
                                          from pg_class
                                          where relnamespace = (select oid
                                                                from pg_namespace
                                                                where nspname = 'pg_catalog')
                                            and relkind = 'r'),
                                         'current_user',
                                         current_user,
                                         'pg_version',
                                         version(),
                                         'introspection_version',
                                         1
                                 ) ::text as introspection \
                          """

    connection = connect("dbname=postgres user=postgres password=postgres host=localhost port=5432")
    with connection.cursor() as cursor:
        cursor.execute(introspection_query)
        result = cursor.fetchone()
        if result:
            introspection = Introspection.model_validate_json(result[0])
            oid_by_catalog = introspection.oid_by_catalog
            PG_NAMESPACE = oid_by_catalog.get("pg_namespace")
            PG_CLASS = oid_by_catalog.get("pg_class")
            PG_PROC = oid_by_catalog.get("pg_proc")
            PG_TYPE = oid_by_catalog.get("pg_type")
            PG_CONSTRAINT = oid_by_catalog.get("pg_constraint")
            PG_EXTENSION = oid_by_catalog.get("pg_extension")
            if not all([PG_NAMESPACE, PG_CLASS, PG_PROC, PG_TYPE, PG_CONSTRAINT, PG_EXTENSION]):
                raise ValueError(
                    "Invalid introspection results; could not determine the ids of the system catalogs"
                )
            return introspection
        else:
            raise ValueError("No introspection data found.")


TYPEMAP = {
    "bool": bool,
    "boolean": bool,
    "text": str,
    "varchar": str,
    "char": str,
    "character varying": str,
    "character": str,
    "uuid": str,
    "date": str,
    "timestamp": str,
    "timestamp without time zone": str,
    "timestamp with time zone": str,
    "time": str,
    "time without time zone": str,
    "time with time zone": str,
    "interval": str,  # Use str for interval; can be parsed later
}


from sqlalchemy import BOOLEAN, String, Date, DateTime, Time, Interval

ALCHEMY_TYPEMAP = {
    "bool": BOOLEAN,
    "boolean": BOOLEAN,
    "text": String,
    "varchar": String,
    "char": String,
    "character varying": String,
    "character": String,
    "uuid": String,
    "date": Date,
    "timestamp": DateTime,
    "timestamp without time zone": DateTime,
    "timestamp with time zone": DateTime,
    "time": Time,
    "time without time zone": Time,
    "time with time zone": Time,
    "interval": Interval,  # Use Interval for interval; can be parsed later
}


if __name__ == '__main__':
    from pydantic import create_model
    from sqlmodel import Field, Session, SQLModel, ARRAY, select
    from typing import Optional
    from sqlalchemy import Column, create_engine

    from sqlalchemy import Table, event
    from sqlalchemy.ext.compiler import compiles
    from sqlalchemy import Integer

    class RowID(Column):
        pass


    @compiles(RowID)
    def compile_mycolumn(element, compiler, **kw):
        return "1 as row_id"


    @event.listens_for(Table, "after_parent_attach")
    def after_parent_attach(target, parent):
        if not target.primary_key:
            # if no pkey create our own one based on returned rowid
            # this is untested for writing stuff - likely wont work
            logging.info("No pkey defined for table, using rownumber %s", target)
            target.append_column(RowID('row_id', Integer, primary_key=True))

    introspection = make_introspection_query()
    for tablelike in introspection.classes:
        print(f"Table: {tablelike.relname} (OID: {tablelike.oid})")

        field_definitions = {}

        for attr in introspection.get_attributes(tablelike.oid):
            is_nullable = not attr.attnotnull
            is_primary_key = False
            typname = introspection.get_type(attr.atttypid).typname
            is_array = typname.startswith("_")

            if is_array:
                typname = typname[1:]

            attr_type = TYPEMAP[typname]
            alchemy_type = ALCHEMY_TYPEMAP[typname]
            attr_type = attr_type if not is_array else list  # Use list for arrays

            if is_array:
                attr_type = (attr_type, Field(sa_column=Column(ARRAY(alchemy_type), nullable=is_nullable)))
                field_definitions[attr.attname] = attr_type
            else:
                attr_type = (attr_type, Field(sa_column=Column(alchemy_type, nullable=is_nullable, primary_key=is_primary_key)))
                field_definitions[attr.attname] = attr_type

            print(
                f"  Attribute: {attr.attname} (Type: {introspection.get_type(attr.atttypid).typname}, Nullable: {is_nullable})")


        field_definitions["row_id"] = (int, Field(sa_column=RowID(nullable=False, primary_key=True)))

        table = create_model(
            tablelike.relname,
            __base__=SQLModel,
            __cls_kwargs__={"table": True},
            **field_definitions,
        )

        # dbname=postgres user=postgres password=postgres host=localhost port=5432
        engine = create_engine("postgresql://postgres:postgres@localhost:5432/postgres")
        with Session(engine) as session:
            statement = select(table)
            print(statement)

            results = session.exec(statement)
            for row in results:
                print(row)
