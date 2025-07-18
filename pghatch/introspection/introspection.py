from typing import Any

from asyncpg import Connection
from pydantic import BaseModel

from pghatch.introspection.tables import (
    PgDatabase,
    PgNamespace,
    PgClass,
    PgAttribute,
    PgConstraint,
    PgProc,
    PgAuthMembers,
    PgType,
    PgEnum,
    PgExtension,
    PgIndex,
    PgInherits,
    PgLanguage,
    PgPolicy,
    PgRange,
    PgDepend,
    PgDescription,
    PgAm,
    PgRoles,
)


class Introspection(BaseModel):
    database: "PgDatabase"
    namespaces: list["PgNamespace"]
    classes: list["PgClass"]
    attributes: list["PgAttribute"]
    constraints: list["PgConstraint"]
    procs: list["PgProc"]
    roles: list["PgRoles"]
    auth_members: list["PgAuthMembers"]
    types: list["PgType"]
    enums: list["PgEnum"]
    extensions: list["PgExtension"]
    indexes: list["PgIndex"]
    inherits: list["PgInherits"]
    languages: list["PgLanguage"]
    policies: list["PgPolicy"]
    ranges: list["PgRange"]
    depends: list["PgDepend"]
    descriptions: list["PgDescription"]
    am: list["PgAm"]

    catalog_by_oid: dict[str, str]
    oid_by_catalog: dict[str, str] = {}

    current_user: str
    pg_version: str
    introspection_version: int = 1

    include_extension_resources: bool = True

    PG_NAMESPACE: str | None = None
    PG_CLASS: str | None = None
    PG_PROC: str | None = None
    PG_TYPE: str | None = None
    PG_CONSTRAINT: str | None = None
    PG_EXTENSION: str | None = None

    @classmethod
    def del_items(cls, delete: list[Any], collection: list[Any], attr: str) -> None:
        for del_item in delete:
            for item in collection:
                if getattr(item, attr, None) == del_item:
                    collection.remove(item)

    def model_post_init(self, __context):
        """Post-initialization to set up catalog mappings."""
        self.oid_by_catalog = {
            catalog: oid for oid, catalog in self.catalog_by_oid.items()
        }

        self.PG_NAMESPACE = self.oid_by_catalog.get("pg_namespace")
        self.PG_CLASS = self.oid_by_catalog.get("pg_class")
        self.PG_PROC = self.oid_by_catalog.get("pg_proc")
        self.PG_TYPE = self.oid_by_catalog.get("pg_type")
        self.PG_CONSTRAINT = self.oid_by_catalog.get("pg_constraint")

        if not all(
                [
                    self.PG_NAMESPACE,
                    self.PG_CLASS,
                    self.PG_PROC,
                    self.PG_TYPE,
                    self.PG_CONSTRAINT,
                ]
        ):
            raise ValueError(
                "Invalid introspection results; could not determine the ids of the system catalogs"
            )

        if not self.include_extension_resources:
            extension_proc_oids = set()
            extension_class_oids = set()

            for dependency in self.depends:
                if (
                        dependency.deptype == "e"
                        and self.oid_by_catalog.get("pg_extension") == dependency.refclassid
                ):
                    if dependency.classid == self.oid_by_catalog.get("pg_proc"):
                        extension_proc_oids.add(dependency.objid)
                    elif dependency.classid == self.oid_by_catalog.get("pg_class"):
                        extension_class_oids.add(dependency.objid)

            extension_proc_oids = list(extension_proc_oids)
            extension_class_oids = list(extension_class_oids)
            self.del_items(extension_proc_oids, self.procs, "oid")
            self.del_items(extension_class_oids, self.classes, "oid")
            self.del_items(extension_class_oids, self.attributes, "attrelid")
            self.del_items(extension_class_oids, self.constraints, "conrelid")
            self.del_items(extension_class_oids, self.constraints, "confrelid")
            self.del_items(extension_class_oids, self.type, "typrelid")

    def get_role(self, oid: str | None = None) -> "PgRoles | None":
        """Get a role by its OID."""
        for role in self.roles:
            if role.oid == oid:
                return role
        return None

    def get_namespace(self, id: str | None) -> "PgNamespace | None":
        return next(
            (ns for ns in self.namespaces if getattr(ns, "oid", None) == id), None
        )

    def get_type(self, id: str | None) -> "PgType | None":
        return next((t for t in self.types if getattr(t, "oid", None) == id), None)

    def get_class(self, id: str | None) -> "PgClass | None":
        return next((c for c in self.classes if getattr(c, "oid", None) == id), None)

    def get_range(self, id: str | None) -> "PgRange | None":
        return next(
            (r for r in self.ranges if getattr(r, "rngtypid", None) == id), None
        )

    def get_attributes(self, id: str | None) -> list["PgAttribute"]:
        return sorted(
            [a for a in self.attributes if getattr(a, "attrelid", None) == id],
            key=lambda a: getattr(a, "attnum", 0),
        )

    def get_constraints(self, id: str | None) -> list["PgConstraint"]:
        return sorted(
            [c for c in self.constraints if getattr(c, "conrelid", None) == id],
            key=lambda c: getattr(c, "conname", ""),
        )

    def get_foreign_constraints(self, id: str | None) -> list["PgConstraint"]:
        return [c for c in self.constraints if getattr(c, "confrelid", None) == id]

    def get_enums(self, id: str | None) -> list["PgEnum"]:
        return sorted(
            [e for e in self.enums if getattr(e, "enumtypid", None) == id],
            key=lambda e: getattr(e, "enumsortorder", 0),
        )

    def get_indexes(self, id: str | None) -> list["PgIndex"]:
        return [i for i in self.indexes if getattr(i, "indrelid", None) == id]

    def get_description(
            self, classoid: str, objoid: str, objsubid: int | None = None
    ) -> str | None:
        if objsubid is None:
            desc = next(
                (
                    d
                    for d in self.descriptions
                    if d.classoid == classoid and d.objoid == objoid
                ),
                None,
            )
        else:
            desc = next(
                (
                    d
                    for d in self.descriptions
                    if d.classoid == classoid
                       and d.objoid == objoid
                       and d.objsubid == objsubid
                ),
                None,
            )
        return getattr(desc, "description", None) if desc else None

    def get_tags_and_description(
            self,
            classoid: str,
            objoid: str,
            objsubid: int | None = None,
            fallback: dict | None = None,
    ) -> dict:
        description = self.get_description(classoid, objoid, objsubid)
        if description is None and fallback:
            description = self.get_description(
                fallback.get("classoid"),
                fallback.get("objoid"),
                fallback.get("objsubid"),
            )
        # parseSmartComment is not defined; return as dict for now
        return {"description": description}

    def get_current_user(self):
        return next((r for r in self.roles if r.rolname == self.current_user), None)

    def get_constraint(self, by: dict) -> "PgConstraint | None":
        return next(
            (c for c in self.constraints if getattr(c, "oid", None) == by.get("oid")),
            None,
        )

    def get_proc(self, id: str) -> "PgProc | None":
        return next(
            (c for c in self.procs if getattr(c, "oid", None) == id), None
        )

    def get_roles(self, by: dict) -> "PgRoles | None":
        return next(
            (c for c in self.roles if getattr(c, "oid", None) == by.get("oid")), None
        )

    def get_enum(self, by: dict) -> "PgEnum | None":
        return next(
            (c for c in self.enums if getattr(c, "oid", None) == by.get("oid")), None
        )

    def get_extension(self, by: dict) -> "PgExtension | None":
        return next(
            (c for c in self.extensions if getattr(c, "_id", None) == by.get("id")),
            None,
        )

    def get_index(self, by: dict) -> "PgIndex | None":
        return next(
            (c for c in self.indexes if getattr(c, "indexrelid", None) == by.get("id")),
            None,
        )

    def get_language(self, by: dict) -> "PgLanguage | None":
        return next(
            (c for c in self.languages if getattr(c, "_id", None) == by.get("id")), None
        )


async def make_introspection_query(conn: Connection) -> Introspection:
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

    result = await conn.fetchval(introspection_query)
    if result:
        introspection = Introspection.model_validate_json(result)
        return introspection
    else:
        raise ValueError("No introspection data found.")
