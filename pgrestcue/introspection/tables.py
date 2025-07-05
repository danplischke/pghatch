import logging
from datetime import datetime
from functools import lru_cache
from typing import Any, Optional, TYPE_CHECKING, Tuple

from pydantic import BaseModel
from sqlalchemy import ARRAY, BigInteger, Boolean, Column, DateTime, Index, Integer, LargeBinary, PrimaryKeyConstraint, \
    REAL, SmallInteger, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import OID
from sqlalchemy.sql.sqltypes import NullType
from sqlmodel import Field, SQLModel

if TYPE_CHECKING:
    from pgrestcue.introspection.introspection import Introspection
    from pgrestcue.introspection.acl import AclObject

RESERVED_WORDS = [
    "ABS",
    "ABSENT",
    "ACOS",
    "ALL",
    "ALLOCATE",
    "ANALYSE",
    "ANALYZE",
    "AND",
    "ANY",
    "ANY_VALUE",
    "ARE",
    "ARRAY",
    "ARRAY_AGG",
    "ARRAY_MAX_CARDINALITY",
    "AS",
    "ASC",
    "ASENSITIVE",
    "ASIN",
    "ASYMMETRIC",
    "ATAN",
    "ATOMIC",
    "AUTHORIZATION",
    "AVG",
    "BEGIN_FRAME",
    "BEGIN_PARTITION",
    "BETWEEN",
    "BIGINT",
    "BINARY",
    "BIT",
    "BIT_LENGTH",
    "BLOB",
    "BOOLEAN",
    "BOTH",
    "BTRIM",
    "CALL",
    "CARDINALITY",
    "CASE",
    "CAST",
    "CEIL",
    "CEILING",
    "CHAR",
    "CHARACTER",
    "CHARACTER_LENGTH",
    "CHAR_LENGTH",
    "CHECK",
    "CLASSIFIER",
    "CLOB",
    "COALESCE",
    "COLLATE",
    "COLLATION",
    "COLLECT",
    "COLUMN",
    "CONCURRENTLY",
    "CONDITION",
    "CONNECT",
    "CONSTRAINT",
    "CONTAINS",
    "CONVERT",
    "CORR",
    "CORRESPONDING",
    "COS",
    "COSH",
    "COUNT",
    "COVAR_POP",
    "COVAR_SAMP",
    "CREATE",
    "CROSS",
    "CUME_DIST",
    "CURRENT_CATALOG",
    "CURRENT_DATE",
    "CURRENT_DEFAULT_TRANSFORM_GROUP",
    "CURRENT_PATH",
    "CURRENT_ROLE",
    "CURRENT_ROW",
    "CURRENT_SCHEMA",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "CURRENT_TRANSFORM_GROUP_FOR_TYPE",
    "CURRENT_USER",
    "DATALINK",
    "DATE",
    "DAY",
    "DEC",
    "DECFLOAT",
    "DECIMAL",
    "DEFAULT",
    "DEFERRABLE",
    "DEFINE",
    "DENSE_RANK",
    "DEREF",
    "DESC",
    "DESCRIBE",
    "DESCRIPTOR",
    "DETERMINISTIC",
    "DIAGNOSTICS",
    "DISCONNECT",
    "DISTINCT",
    "DLNEWCOPY",
    "DLPREVIOUSCOPY",
    "DLURLCOMPLETE",
    "DLURLCOMPLETEONLY",
    "DLURLCOMPLETEWRITE",
    "DLURLPATH",
    "DLURLPATHONLY",
    "DLURLPATHWRITE",
    "DLURLSCHEME",
    "DLURLSERVER",
    "DLVALUE",
    "DO",
    "DYNAMIC",
    "ELEMENT",
    "ELSE",
    "EMPTY",
    "END",
    "END-EXEC",
    "END_FRAME",
    "END_PARTITION",
    "EQUALS",
    "EVERY",
    "EXCEPT",
    "EXCEPTION",
    "EXEC",
    "EXISTS",
    "EXP",
    "EXTRACT",
    "FALSE",
    "FETCH",
    "FILTER",
    "FIRST_VALUE",
    "FLOAT",
    "FLOOR",
    "FOR",
    "FOREIGN",
    "FOUND",
    "FRAME_ROW",
    "FREE",
    "FREEZE",
    "FROM",
    "FULL",
    "FUSION",
    "GET",
    "GO",
    "GOTO",
    "GRANT",
    "GREATEST",
    "GROUP",
    "GROUPING",
    "GROUPS",
    "HAVING",
    "HOUR",
    "ILIKE",
    "IN",
    "INDICATOR",
    "INITIAL",
    "INITIALLY",
    "INNER",
    "INOUT",
    "INT",
    "INTEGER",
    "INTERSECT",
    "INTERSECTION",
    "INTERVAL",
    "INTO",
    "IS",
    "ISNULL",
    "JOIN",
    "JSON",
    "JSON_ARRAY",
    "JSON_ARRAYAGG",
    "JSON_EXISTS",
    "JSON_OBJECT",
    "JSON_OBJECTAGG",
    "JSON_QUERY",
    "JSON_SCALAR",
    "JSON_SERIALIZE",
    "JSON_TABLE",
    "JSON_TABLE_PRIMITIVE",
    "JSON_VALUE",
    "LAG",
    "LAST_VALUE",
    "LATERAL",
    "LEAD",
    "LEADING",
    "LEAST",
    "LEFT",
    "LIKE",
    "LIKE_REGEX",
    "LIMIT",
    "LISTAGG",
    "LN",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "LOG",
    "LOG10",
    "LOWER",
    "LPAD",
    "LTRIM",
    "MATCHES",
    "MATCH_NUMBER",
    "MATCH_RECOGNIZE",
    "MAX",
    "MAX_CARDINALITY",
    "MEASURES",
    "MEMBER",
    "MERGE",
    "MERGE_ACTION",
    "MIN",
    "MINUTE",
    "MOD",
    "MODIFIES",
    "MODULE",
    "MONTH",
    "MULTISET",
    "NATIONAL",
    "NATURAL",
    "NCHAR",
    "NCLOB",
    "NONE",
    "NORMALIZE",
    "NOT",
    "NOTNULL",
    "NTH_VALUE",
    "NTILE",
    "NULL",
    "NULLIF",
    "NUMERIC",
    "OCCURRENCES_REGEX",
    "OCTET_LENGTH",
    "OFFSET",
    "OMIT",
    "ON",
    "ONE",
    "ONLY",
    "OPEN",
    "OR",
    "ORDER",
    "OUT",
    "OUTER",
    "OUTPUT",
    "OVER",
    "OVERLAPS",
    "OVERLAY",
    "PAD",
    "PARAMETER",
    "PATTERN",
    "PER",
    "PERCENT",
    "PERCENTILE_CONT",
    "PERCENTILE_DISC",
    "PERCENT_RANK",
    "PERIOD",
    "PERMUTE",
    "PLACING",
    "PORTION",
    "POSITION",
    "POSITION_REGEX",
    "POWER",
    "PRECEDES",
    "PRECISION",
    "PRIMARY",
    "PTF",
    "RANK",
    "READS",
    "REAL",
    "REFERENCES",
    "REGR_AVGX",
    "REGR_AVGY",
    "REGR_COUNT",
    "REGR_INTERCEPT",
    "REGR_R2",
    "REGR_SLOPE",
    "REGR_SXX",
    "REGR_SXY",
    "REGR_SYY",
    "RESULT",
    "RETURN",
    "RETURNING",
    "RIGHT",
    "ROW",
    "ROW_NUMBER",
    "RPAD",
    "RTRIM",
    "RUNNING",
    "SCOPE",
    "SECOND",
    "SECTION",
    "SEEK",
    "SELECT",
    "SENSITIVE",
    "SESSION_USER",
    "SETOF",
    "SIMILAR",
    "SIN",
    "SINH",
    "SIZE",
    "SMALLINT",
    "SOME",
    "SPACE",
    "SPECIFIC",
    "SPECIFICTYPE",
    "SQLCODE",
    "SQLERROR",
    "SQLEXCEPTION",
    "SQLSTATE",
    "SQLWARNING",
    "SQRT",
    "STATIC",
    "STDDEV_POP",
    "STDDEV_SAMP",
    "SUBMULTISET",
    "SUBSET",
    "SUBSTRING",
    "SUBSTRING_REGEX",
    "SUCCEEDS",
    "SUM",
    "SYMMETRIC",
    "SYSTEM_TIME",
    "SYSTEM_USER",
    "TABLE",
    "TABLESAMPLE",
    "TAN",
    "TANH",
    "THEN",
    "TIME",
    "TIMESTAMP",
    "TIMEZONE_HOUR",
    "TIMEZONE_MINUTE",
    "TO",
    "TRAILING",
    "TRANSLATE",
    "TRANSLATE_REGEX",
    "TRANSLATION",
    "TREAT",
    "TRIM",
    "TRIM_ARRAY",
    "TRUE",
    "UESCAPE",
    "UNION",
    "UNIQUE",
    "UNMATCHED",
    "UNNEST",
    "UPPER",
    "USAGE",
    "USER",
    "USING",
    "VALUES",
    "VALUE_OF",
    "VARBINARY",
    "VARCHAR",
    "VARIADIC",
    "VARYING",
    "VAR_POP",
    "VAR_SAMP",
    "VERBOSE",
    "VERSIONING",
    "WHEN",
    "WHENEVER",
    "WHERE",
    "WIDTH_BUCKET",
    "WINDOW",
    "WITH",
    "WITHIN",
    "WITHOUT",
    "XMLAGG",
    "XMLATTRIBUTES",
    "XMLBINARY",
    "XMLCAST",
    "XMLCOMMENT",
    "XMLCONCAT",
    "XMLDOCUMENT",
    "XMLELEMENT",
    "XMLEXISTS",
    "XMLFOREST",
    "XMLITERATE",
    "XMLNAMESPACES",
    "XMLPARSE",
    "XMLPI",
    "XMLQUERY",
    "XMLROOT",
    "XMLSERIALIZE",
    "XMLTABLE",
    "XMLTEXT",
    "XMLVALIDATE",
    "YEAR",
]


class PgSQLFeatures(SQLModel, table=True):
    __tablename__ = 'sql_features'
    __table_args__ = {'schema': 'information_schema'}

    feature_id: str = Field(sa_column=Column('feature_id', String, primary_key=True))
    feature_name: str = Field(sa_column=Column('feature_name', String))
    sub_feature_id: str = Field(sa_column=Column('sub_feature_id', String))
    sub_feature_name: str = Field(sa_column=Column('sub_feature_name', String))
    is_supported: str = Field(sa_column=Column('is_supported', String))
    is_verified_by: str = Field(sa_column=Column('is_verified_by', String))
    comments: str = Field(sa_column=Column('comments', String))


class PgSQLImplementationInfo(SQLModel, table=True):
    __tablename__ = 'sql_implementation_info'
    __table_args__ = {'schema': 'information_schema'}

    implementation_info_id: str = Field(sa_column=Column('implementation_info_id', String, primary_key=True))
    implementation_info_name: str = Field(sa_column=Column('implementation_info_name', String))
    integer_value: Optional[int] = Field(default=None, sa_column=Column('integer_value', Integer))
    character_value: Optional[str] = Field(default=None, sa_column=Column('character_value', String))
    comments: str = Field(sa_column=Column('comments', String))


class PgSQLParts(SQLModel, table=True):
    __tablename__ = 'sql_parts'
    __table_args__ = {'schema': 'information_schema'}

    feature_id: str = Field(sa_column=Column('feature_id', String, primary_key=True))
    feature_name: str = Field(sa_column=Column('feature_name', String))
    is_supported: str = Field(sa_column=Column('is_supported', String))
    is_verified_by: str = Field(sa_column=Column('is_verified_by', String))
    comments: str = Field(sa_column=Column('comments', String))


class PgSQLSizing(SQLModel, table=True):
    __tablename__ = 'sql_sizing'
    __table_args__ = {'schema': 'information_schema'}

    sizing_id: int = Field(sa_column=Column('sizing_id', Integer, primary_key=True))
    sizing_name: str = Field(sa_column=Column('sizing_name', String))
    supported_value: int = Field(sa_column=Column('supported_value', Integer))
    comments: str = Field(sa_column=Column('comments', String))


class PgAggregate(SQLModel, table=True):
    __tablename__ = 'pg_aggregate'
    __table_args__ = (
        PrimaryKeyConstraint('aggfnoid', name='pg_aggregate_fnoid_index'),
        {'schema': 'pg_catalog'}
    )

    aggfnoid: Any = Field(sa_column=Column('aggfnoid', NullType, primary_key=True))
    aggkind: str = Field(sa_column=Column('aggkind', String))
    aggnumdirectargs: int = Field(sa_column=Column('aggnumdirectargs', SmallInteger))
    aggtransfn: Any = Field(sa_column=Column('aggtransfn', NullType))
    aggfinalfn: Any = Field(sa_column=Column('aggfinalfn', NullType))
    aggcombinefn: Any = Field(sa_column=Column('aggcombinefn', NullType))
    aggserialfn: Any = Field(sa_column=Column('aggserialfn', NullType))
    aggdeserialfn: Any = Field(sa_column=Column('aggdeserialfn', NullType))
    aggmtransfn: Any = Field(sa_column=Column('aggmtransfn', NullType))
    aggminvtransfn: Any = Field(sa_column=Column('aggminvtransfn', NullType))
    aggmfinalfn: Any = Field(sa_column=Column('aggmfinalfn', NullType))
    aggfinalextra: bool = Field(sa_column=Column('aggfinalextra', Boolean))
    aggmfinalextra: bool = Field(sa_column=Column('aggmfinalextra', Boolean))
    aggfinalmodify: str = Field(sa_column=Column('aggfinalmodify', String))
    aggmfinalmodify: str = Field(sa_column=Column('aggmfinalmodify', String))
    aggsortop: Any = Field(sa_column=Column('aggsortop', OID))
    aggtranstype: Any = Field(sa_column=Column('aggtranstype', OID))
    aggtransspace: int = Field(sa_column=Column('aggtransspace', Integer))
    aggmtranstype: Any = Field(sa_column=Column('aggmtranstype', OID))
    aggmtransspace: int = Field(sa_column=Column('aggmtransspace', Integer))
    agginitval: Optional[str] = Field(default=None, sa_column=Column('agginitval', Text))
    aggminitval: Optional[str] = Field(default=None, sa_column=Column('aggminitval', Text))


class PgAm(SQLModel, table=True):
    __tablename__ = 'pg_am'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_am_oid_index'),
        UniqueConstraint('amname', name='pg_am_name_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    amname: str = Field(sa_column=Column('amname', String))
    amhandler: Any = Field(sa_column=Column('amhandler', NullType))
    amtype: str = Field(sa_column=Column('amtype', String))


class PgAmop(SQLModel, table=True):
    __tablename__ = 'pg_amop'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_amop_oid_index'),
        UniqueConstraint('amopfamily', 'amoplefttype', 'amoprighttype', 'amopstrategy', name='pg_amop_fam_strat_index'),
        UniqueConstraint('amopopr', 'amoppurpose', 'amopfamily', name='pg_amop_opr_fam_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    amopfamily: Any = Field(sa_column=Column('amopfamily', OID))
    amoplefttype: Any = Field(sa_column=Column('amoplefttype', OID))
    amoprighttype: Any = Field(sa_column=Column('amoprighttype', OID))
    amopstrategy: int = Field(sa_column=Column('amopstrategy', SmallInteger))
    amoppurpose: str = Field(sa_column=Column('amoppurpose', String))
    amopopr: Any = Field(sa_column=Column('amopopr', OID))
    amopmethod: Any = Field(sa_column=Column('amopmethod', OID))
    amopsortfamily: Any = Field(sa_column=Column('amopsortfamily', OID))


class PgAmproc(SQLModel, table=True):
    __tablename__ = 'pg_amproc'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_amproc_oid_index'),
        UniqueConstraint('amprocfamily', 'amproclefttype', 'amprocrighttype', 'amprocnum',
                         name='pg_amproc_fam_proc_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    amprocfamily: Any = Field(sa_column=Column('amprocfamily', OID))
    amproclefttype: Any = Field(sa_column=Column('amproclefttype', OID))
    amprocrighttype: Any = Field(sa_column=Column('amprocrighttype', OID))
    amprocnum: int = Field(sa_column=Column('amprocnum', SmallInteger))
    amproc: Any = Field(sa_column=Column('amproc', NullType))


class PgAttrdef(SQLModel, table=True):
    __tablename__ = 'pg_attrdef'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_attrdef_oid_index'),
        UniqueConstraint('adrelid', 'adnum', name='pg_attrdef_adrelid_adnum_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    adrelid: Any = Field(sa_column=Column('adrelid', OID))
    adnum: int = Field(sa_column=Column('adnum', SmallInteger))
    adbin: Any = Field(sa_column=Column('adbin', NullType))


class PgAttribute(SQLModel, table=True):
    __tablename__ = 'pg_attribute'
    __table_args__ = (
        PrimaryKeyConstraint('attrelid', 'attnum', name='pg_attribute_relid_attnum_index'),
        UniqueConstraint('attrelid', 'attname', name='pg_attribute_relid_attnam_index'),
        {'schema': 'pg_catalog'}
    )

    attrelid: Any = Field(sa_column=Column('attrelid', OID, primary_key=True))
    attname: str = Field(sa_column=Column('attname', String))
    atttypid: Any = Field(sa_column=Column('atttypid', OID))
    attlen: int = Field(sa_column=Column('attlen', SmallInteger))
    attnum: int = Field(sa_column=Column('attnum', SmallInteger, primary_key=True))
    attcacheoff: int = Field(sa_column=Column('attcacheoff', Integer))
    atttypmod: int = Field(sa_column=Column('atttypmod', Integer))
    attndims: int = Field(sa_column=Column('attndims', SmallInteger))
    attbyval: bool = Field(sa_column=Column('attbyval', Boolean))
    attalign: str = Field(sa_column=Column('attalign', String))
    attstorage: str = Field(sa_column=Column('attstorage', String))
    attcompression: str = Field(sa_column=Column('attcompression', String))
    attnotnull: bool = Field(sa_column=Column('attnotnull', Boolean))
    atthasdef: bool = Field(sa_column=Column('atthasdef', Boolean))
    atthasmissing: bool = Field(sa_column=Column('atthasmissing', Boolean))
    attidentity: str = Field(sa_column=Column('attidentity', String))
    attgenerated: str = Field(sa_column=Column('attgenerated', String))
    attisdropped: bool = Field(sa_column=Column('attisdropped', Boolean))
    attislocal: bool = Field(sa_column=Column('attislocal', Boolean))
    attinhcount: int = Field(sa_column=Column('attinhcount', SmallInteger))
    attcollation: Any = Field(sa_column=Column('attcollation', OID))
    attstattarget: Optional[int] = Field(default=None, sa_column=Column('attstattarget', SmallInteger))
    attacl: Optional[Any] = Field(default=None, sa_column=Column('attacl', NullType))
    attoptions: Optional[list] = Field(default=None, sa_column=Column('attoptions', ARRAY(Text())))
    attfdwoptions: Optional[list] = Field(default=None, sa_column=Column('attfdwoptions', ARRAY(Text())))
    attmissingval: Optional[Any] = Field(default=None, sa_column=Column('attmissingval', NullType))

    def get_class(self, introspection: "Introspection") -> Optional["PgClass"]:
        return next(filter(lambda x: x.relnamespace == self.attrelid, introspection.classes))

    def get_type(self, introspection: "Introspection") -> Optional["PgType"]:
        return introspection.get_type(self.atttypid)

    def get_description(self, introspection: "Introspection") -> Optional[str]:
        return introspection.get_description(introspection.PG_CLASS, self.attrelid, self.attnum)

    def get_tags_and_description(self, introspection: "Introspection") -> Tuple[list, Optional[str]]:
        tags = introspection.get_tags(introspection.PG_CLASS, self.attrelid, self.attnum)
        description = introspection.get_description(introspection.PG_CLASS, self.attrelid, self.attnum)
        return tags, description

    def get_tags(self, introspection: "Introspection"):
        return self.get_tags_and_description(introspection)[0]

    def get_acl(self, introspection: "Introspection") -> Optional["AclObject"]:
        """
        Get the ACL (Access Control List) for this attribute.
        """
        return introspection.get_acl(introspection.PG_CLASS, self.attrelid, self.attnum)


class PgAuthMembers(SQLModel, table=True):
    __tablename__ = 'pg_auth_members'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_auth_members_oid_index'),
        UniqueConstraint('member', 'roleid', 'grantor', name='pg_auth_members_member_role_index'),
        UniqueConstraint('roleid', 'member', 'grantor', name='pg_auth_members_role_member_index'),
        Index('pg_auth_members_grantor_index', 'grantor'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    roleid: Any = Field(sa_column=Column('roleid', OID))
    member: Any = Field(sa_column=Column('member', OID))
    grantor: Any = Field(sa_column=Column('grantor', OID))
    admin_option: bool = Field(sa_column=Column('admin_option', Boolean))
    inherit_option: bool = Field(sa_column=Column('inherit_option', Boolean))
    set_option: bool = Field(sa_column=Column('set_option', Boolean))


class PgAuthid(SQLModel, table=True):
    __tablename__ = 'pg_authid'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_authid_oid_index'),
        UniqueConstraint('rolname', name='pg_authid_rolname_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    rolname: str = Field(sa_column=Column('rolname', String))
    rolsuper: bool = Field(sa_column=Column('rolsuper', Boolean))
    rolinherit: bool = Field(sa_column=Column('rolinherit', Boolean))
    rolcreaterole: bool = Field(sa_column=Column('rolcreaterole', Boolean))
    rolcreatedb: bool = Field(sa_column=Column('rolcreatedb', Boolean))
    rolcanlogin: bool = Field(sa_column=Column('rolcanlogin', Boolean))
    rolreplication: bool = Field(sa_column=Column('rolreplication', Boolean))
    rolbypassrls: bool = Field(sa_column=Column('rolbypassrls', Boolean))
    rolconnlimit: int = Field(sa_column=Column('rolconnlimit', Integer))
    rolpassword: Optional[str] = Field(default=None, sa_column=Column('rolpassword', Text))
    rolvaliduntil: Optional[datetime] = Field(default=None, sa_column=Column('rolvaliduntil', DateTime(True)))


class PgCast(SQLModel, table=True):
    __tablename__ = 'pg_cast'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_cast_oid_index'),
        UniqueConstraint('castsource', 'casttarget', name='pg_cast_source_target_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    castsource: Any = Field(sa_column=Column('castsource', OID))
    casttarget: Any = Field(sa_column=Column('casttarget', OID))
    castfunc: Any = Field(sa_column=Column('castfunc', OID))
    castcontext: str = Field(sa_column=Column('castcontext', String))
    castmethod: str = Field(sa_column=Column('castmethod', String))


class PgClass(SQLModel, table=True):
    __tablename__ = 'pg_class'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_class_oid_index'),
        UniqueConstraint('relname', 'relnamespace', name='pg_class_relname_nsp_index'),
        Index('pg_class_tblspc_relfilenode_index', 'reltablespace', 'relfilenode'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    relname: str = Field(sa_column=Column('relname', String))
    relnamespace: Any = Field(sa_column=Column('relnamespace', OID))
    reltype: Any = Field(sa_column=Column('reltype', OID))
    reloftype: Any = Field(sa_column=Column('reloftype', OID))
    relowner: Any = Field(sa_column=Column('relowner', OID))
    relam: Any = Field(sa_column=Column('relam', OID))
    relfilenode: Any = Field(sa_column=Column('relfilenode', OID))
    reltablespace: Any = Field(sa_column=Column('reltablespace', OID))
    relpages: int = Field(sa_column=Column('relpages', Integer))
    reltuples: float = Field(sa_column=Column('reltuples', REAL))
    relallvisible: int = Field(sa_column=Column('relallvisible', Integer))
    reltoastrelid: Any = Field(sa_column=Column('reltoastrelid', OID))
    relhasindex: bool = Field(sa_column=Column('relhasindex', Boolean))
    relisshared: bool = Field(sa_column=Column('relisshared', Boolean))
    relpersistence: str = Field(sa_column=Column('relpersistence', String))
    relkind: str = Field(sa_column=Column('relkind', String))
    relnatts: int = Field(sa_column=Column('relnatts', SmallInteger))
    relchecks: int = Field(sa_column=Column('relchecks', SmallInteger))
    relhasrules: bool = Field(sa_column=Column('relhasrules', Boolean))
    relhastriggers: bool = Field(sa_column=Column('relhastriggers', Boolean))
    relhassubclass: bool = Field(sa_column=Column('relhassubclass', Boolean))
    relrowsecurity: bool = Field(sa_column=Column('relrowsecurity', Boolean))
    relforcerowsecurity: bool = Field(sa_column=Column('relforcerowsecurity', Boolean))
    relispopulated: bool = Field(sa_column=Column('relispopulated', Boolean))
    relreplident: str = Field(sa_column=Column('relreplident', String))
    relispartition: bool = Field(sa_column=Column('relispartition', Boolean))
    relrewrite: Any = Field(sa_column=Column('relrewrite', OID))
    relfrozenxid: Any = Field(sa_column=Column('relfrozenxid', NullType))
    relminmxid: Any = Field(sa_column=Column('relminmxid', NullType))
    relacl: Optional[Any] = Field(default=None, sa_column=Column('relacl', NullType))
    reloptions: Optional[list] = Field(default=None, sa_column=Column('reloptions', ARRAY(Text())))
    relpartbound: Optional[Any] = Field(default=None, sa_column=Column('relpartbound', NullType))

    _type: str = "PgClass"

    def get_namespace(self, introspection: "Introspection") -> "PgNamespace":
        return introspection.get_namespace(self.relnamespace)

    def get_type(self, introspection: "Introspection") -> "PgType":
        return introspection.get_type(self.reltype)

    def get_of_type(self, introspection: "Introspection") -> Optional["PgType"]:
        return introspection.get_type(self.reloftype)

    def get_owner(self, introspection: "Introspection") -> "PgRoles":
        return introspection.get_role(self.relowner)

    def get_attributes(self, introspection: "Introspection") -> list["PgAttribute"]:
        return introspection.get_attributes(self.oid)

    def get_constraints(self, introspection: "Introspection") -> list["PgConstraint"]:
        return introspection.get_constraints(self.oid)

    def get_foreign_constraints(self, introspection: "Introspection") -> list["PgConstraint"]:
        return introspection.get_foreign_constraints(self.oid)

    def get_indexes(self, introspection: "Introspection") -> list["PgIndex"]:
        return introspection.get_indexes(self.oid)

    def get_description(self, introspection: "Introspection", PG_CLASS) -> Optional[str]:
        return introspection.get_description(PG_CLASS, self.oid, 0)

    def get_tags_and_description(self, introspection: "Introspection", PG_CLASS, PG_TYPE) -> Optional[dict]:
        return introspection.get_tags_and_description(
            PG_CLASS, self.oid, 0, {"classoid": PG_TYPE, "objoid": self.reltype}
        )

    def get_tags(self, introspection: "Introspection", PG_CLASS, PG_TYPE) -> Optional[list]:
        tags_and_desc = self.get_tags_and_description(introspection, PG_CLASS, PG_TYPE)
        return tags_and_desc.get('tags') if tags_and_desc else None

    def get_acl(self, introspection: "Introspection", OBJECT_TABLE, OBJECT_SEQUENCE) -> list["AclObject"]:
        from pgrestcue.introspection.acl import parse_acls

        objtype = OBJECT_SEQUENCE if getattr(self, 'relkind', None) == "S" else OBJECT_TABLE
        return parse_acls(introspection, self.relacl, self.relowner, objtype)

    def get_attribute(self, by, introspection: "Introspection") -> Optional["PgAttribute"]:
        attributes = self.get_attributes(introspection)
        if 'number' in by and by['number']:
            return next((att for att in attributes if getattr(att, 'attnum', None) == by['number']), None)
        elif 'name' in by and by['name']:
            return next((att for att in attributes if getattr(att, 'attname', None) == by['name']), None)
        return None

    def get_inherited(self, introspection: "Introspection") -> list["PgInherits"]:
        return [inh for inh in introspection.inherits if getattr(inh, 'inhrelid', None) == self.oid]

    def get_access_method(self, introspection: "Introspection") -> Optional["PgAm"]:
        if self.relam is not None:
            return next((am for am in introspection.am if getattr(am, 'oid', None) == self.relam), None)
        return None


class PgCollation(SQLModel, table=True):
    __tablename__ = 'pg_collation'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_collation_oid_index'),
        UniqueConstraint('collname', 'collencoding', 'collnamespace', name='pg_collation_name_enc_nsp_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    collname: str = Field(sa_column=Column('collname', String))
    collnamespace: Any = Field(sa_column=Column('collnamespace', OID))
    collowner: Any = Field(sa_column=Column('collowner', OID))
    collprovider: str = Field(sa_column=Column('collprovider', String))
    collisdeterministic: bool = Field(sa_column=Column('collisdeterministic', Boolean))
    collencoding: int = Field(sa_column=Column('collencoding', Integer))
    collcollate: Optional[str] = Field(default=None, sa_column=Column('collcollate', Text))
    collctype: Optional[str] = Field(default=None, sa_column=Column('collctype', Text))
    colllocale: Optional[str] = Field(default=None, sa_column=Column('colllocale', Text))
    collicurules: Optional[str] = Field(default=None, sa_column=Column('collicurules', Text))
    collversion: Optional[str] = Field(default=None, sa_column=Column('collversion', Text))


class PgConstraint(SQLModel, table=True):
    __tablename__ = 'pg_constraint'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_constraint_oid_index'),
        UniqueConstraint('conrelid', 'contypid', 'conname', name='pg_constraint_conrelid_contypid_conname_index'),
        Index('pg_constraint_conname_nsp_index', 'conname', 'connamespace'),
        Index('pg_constraint_conparentid_index', 'conparentid'),
        Index('pg_constraint_contypid_index', 'contypid'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    conname: str = Field(sa_column=Column('conname', String))
    connamespace: Any = Field(sa_column=Column('connamespace', OID))
    contype: str = Field(sa_column=Column('contype', String))
    condeferrable: bool = Field(sa_column=Column('condeferrable', Boolean))
    condeferred: bool = Field(sa_column=Column('condeferred', Boolean))
    convalidated: bool = Field(sa_column=Column('convalidated', Boolean))
    conrelid: Any = Field(sa_column=Column('conrelid', OID))
    contypid: Any = Field(sa_column=Column('contypid', OID))
    conindid: Any = Field(sa_column=Column('conindid', OID))
    conparentid: Any = Field(sa_column=Column('conparentid', OID))
    confrelid: Any = Field(sa_column=Column('confrelid', OID))
    confupdtype: str = Field(sa_column=Column('confupdtype', String))
    confdeltype: str = Field(sa_column=Column('confdeltype', String))
    confmatchtype: str = Field(sa_column=Column('confmatchtype', String))
    conislocal: bool = Field(sa_column=Column('conislocal', Boolean))
    coninhcount: int = Field(sa_column=Column('coninhcount', SmallInteger))
    connoinherit: bool = Field(sa_column=Column('connoinherit', Boolean))
    conkey: Optional[list] = Field(default=None, sa_column=Column('conkey', ARRAY(SmallInteger())))
    confkey: Optional[list] = Field(default=None, sa_column=Column('confkey', ARRAY(SmallInteger())))
    conpfeqop: Optional[list] = Field(default=None, sa_column=Column('conpfeqop', ARRAY(OID())))
    conppeqop: Optional[list] = Field(default=None, sa_column=Column('conppeqop', ARRAY(OID())))
    conffeqop: Optional[list] = Field(default=None, sa_column=Column('conffeqop', ARRAY(OID())))
    confdelsetcols: Optional[list] = Field(default=None, sa_column=Column('confdelsetcols', ARRAY(SmallInteger())))
    conexclop: Optional[list] = Field(default=None, sa_column=Column('conexclop', ARRAY(OID())))
    conbin: Optional[Any] = Field(default=None, sa_column=Column('conbin', NullType))

    def get_namespace(self, introspection: "Introspection") -> "PgNamespace":
        return introspection.get_namespace(self.connamespace)

    def get_class(self, introspection: "Introspection") -> Optional["PgClass"]:
        return introspection.get_class(self.conrelid)

    def get_attributes(self, introspection: "Introspection") -> list["PgAttribute"] | None:
        """
        Get the attributes associated with this constraint.
        """
        klass = self.get_class(introspection)
        if not klass:
            logging.warning(
                f"get_attributes called on constraint {self.oid} with no class found for conrelid {self.conrelid}")
            return []
        if not self.conkey:
            if self.contype == 'f':
                return []
            else:
                return None

        attrs = klass.get_attributes(introspection)
        return [attr for key in self.conkey for attr in attrs if attr.attnum == key]

    def get_type(self, introspection: "Introspection") -> Optional["PgType"]:
        """
        Get the type associated with this constraint.
        """
        return introspection.get_type(self.contypid)

    def get_foreign_class(self, introspection: "Introspection") -> Optional["PgClass"]:
        """
        Get the foreign class associated with this constraint, if it is a foreign key.
        """
        return introspection.get_class(self.confrelid)

    def get_foreign_attributes(self, introspection: "Introspection") -> list["PgAttribute"] | None:
        """
        Get the foreign attributes associated with this constraint, if it is a foreign key.
        """
        foreign_class = self.get_foreign_class(introspection)
        if not foreign_class:
            logging.warning(
                f"get_foreign_attributes called on constraint {self.oid} with no foreign class found for confrelid {self.confrelid}")
            return []
        if not self.confkey:
            return None

        attrs = foreign_class.get_attributes(introspection)
        return [attr for key in self.confkey for attr in attrs if attr.attnum == key]

    def get_description(self, introspection: "Introspection") -> Optional[str]:
        """
        Get the description of this constraint.
        """
        return introspection.get_description(introspection.PG_CONSTRAINT, self.oid, 0)

    def get_tags_and_description(self, introspection: "Introspection") -> Optional[dict]:
        """
        Get the tags and description of this constraint.
        """
        return introspection.get_tags_and_description(introspection.PG_CONSTRAINT, self.oid)

    def get_tags(self, introspection: "Introspection") -> Optional[list]:
        """
        Get the tags associated with this constraint.
        """
        tags_and_desc = self.get_tags_and_description(introspection)
        return tags_and_desc.get('tags') if tags_and_desc else None


class PgConversion(SQLModel, table=True):
    __tablename__ = 'pg_conversion'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_conversion_oid_index'),
        UniqueConstraint('conname', 'connamespace', name='pg_conversion_name_nsp_index'),
        UniqueConstraint('connamespace', 'conforencoding', 'contoencoding', 'oid', name='pg_conversion_default_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    conname: str = Field(sa_column=Column('conname', String))
    connamespace: Any = Field(sa_column=Column('connamespace', OID))
    conowner: Any = Field(sa_column=Column('conowner', OID))
    conforencoding: int = Field(sa_column=Column('conforencoding', Integer))
    contoencoding: int = Field(sa_column=Column('contoencoding', Integer))
    conproc: Any = Field(sa_column=Column('conproc', NullType))
    condefault: bool = Field(sa_column=Column('condefault', Boolean))


class PgDatabase(SQLModel, table=True):
    __tablename__ = 'pg_database'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_database_oid_index'),
        UniqueConstraint('datname', name='pg_database_datname_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    datname: str = Field(sa_column=Column('datname', String))
    datdba: Any = Field(sa_column=Column('datdba', OID))
    encoding: int = Field(sa_column=Column('encoding', Integer))
    datlocprovider: str = Field(sa_column=Column('datlocprovider', String))
    datistemplate: bool = Field(sa_column=Column('datistemplate', Boolean))
    datallowconn: bool = Field(sa_column=Column('datallowconn', Boolean))
    dathasloginevt: bool = Field(sa_column=Column('dathasloginevt', Boolean))
    datconnlimit: int = Field(sa_column=Column('datconnlimit', Integer))
    datfrozenxid: Any = Field(sa_column=Column('datfrozenxid', NullType))
    datminmxid: Any = Field(sa_column=Column('datminmxid', NullType))
    dattablespace: Any = Field(sa_column=Column('dattablespace', OID))
    datcollate: str = Field(sa_column=Column('datcollate', Text))
    datctype: str = Field(sa_column=Column('datctype', Text))
    datlocale: Optional[str] = Field(default=None, sa_column=Column('datlocale', Text))
    daticurules: Optional[str] = Field(default=None, sa_column=Column('daticurules', Text))
    datcollversion: Optional[str] = Field(default=None, sa_column=Column('datcollversion', Text))
    datacl: Optional[Any] = Field(default=None, sa_column=Column('datacl', NullType))

    _type: str = "PgDatabase"

    def get_owner(self, introspection: "Introspection") -> Any:
        return introspection.get_role(self.datdba)

    @lru_cache()
    def get_acl(self, introspection: "Introspection", OBJECT_DATABASE) -> Any:
        from pgrestcue.introspection.acl import parse_acls
        return parse_acls(
            introspection,
            self.datacl,
            self.datdba,
            OBJECT_DATABASE,
        )


class PgDbRoleSetting(SQLModel, table=True):
    __tablename__ = 'pg_db_role_setting'
    __table_args__ = (
        PrimaryKeyConstraint('setdatabase', 'setrole', name='pg_db_role_setting_databaseid_rol_index'),
        {'schema': 'pg_catalog'}
    )

    setdatabase: Any = Field(sa_column=Column('setdatabase', OID, primary_key=True))
    setrole: Any = Field(sa_column=Column('setrole', OID, primary_key=True))
    setconfig: Optional[list] = Field(default=None, sa_column=Column('setconfig', ARRAY(Text())))


class PgDefaultAcl(SQLModel, table=True):
    __tablename__ = 'pg_default_acl'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_default_acl_oid_index'),
        UniqueConstraint('defaclrole', 'defaclnamespace', 'defaclobjtype', name='pg_default_acl_role_nsp_obj_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    defaclrole: Any = Field(sa_column=Column('defaclrole', OID))
    defaclnamespace: Any = Field(sa_column=Column('defaclnamespace', OID))
    defaclobjtype: str = Field(sa_column=Column('defaclobjtype', String))
    defaclacl: Any = Field(sa_column=Column('defaclacl', NullType))


class PgDepend(SQLModel, table=True):
    __tablename__ = 'pg_depend'
    __table_args__ = (
        PrimaryKeyConstraint('classid', 'objid', 'objsubid', 'refclassid', 'refobjid', 'refobjsubid',
                             name='pg_depend_c_o_s_r_c_o_s_index'),
        Index('pg_depend_depender_index', 'classid', 'objid', 'objsubid'),
        Index('pg_depend_reference_index', 'refclassid', 'refobjid', 'refobjsubid'),
        {'schema': 'pg_catalog'}
    )

    classid: Any = Field(sa_column=Column('classid', OID, primary_key=True))
    objid: Any = Field(sa_column=Column('objid', OID, primary_key=True))
    objsubid: int = Field(sa_column=Column('objsubid', Integer, primary_key=True))
    refclassid: Any = Field(sa_column=Column('refclassid', OID, primary_key=True))
    refobjid: Any = Field(sa_column=Column('refobjid', OID, primary_key=True))
    refobjsubid: int = Field(sa_column=Column('refobjsubid', Integer, primary_key=True))
    deptype: str = Field(sa_column=Column('deptype', String))


class PgDescription(SQLModel, table=True):
    __tablename__ = 'pg_description'
    __table_args__ = (
        PrimaryKeyConstraint('objoid', 'classoid', 'objsubid', name='pg_description_o_c_o_index'),
        {'schema': 'pg_catalog'}
    )

    objoid: Any = Field(sa_column=Column('objoid', OID, primary_key=True))
    classoid: Any = Field(sa_column=Column('classoid', OID, primary_key=True))
    objsubid: int = Field(sa_column=Column('objsubid', Integer, primary_key=True))
    description: str = Field(sa_column=Column('description', Text))


class PgEnum(SQLModel, table=True):
    __tablename__ = 'pg_enum'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_enum_oid_index'),
        UniqueConstraint('enumtypid', 'enumlabel', name='pg_enum_typid_label_index'),
        UniqueConstraint('enumtypid', 'enumsortorder', name='pg_enum_typid_sortorder_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    enumtypid: Any = Field(sa_column=Column('enumtypid', OID))
    enumsortorder: float = Field(sa_column=Column('enumsortorder', REAL))
    enumlabel: str = Field(sa_column=Column('enumlabel', String))

    def get_type(self, introspection: "Introspection") -> Optional["PgType"]:
        """
        Get the type associated with this enum.
        """
        return introspection.get_type(self.enumtypid)

    def get_tags_and_description(self, introspection: "Introspection") -> Optional[dict]:
        """
        Get the tags and description of this enum.
        """
        return introspection.get_tags_and_description(introspection.PG_ENUM, self.oid)

    def get_tags(self, introspection: "Introspection") -> Optional[list]:
        """
        Get the tags associated with this enum.
        """
        tags_and_desc = self.get_tags_and_description(introspection)
        return tags_and_desc.get('tags') if tags_and_desc else None


class PgEventTrigger(SQLModel, table=True):
    __tablename__ = 'pg_event_trigger'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_event_trigger_oid_index'),
        UniqueConstraint('evtname', name='pg_event_trigger_evtname_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    evtname: str = Field(sa_column=Column('evtname', String))
    evtevent: str = Field(sa_column=Column('evtevent', String))
    evtowner: Any = Field(sa_column=Column('evtowner', OID))
    evtfoid: Any = Field(sa_column=Column('evtfoid', OID))
    evtenabled: str = Field(sa_column=Column('evtenabled', String))
    evttags: Optional[list] = Field(default=None, sa_column=Column('evttags', ARRAY(Text())))


class PgExtension(SQLModel, table=True):
    __tablename__ = 'pg_extension'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_extension_oid_index'),
        UniqueConstraint('extname', name='pg_extension_name_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    extname: str = Field(sa_column=Column('extname', String))
    extowner: Any = Field(sa_column=Column('extowner', OID))
    extnamespace: Any = Field(sa_column=Column('extnamespace', OID))
    extrelocatable: bool = Field(sa_column=Column('extrelocatable', Boolean))
    extversion: str = Field(sa_column=Column('extversion', Text))
    extconfig: Optional[list] = Field(default=None, sa_column=Column('extconfig', ARRAY(OID())))
    extcondition: Optional[list] = Field(default=None, sa_column=Column('extcondition', ARRAY(Text())))


class PgForeignDataWrapper(SQLModel, table=True):
    __tablename__ = 'pg_foreign_data_wrapper'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_foreign_data_wrapper_oid_index'),
        UniqueConstraint('fdwname', name='pg_foreign_data_wrapper_name_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    fdwname: str = Field(sa_column=Column('fdwname', String))
    fdwowner: Any = Field(sa_column=Column('fdwowner', OID))
    fdwhandler: Any = Field(sa_column=Column('fdwhandler', OID))
    fdwvalidator: Any = Field(sa_column=Column('fdwvalidator', OID))
    fdwacl: Optional[Any] = Field(default=None, sa_column=Column('fdwacl', NullType))
    fdwoptions: Optional[list] = Field(default=None, sa_column=Column('fdwoptions', ARRAY(Text())))


class PgForeignServer(SQLModel, table=True):
    __tablename__ = 'pg_foreign_server'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_foreign_server_oid_index'),
        UniqueConstraint('srvname', name='pg_foreign_server_name_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    srvname: str = Field(sa_column=Column('srvname', String))
    srvowner: Any = Field(sa_column=Column('srvowner', OID))
    srvfdw: Any = Field(sa_column=Column('srvfdw', OID))
    srvtype: Optional[str] = Field(default=None, sa_column=Column('srvtype', Text))
    srvversion: Optional[str] = Field(default=None, sa_column=Column('srvversion', Text))
    srvacl: Optional[Any] = Field(default=None, sa_column=Column('srvacl', NullType))
    srvoptions: Optional[list] = Field(default=None, sa_column=Column('srvoptions', ARRAY(Text())))


class PgForeignTable(SQLModel, table=True):
    __tablename__ = 'pg_foreign_table'
    __table_args__ = (
        PrimaryKeyConstraint('ftrelid', name='pg_foreign_table_relid_index'),
        {'schema': 'pg_catalog'}
    )

    ftrelid: Any = Field(sa_column=Column('ftrelid', OID, primary_key=True))
    ftserver: Any = Field(sa_column=Column('ftserver', OID))
    ftoptions: Optional[list] = Field(default=None, sa_column=Column('ftoptions', ARRAY(Text())))


class PgIndex(SQLModel, table=True):
    __tablename__ = 'pg_index'
    __table_args__ = (
        PrimaryKeyConstraint('indexrelid', name='pg_index_indexrelid_index'),
        Index('pg_index_indrelid_index', 'indrelid'),
        {'schema': 'pg_catalog'}
    )

    indexrelid: Any = Field(sa_column=Column('indexrelid', OID, primary_key=True))
    indrelid: Any = Field(sa_column=Column('indrelid', OID))
    indnatts: int = Field(sa_column=Column('indnatts', SmallInteger))
    indnkeyatts: int = Field(sa_column=Column('indnkeyatts', SmallInteger))
    indisunique: bool = Field(sa_column=Column('indisunique', Boolean))
    indnullsnotdistinct: bool = Field(sa_column=Column('indnullsnotdistinct', Boolean))
    indisprimary: bool = Field(sa_column=Column('indisprimary', Boolean))
    indisexclusion: bool = Field(sa_column=Column('indisexclusion', Boolean))
    indimmediate: bool = Field(sa_column=Column('indimmediate', Boolean))
    indisclustered: bool = Field(sa_column=Column('indisclustered', Boolean))
    indisvalid: bool = Field(sa_column=Column('indisvalid', Boolean))
    indcheckxmin: bool = Field(sa_column=Column('indcheckxmin', Boolean))
    indisready: bool = Field(sa_column=Column('indisready', Boolean))
    indislive: bool = Field(sa_column=Column('indislive', Boolean))
    indisreplident: bool = Field(sa_column=Column('indisreplident', Boolean))
    indkey: Any = Field(sa_column=Column('indkey', NullType))
    indcollation: Any = Field(sa_column=Column('indcollation', NullType))
    indclass: Any = Field(sa_column=Column('indclass', NullType))
    indoption: Any = Field(sa_column=Column('indoption', NullType))
    indexprs: Optional[Any] = Field(default=None, sa_column=Column('indexprs', NullType))
    indpred: Optional[Any] = Field(default=None, sa_column=Column('indpred', NullType))

    _type: str = "PgIndex"

    def get_index_class(self, introspection: "Introspection") -> "PgClass":
        return introspection.get_class(self.indexrelid)

    def get_class(self, introspection: "Introspection") -> Optional["PgClass"]:
        return introspection.get_class(self.indrelid)

    def get_keys(self, introspection: "Introspection") -> list[Optional["PgAttribute"]]:
        owner = self.get_class(introspection)
        attrs = owner.get_attributes(introspection) if owner else []
        keys = self.indkey if hasattr(self, 'indkey') else []
        return [None if key == 0 else next((a for a in attrs if getattr(a, 'attnum', None) == key), None) for key in
                keys]


class PgInherits(SQLModel, table=True):
    __tablename__ = 'pg_inherits'
    __table_args__ = (
        PrimaryKeyConstraint('inhrelid', 'inhseqno', name='pg_inherits_relid_seqno_index'),
        Index('pg_inherits_parent_index', 'inhparent'),
        {'schema': 'pg_catalog'}
    )

    inhrelid: Any = Field(sa_column=Column('inhrelid', OID, primary_key=True))
    inhparent: Any = Field(sa_column=Column('inhparent', OID))
    inhseqno: int = Field(sa_column=Column('inhseqno', Integer, primary_key=True))
    inhdetachpending: bool = Field(sa_column=Column('inhdetachpending', Boolean))


class PgInitPrivs(SQLModel, table=True):
    __tablename__ = 'pg_init_privs'
    __table_args__ = (
        PrimaryKeyConstraint('objoid', 'classoid', 'objsubid', name='pg_init_privs_o_c_o_index'),
        {'schema': 'pg_catalog'}
    )

    objoid: Any = Field(sa_column=Column('objoid', OID, primary_key=True))
    classoid: Any = Field(sa_column=Column('classoid', OID, primary_key=True))
    objsubid: int = Field(sa_column=Column('objsubid', Integer, primary_key=True))
    privtype: str = Field(sa_column=Column('privtype', String))
    initprivs: Any = Field(sa_column=Column('initprivs', NullType))


class PgLanguage(SQLModel, table=True):
    __tablename__ = 'pg_language'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_language_oid_index'),
        UniqueConstraint('lanname', name='pg_language_name_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    lanname: str = Field(sa_column=Column('lanname', String))
    lanowner: Any = Field(sa_column=Column('lanowner', OID))
    lanispl: bool = Field(sa_column=Column('lanispl', Boolean))
    lanpltrusted: bool = Field(sa_column=Column('lanpltrusted', Boolean))
    lanplcallfoid: Any = Field(sa_column=Column('lanplcallfoid', OID))
    laninline: Any = Field(sa_column=Column('laninline', OID))
    lanvalidator: Any = Field(sa_column=Column('lanvalidator', OID))
    lanacl: Optional[Any] = Field(default=None, sa_column=Column('lanacl', NullType))


class PgLargeobject(SQLModel, table=True):
    __tablename__ = 'pg_largeobject'
    __table_args__ = (
        PrimaryKeyConstraint('loid', 'pageno', name='pg_largeobject_loid_pn_index'),
        {'schema': 'pg_catalog'}
    )

    loid: Any = Field(sa_column=Column('loid', OID, primary_key=True))
    pageno: int = Field(sa_column=Column('pageno', Integer, primary_key=True))
    data: bytes = Field(sa_column=Column('data', LargeBinary))


class PgLargeobjectMetadata(SQLModel, table=True):
    __tablename__ = 'pg_largeobject_metadata'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_largeobject_metadata_oid_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    lomowner: Any = Field(sa_column=Column('lomowner', OID))
    lomacl: Optional[Any] = Field(default=None, sa_column=Column('lomacl', NullType))


class PgNamespace(SQLModel, table=True):
    __tablename__ = 'pg_namespace'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_namespace_oid_index'),
        UniqueConstraint('nspname', name='pg_namespace_nspname_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    nspname: str = Field(sa_column=Column('nspname', String))
    nspowner: Any = Field(sa_column=Column('nspowner', OID))
    nspacl: Optional[Any] = Field(default=None, sa_column=Column('nspacl', NullType))

    _type: str = "PgNamespace"

    def get_owner(self, introspection: "Introspection"):
        return introspection.get_role(self.nspowner)

    def get_description(self, introspection: "Introspection", PG_NAMESPACE):
        return introspection.get_description(PG_NAMESPACE, self.oid)

    def get_tags_and_description(self, introspection: "Introspection", PG_NAMESPACE):
        return introspection.get_tags_and_description(PG_NAMESPACE, self.oid)

    def get_tags(self, introspection: "Introspection", PG_NAMESPACE):
        tags_and_desc = self.get_tags_and_description(introspection, PG_NAMESPACE)
        return tags_and_desc.get('tags') if tags_and_desc else None

    def get_acl(self, introspection: "Introspection", OBJECT_SCHEMA):
        from pgrestcue.introspection.acl import parse_acls
        return parse_acls(introspection, self.nspacl, self.nspowner, OBJECT_SCHEMA)

    def get_class(self, introspection: "Introspection", by):
        return next((child for child in introspection.classes if
                     getattr(child, 'relnamespace', None) == self.oid and getattr(child, 'relname', None) == by.get(
                         'name')), None)

    def get_constraint(self, introspection: "Introspection", by):
        return next((child for child in introspection.constraints if
                     getattr(child, 'connamespace', None) == self.oid and getattr(child, 'conname', None) == by.get(
                         'name')), None)

    def get_procs(self, introspection: "Introspection", by):
        return [child for child in introspection.procs if
                getattr(child, 'pronamespace', None) == self.oid and getattr(child, 'proname', None) == by.get('name')]


class PgOpclass(SQLModel, table=True):
    __tablename__ = 'pg_opclass'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_opclass_oid_index'),
        UniqueConstraint('opcmethod', 'opcname', 'opcnamespace', name='pg_opclass_am_name_nsp_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    opcmethod: Any = Field(sa_column=Column('opcmethod', OID))
    opcname: str = Field(sa_column=Column('opcname', String))
    opcnamespace: Any = Field(sa_column=Column('opcnamespace', OID))
    opcowner: Any = Field(sa_column=Column('opcowner', OID))
    opcfamily: Any = Field(sa_column=Column('opcfamily', OID))
    opcintype: Any = Field(sa_column=Column('opcintype', OID))
    opcdefault: bool = Field(sa_column=Column('opcdefault', Boolean))
    opckeytype: Any = Field(sa_column=Column('opckeytype', OID))


class PgOperator(SQLModel, table=True):
    __tablename__ = 'pg_operator'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_operator_oid_index'),
        UniqueConstraint('oprname', 'oprleft', 'oprright', 'oprnamespace', name='pg_operator_oprname_l_r_n_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    oprname: str = Field(sa_column=Column('oprname', String))
    oprnamespace: Any = Field(sa_column=Column('oprnamespace', OID))
    oprowner: Any = Field(sa_column=Column('oprowner', OID))
    oprkind: str = Field(sa_column=Column('oprkind', String))
    oprcanmerge: bool = Field(sa_column=Column('oprcanmerge', Boolean))
    oprcanhash: bool = Field(sa_column=Column('oprcanhash', Boolean))
    oprleft: Any = Field(sa_column=Column('oprleft', OID))
    oprright: Any = Field(sa_column=Column('oprright', OID))
    oprresult: Any = Field(sa_column=Column('oprresult', OID))
    oprcom: Any = Field(sa_column=Column('oprcom', OID))
    oprnegate: Any = Field(sa_column=Column('oprnegate', OID))
    oprcode: Any = Field(sa_column=Column('oprcode', NullType))
    oprrest: Any = Field(sa_column=Column('oprrest', NullType))
    oprjoin: Any = Field(sa_column=Column('oprjoin', NullType))


class PgOpfamily(SQLModel, table=True):
    __tablename__ = 'pg_opfamily'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_opfamily_oid_index'),
        UniqueConstraint('opfmethod', 'opfname', 'opfnamespace', name='pg_opfamily_am_name_nsp_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    opfmethod: Any = Field(sa_column=Column('opfmethod', OID))
    opfname: str = Field(sa_column=Column('opfname', String))
    opfnamespace: Any = Field(sa_column=Column('opfnamespace', OID))
    opfowner: Any = Field(sa_column=Column('opfowner', OID))


class PgParameterAcl(SQLModel, table=True):
    __tablename__ = 'pg_parameter_acl'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_parameter_acl_oid_index'),
        UniqueConstraint('parname', name='pg_parameter_acl_parname_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    parname: str = Field(sa_column=Column('parname', Text))
    paracl: Optional[Any] = Field(default=None, sa_column=Column('paracl', NullType))


class PgPartitionedTable(SQLModel, table=True):
    __tablename__ = 'pg_partitioned_table'
    __table_args__ = (
        PrimaryKeyConstraint('partrelid', name='pg_partitioned_table_partrelid_index'),
        {'schema': 'pg_catalog'}
    )

    partrelid: Any = Field(sa_column=Column('partrelid', OID, primary_key=True))
    partstrat: str = Field(sa_column=Column('partstrat', String))
    partnatts: int = Field(sa_column=Column('partnatts', SmallInteger))
    partdefid: Any = Field(sa_column=Column('partdefid', OID))
    partattrs: Any = Field(sa_column=Column('partattrs', NullType))
    partclass: Any = Field(sa_column=Column('partclass', NullType))
    partcollation: Any = Field(sa_column=Column('partcollation', NullType))
    partexprs: Optional[Any] = Field(default=None, sa_column=Column('partexprs', NullType))


class PgPolicy(SQLModel, table=True):
    __tablename__ = 'pg_policy'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_policy_oid_index'),
        UniqueConstraint('polrelid', 'polname', name='pg_policy_polrelid_polname_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    polname: str = Field(sa_column=Column('polname', String))
    polrelid: Any = Field(sa_column=Column('polrelid', OID))
    polcmd: str = Field(sa_column=Column('polcmd', String))
    polpermissive: bool = Field(sa_column=Column('polpermissive', Boolean))
    polroles: list = Field(sa_column=Column('polroles', ARRAY(OID())))
    polqual: Optional[Any] = Field(default=None, sa_column=Column('polqual', NullType))
    polwithcheck: Optional[Any] = Field(default=None, sa_column=Column('polwithcheck', NullType))


class ProcArgument(BaseModel):
    is_in: bool
    is_out: bool
    is_variadic: bool
    has_default: bool
    typ: Optional["PgType"]
    name: Optional[str]


class PgProc(SQLModel, table=True):
    __tablename__ = 'pg_proc'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_proc_oid_index'),
        UniqueConstraint('proname', 'proargtypes', 'pronamespace', name='pg_proc_proname_args_nsp_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    proname: str = Field(sa_column=Column('proname', String))
    pronamespace: Any = Field(sa_column=Column('pronamespace', OID))
    proowner: Any = Field(sa_column=Column('proowner', OID))
    prolang: Any = Field(sa_column=Column('prolang', OID))
    procost: float = Field(sa_column=Column('procost', REAL))
    prorows: float = Field(sa_column=Column('prorows', REAL))
    provariadic: Any = Field(sa_column=Column('provariadic', OID))
    prosupport: Any = Field(sa_column=Column('prosupport', NullType))
    prokind: str = Field(sa_column=Column('prokind', String))
    prosecdef: bool = Field(sa_column=Column('prosecdef', Boolean))
    proleakproof: bool = Field(sa_column=Column('proleakproof', Boolean))
    proisstrict: bool = Field(sa_column=Column('proisstrict', Boolean))
    proretset: bool = Field(sa_column=Column('proretset', Boolean))
    provolatile: str = Field(sa_column=Column('provolatile', String))
    proparallel: str = Field(sa_column=Column('proparallel', String))
    pronargs: int = Field(sa_column=Column('pronargs', SmallInteger))
    pronargdefaults: int = Field(sa_column=Column('pronargdefaults', SmallInteger))
    prorettype: Any = Field(sa_column=Column('prorettype', OID))
    proargtypes: Any = Field(sa_column=Column('proargtypes', NullType))
    prosrc: str = Field(sa_column=Column('prosrc', Text))
    proallargtypes: Optional[list] = Field(default=None, sa_column=Column('proallargtypes', ARRAY(OID())))
    proargmodes: Optional[list] = Field(default=None, sa_column=Column('proargmodes', ARRAY(String())))
    proargnames: Optional[list] = Field(default=None, sa_column=Column('proargnames', ARRAY(Text())))
    proargdefaults: Optional[Any] = Field(default=None, sa_column=Column('proargdefaults', NullType))
    protrftypes: Optional[list] = Field(default=None, sa_column=Column('protrftypes', ARRAY(OID())))
    probin: Optional[str] = Field(default=None, sa_column=Column('probin', Text))
    prosqlbody: Optional[Any] = Field(default=None, sa_column=Column('prosqlbody', NullType))
    proconfig: Optional[list] = Field(default=None, sa_column=Column('proconfig', ARRAY(Text())))
    proacl: Optional[Any] = Field(default=None, sa_column=Column('proacl', NullType))

    def get_namespace(self, introspection: "Introspection") -> Optional[PgNamespace]:
        return introspection.get_namespace(self.pronamespace)

    def get_owner(self, introspection: "Introspection") -> Any:
        return introspection.get_role(self.proowner)

    def get_return_type(self, introspection: "Introspection") -> Optional["PgType"]:
        return introspection.get_type(self.prorettype)

    def get_description(self, introspection: "Introspection", PG_PROC) -> Optional[str]:
        return introspection.get_description(PG_PROC, self.oid)

    def get_tags_and_description(self, introspection: "Introspection", PG_PROC) -> Optional[dict]:
        return introspection.get_tags_and_description(PG_PROC, self.oid)

    def get_tags(self, introspection: "Introspection", PG_PROC) -> Optional[list]:
        tags_and_desc = self.get_tags_and_description(introspection, PG_PROC)
        return tags_and_desc.get('tags') if tags_and_desc else None

    def get_arguments(self, introspection: "Introspection") -> list[ProcArgument]:
        args: list[ProcArgument] = list()
        for arglist in (self.proargtypes, self.proallargtypes):
            if not arglist:
                continue

            for idx, type_id in enumerate(arglist):
                typ = introspection.get_type(type_id)
                if not typ:
                    raise ValueError(f"Argument type with OID {type_id} not found in introspection data.")
                mode = self.progarmodes[idx] or 'i'
                is_in = mode in ('i', 'b', 'v')
                is_out = mode in ('o', 'b', 'v')
                is_variadic = mode == 'v'
                has_default = idx >= len(arglist) - self.pronargdefaults if self.proargdefaults else False
                name = self.proargnames[idx] or None

                args.append(
                    ProcArgument(
                        is_in=is_in,
                        is_out=is_out,
                        is_variadic=is_variadic,
                        has_default=has_default,
                        typ=typ,
                        name=name
                    )
                )
        return args

    def get_acl(self, introspection: "Introspection") -> Any:
        from pgrestcue.introspection.acl import parse_acls, OBJECT_FUNCTION
        return parse_acls(
            introspection,
            self.proacl,
            self.proowner,
            OBJECT_FUNCTION,
        )


class PgPublication(SQLModel, table=True):
    __tablename__ = 'pg_publication'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_publication_oid_index'),
        UniqueConstraint('pubname', name='pg_publication_pubname_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    pubname: str = Field(sa_column=Column('pubname', String))
    pubowner: Any = Field(sa_column=Column('pubowner', OID))
    puballtables: bool = Field(sa_column=Column('puballtables', Boolean))
    pubinsert: bool = Field(sa_column=Column('pubinsert', Boolean))
    pubupdate: bool = Field(sa_column=Column('pubupdate', Boolean))
    pubdelete: bool = Field(sa_column=Column('pubdelete', Boolean))
    pubtruncate: bool = Field(sa_column=Column('pubtruncate', Boolean))
    pubviaroot: bool = Field(sa_column=Column('pubviaroot', Boolean))


class PgPublicationNamespace(SQLModel, table=True):
    __tablename__ = 'pg_publication_namespace'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_publication_namespace_oid_index'),
        UniqueConstraint('pnnspid', 'pnpubid', name='pg_publication_namespace_pnnspid_pnpubid_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    pnpubid: Any = Field(sa_column=Column('pnpubid', OID))
    pnnspid: Any = Field(sa_column=Column('pnnspid', OID))


class PgPublicationRel(SQLModel, table=True):
    __tablename__ = 'pg_publication_rel'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_publication_rel_oid_index'),
        UniqueConstraint('prrelid', 'prpubid', name='pg_publication_rel_prrelid_prpubid_index'),
        Index('pg_publication_rel_prpubid_index', 'prpubid'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    prpubid: Any = Field(sa_column=Column('prpubid', OID))
    prrelid: Any = Field(sa_column=Column('prrelid', OID))
    prqual: Optional[Any] = Field(default=None, sa_column=Column('prqual', NullType))
    prattrs: Optional[Any] = Field(default=None, sa_column=Column('prattrs', NullType))


class PgRange(SQLModel, table=True):
    __tablename__ = 'pg_range'
    __table_args__ = (
        PrimaryKeyConstraint('rngtypid', name='pg_range_rngtypid_index'),
        UniqueConstraint('rngmultitypid', name='pg_range_rngmultitypid_index'),
        {'schema': 'pg_catalog'}
    )

    rngtypid: Any = Field(sa_column=Column('rngtypid', OID, primary_key=True))
    rngsubtype: Any = Field(sa_column=Column('rngsubtype', OID))
    rngmultitypid: Any = Field(sa_column=Column('rngmultitypid', OID))
    rngcollation: Any = Field(sa_column=Column('rngcollation', OID))
    rngsubopc: Any = Field(sa_column=Column('rngsubopc', OID))
    rngcanonical: Any = Field(sa_column=Column('rngcanonical', NullType))
    rngsubdiff: Any = Field(sa_column=Column('rngsubdiff', NullType))

    def get_type(self, introspection: "Introspection") -> Optional["PgType"]:
        """
        Get the type associated with this range.
        """
        return introspection.get_type(self.rngtypid)

    def get_subtype(self, introspection: "Introspection") -> Optional["PgType"]:
        """
        Get the subtype associated with this range.
        """
        return introspection.get_type(self.rngsubtype)


class PgReplicationOrigin(SQLModel, table=True):
    __tablename__ = 'pg_replication_origin'
    __table_args__ = (
        PrimaryKeyConstraint('roident', name='pg_replication_origin_roiident_index'),
        UniqueConstraint('roname', name='pg_replication_origin_roname_index'),
        {'schema': 'pg_catalog'}
    )

    roident: Any = Field(sa_column=Column('roident', OID, primary_key=True))
    roname: str = Field(sa_column=Column('roname', Text))


class PgRewrite(SQLModel, table=True):
    __tablename__ = 'pg_rewrite'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_rewrite_oid_index'),
        UniqueConstraint('ev_class', 'rulename', name='pg_rewrite_rel_rulename_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    rulename: str = Field(sa_column=Column('rulename', String))
    ev_class: Any = Field(sa_column=Column('ev_class', OID))
    ev_type: str = Field(sa_column=Column('ev_type', String))
    ev_enabled: str = Field(sa_column=Column('ev_enabled', String))
    is_instead: bool = Field(sa_column=Column('is_instead', Boolean))
    ev_qual: Any = Field(sa_column=Column('ev_qual', NullType))
    ev_action: Any = Field(sa_column=Column('ev_action', NullType))


class PgSeclabel(SQLModel, table=True):
    __tablename__ = 'pg_seclabel'
    __table_args__ = (
        PrimaryKeyConstraint('objoid', 'classoid', 'objsubid', 'provider', name='pg_seclabel_object_index'),
        {'schema': 'pg_catalog'}
    )

    objoid: Any = Field(sa_column=Column('objoid', OID, primary_key=True))
    classoid: Any = Field(sa_column=Column('classoid', OID, primary_key=True))
    objsubid: int = Field(sa_column=Column('objsubid', Integer, primary_key=True))
    provider: str = Field(sa_column=Column('provider', Text, primary_key=True))
    label: str = Field(sa_column=Column('label', Text))


class PgSequence(SQLModel, table=True):
    __tablename__ = 'pg_sequence'
    __table_args__ = (
        PrimaryKeyConstraint('seqrelid', name='pg_sequence_seqrelid_index'),
        {'schema': 'pg_catalog'}
    )

    seqrelid: Any = Field(sa_column=Column('seqrelid', OID, primary_key=True))
    seqtypid: Any = Field(sa_column=Column('seqtypid', OID))
    seqstart: int = Field(sa_column=Column('seqstart', BigInteger))
    seqincrement: int = Field(sa_column=Column('seqincrement', BigInteger))
    seqmax: int = Field(sa_column=Column('seqmax', BigInteger))
    seqmin: int = Field(sa_column=Column('seqmin', BigInteger))
    seqcache: int = Field(sa_column=Column('seqcache', BigInteger))
    seqcycle: bool = Field(sa_column=Column('seqcycle', Boolean))


class PgShdepend(SQLModel, table=True):
    __tablename__ = 'pg_shdepend'
    __table_args__ = (
        PrimaryKeyConstraint('dbid', 'classid', 'objid', 'objsubid', 'refclassid', 'refobjid', 'deptype',
                             name='pg_shdepend_depender_index'),
        Index('pg_shdepend_reference_index', 'refclassid', 'refobjid'),
        UniqueConstraint('dbid', 'classid', 'objid', 'objsubid', name='pg_shdepend_depender_unique_index'),
        {'schema': 'pg_catalog'}
    )

    dbid: Any = Field(sa_column=Column('dbid', OID, primary_key=True))
    classid: Any = Field(sa_column=Column('classid', OID, primary_key=True))
    objid: Any = Field(sa_column=Column('objid', OID, primary_key=True))
    objsubid: int = Field(sa_column=Column('objsubid', Integer, primary_key=True))
    refclassid: Any = Field(sa_column=Column('refclassid', OID, primary_key=True))
    refobjid: Any = Field(sa_column=Column('refobjid', OID, primary_key=True))
    deptype: str = Field(sa_column=Column('deptype', String, primary_key=True))


class PgShdescription(SQLModel, table=True):
    __tablename__ = 'pg_shdescription'
    __table_args__ = (
        PrimaryKeyConstraint('objoid', 'classoid', name='pg_shdescription_o_c_index'),
        {'schema': 'pg_catalog'}
    )

    objoid: Any = Field(sa_column=Column('objoid', OID, primary_key=True))
    classoid: Any = Field(sa_column=Column('classoid', OID, primary_key=True))
    description: str = Field(sa_column=Column('description', Text))


class PgShseclabel(SQLModel, table=True):
    __tablename__ = 'pg_shseclabel'
    __table_args__ = (
        PrimaryKeyConstraint('objoid', 'classoid', 'provider', name='pg_shseclabel_object_index'),
        {'schema': 'pg_catalog'}
    )

    objoid: Any = Field(sa_column=Column('objoid', OID, primary_key=True))
    classoid: Any = Field(sa_column=Column('classoid', OID, primary_key=True))
    provider: str = Field(sa_column=Column('provider', Text, primary_key=True))
    label: str = Field(sa_column=Column('label', Text))


class PgStatistic(SQLModel, table=True):
    __tablename__ = 'pg_statistic'
    __table_args__ = (
        PrimaryKeyConstraint('starelid', 'staattnum', 'stainherit', name='pg_statistic_relid_att_inh_index'),
        {'schema': 'pg_catalog'}
    )

    starelid: Any = Field(sa_column=Column('starelid', OID, primary_key=True))
    staattnum: int = Field(sa_column=Column('staattnum', SmallInteger, primary_key=True))
    stainherit: bool = Field(sa_column=Column('stainherit', Boolean, primary_key=True))
    stanullfrac: float = Field(sa_column=Column('stanullfrac', REAL))
    stawidth: int = Field(sa_column=Column('stawidth', Integer))
    stadistinct: float = Field(sa_column=Column('stadistinct', REAL))
    stakind1: int = Field(sa_column=Column('stakind1', SmallInteger))
    stakind2: int = Field(sa_column=Column('stakind2', SmallInteger))
    stakind3: int = Field(sa_column=Column('stakind3', SmallInteger))
    stakind4: int = Field(sa_column=Column('stakind4', SmallInteger))
    stakind5: int = Field(sa_column=Column('stakind5', SmallInteger))
    staop1: Any = Field(sa_column=Column('staop1', OID))
    staop2: Any = Field(sa_column=Column('staop2', OID))
    staop3: Any = Field(sa_column=Column('staop3', OID))
    staop4: Any = Field(sa_column=Column('staop4', OID))
    staop5: Any = Field(sa_column=Column('staop5', OID))
    stacoll1: Any = Field(sa_column=Column('stacoll1', OID))
    stacoll2: Any = Field(sa_column=Column('stacoll2', OID))
    stacoll3: Any = Field(sa_column=Column('stacoll3', OID))
    stacoll4: Any = Field(sa_column=Column('stacoll4', OID))
    stacoll5: Any = Field(sa_column=Column('stacoll5', OID))
    stanumbers1: Optional[list] = Field(default=None, sa_column=Column('stanumbers1', ARRAY(REAL())))
    stanumbers2: Optional[list] = Field(default=None, sa_column=Column('stanumbers2', ARRAY(REAL())))
    stanumbers3: Optional[list] = Field(default=None, sa_column=Column('stanumbers3', ARRAY(REAL())))
    stanumbers4: Optional[list] = Field(default=None, sa_column=Column('stanumbers4', ARRAY(REAL())))
    stanumbers5: Optional[list] = Field(default=None, sa_column=Column('stanumbers5', ARRAY(REAL())))
    stavalues1: Optional[Any] = Field(default=None, sa_column=Column('stavalues1', NullType))
    stavalues2: Optional[Any] = Field(default=None, sa_column=Column('stavalues2', NullType))
    stavalues3: Optional[Any] = Field(default=None, sa_column=Column('stavalues3', NullType))
    stavalues4: Optional[Any] = Field(default=None, sa_column=Column('stavalues4', NullType))
    stavalues5: Optional[Any] = Field(default=None, sa_column=Column('stavalues5', NullType))


class PgStatisticExt(SQLModel, table=True):
    __tablename__ = 'pg_statistic_ext'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_statistic_ext_oid_index'),
        UniqueConstraint('stxname', 'stxnamespace', name='pg_statistic_ext_name_index'),
        Index('pg_statistic_ext_relid_index', 'stxrelid'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    stxrelid: Any = Field(sa_column=Column('stxrelid', OID))
    stxname: str = Field(sa_column=Column('stxname', String))
    stxnamespace: Any = Field(sa_column=Column('stxnamespace', OID))
    stxowner: Any = Field(sa_column=Column('stxowner', OID))
    stxkeys: Any = Field(sa_column=Column('stxkeys', NullType))
    stxkind: list = Field(sa_column=Column('stxkind', ARRAY(String())))
    stxstattarget: Optional[int] = Field(default=None, sa_column=Column('stxstattarget', SmallInteger))
    stxexprs: Optional[Any] = Field(default=None, sa_column=Column('stxexprs', NullType))


class PgStatisticExtData(SQLModel, table=True):
    __tablename__ = 'pg_statistic_ext_data'
    __table_args__ = (
        PrimaryKeyConstraint('stxoid', 'stxdinherit', name='pg_statistic_ext_data_stxoid_inh_index'),
        {'schema': 'pg_catalog'}
    )

    stxoid: Any = Field(sa_column=Column('stxoid', OID, primary_key=True))
    stxdinherit: bool = Field(sa_column=Column('stxdinherit', Boolean, primary_key=True))
    stxdndistinct: Optional[Any] = Field(default=None, sa_column=Column('stxdndistinct', NullType))
    stxddependencies: Optional[Any] = Field(default=None, sa_column=Column('stxddependencies', NullType))
    stxdmcv: Optional[Any] = Field(default=None, sa_column=Column('stxdmcv', NullType))
    stxdexpr: Optional[Any] = Field(default=None, sa_column=Column('stxdexpr', NullType))


class PgSubscription(SQLModel, table=True):
    __tablename__ = 'pg_subscription'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_subscription_oid_index'),
        UniqueConstraint('subdbid', 'subname', name='pg_subscription_subname_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    subdbid: Any = Field(sa_column=Column('subdbid', OID))
    subskiplsn: Any = Field(sa_column=Column('subskiplsn', NullType))
    subname: str = Field(sa_column=Column('subname', String))
    subowner: Any = Field(sa_column=Column('subowner', OID))
    subenabled: bool = Field(sa_column=Column('subenabled', Boolean))
    subbinary: bool = Field(sa_column=Column('subbinary', Boolean))
    substream: str = Field(sa_column=Column('substream', String))
    subtwophasestate: str = Field(sa_column=Column('subtwophasestate', String))
    subdisableonerr: bool = Field(sa_column=Column('subdisableonerr', Boolean))
    subpasswordrequired: bool = Field(sa_column=Column('subpasswordrequired', Boolean))
    subrunasowner: bool = Field(sa_column=Column('subrunasowner', Boolean))
    subfailover: bool = Field(sa_column=Column('subfailover', Boolean))
    subconninfo: str = Field(sa_column=Column('subconninfo', Text))
    subsynccommit: str = Field(sa_column=Column('subsynccommit', Text))
    subpublications: list = Field(sa_column=Column('subpublications', ARRAY(Text())))
    subslotname: Optional[str] = Field(default=None, sa_column=Column('subslotname', String))
    suborigin: Optional[str] = Field(default=None, sa_column=Column('suborigin', Text))


class PgSubscriptionRel(SQLModel, table=True):
    __tablename__ = 'pg_subscription_rel'
    __table_args__ = (
        PrimaryKeyConstraint('srrelid', 'srsubid', name='pg_subscription_rel_srrelid_srsubid_index'),
        {'schema': 'pg_catalog'}
    )

    srsubid: Any = Field(sa_column=Column('srsubid', OID, primary_key=True))
    srrelid: Any = Field(sa_column=Column('srrelid', OID, primary_key=True))
    srsubstate: str = Field(sa_column=Column('srsubstate', String))
    srsublsn: Optional[Any] = Field(default=None, sa_column=Column('srsublsn', NullType))


class PgTablespace(SQLModel, table=True):
    __tablename__ = 'pg_tablespace'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_tablespace_oid_index'),
        UniqueConstraint('spcname', name='pg_tablespace_spcname_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    spcname: str = Field(sa_column=Column('spcname', String))
    spcowner: Any = Field(sa_column=Column('spcowner', OID))
    spcacl: Optional[Any] = Field(default=None, sa_column=Column('spcacl', NullType))
    spcoptions: Optional[list] = Field(default=None, sa_column=Column('spcoptions', ARRAY(Text())))


class PgTransform(SQLModel, table=True):
    __tablename__ = 'pg_transform'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_transform_oid_index'),
        UniqueConstraint('trftype', 'trflang', name='pg_transform_type_lang_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    trftype: Any = Field(sa_column=Column('trftype', OID))
    trflang: Any = Field(sa_column=Column('trflang', OID))
    trffromsql: Any = Field(sa_column=Column('trffromsql', NullType))
    trftosql: Any = Field(sa_column=Column('trftosql', NullType))


class PgTrigger(SQLModel, table=True):
    __tablename__ = 'pg_trigger'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_trigger_oid_index'),
        UniqueConstraint('tgrelid', 'tgname', name='pg_trigger_tgrelid_tgname_index'),
        Index('pg_trigger_tgconstraint_index', 'tgconstraint'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    tgrelid: Any = Field(sa_column=Column('tgrelid', OID))
    tgparentid: Any = Field(sa_column=Column('tgparentid', OID))
    tgname: str = Field(sa_column=Column('tgname', String))
    tgfoid: Any = Field(sa_column=Column('tgfoid', OID))
    tgtype: int = Field(sa_column=Column('tgtype', SmallInteger))
    tgenabled: str = Field(sa_column=Column('tgenabled', String))
    tgisinternal: bool = Field(sa_column=Column('tgisinternal', Boolean))
    tgconstrrelid: Any = Field(sa_column=Column('tgconstrrelid', OID))
    tgconstrindid: Any = Field(sa_column=Column('tgconstrindid', OID))
    tgconstraint: Any = Field(sa_column=Column('tgconstraint', OID))
    tgdeferrable: bool = Field(sa_column=Column('tgdeferrable', Boolean))
    tginitdeferred: bool = Field(sa_column=Column('tginitdeferred', Boolean))
    tgnargs: int = Field(sa_column=Column('tgnargs', SmallInteger))
    tgattr: Any = Field(sa_column=Column('tgattr', NullType))
    tgargs: bytes = Field(sa_column=Column('tgargs', LargeBinary))
    tgqual: Optional[Any] = Field(default=None, sa_column=Column('tgqual', NullType))
    tgoldtable: Optional[str] = Field(default=None, sa_column=Column('tgoldtable', String))
    tgnewtable: Optional[str] = Field(default=None, sa_column=Column('tgnewtable', String))


class PgTsConfig(SQLModel, table=True):
    __tablename__ = 'pg_ts_config'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_ts_config_oid_index'),
        UniqueConstraint('cfgname', 'cfgnamespace', name='pg_ts_config_cfgname_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    cfgname: str = Field(sa_column=Column('cfgname', String))
    cfgnamespace: Any = Field(sa_column=Column('cfgnamespace', OID))
    cfgowner: Any = Field(sa_column=Column('cfgowner', OID))
    cfgparser: Any = Field(sa_column=Column('cfgparser', OID))


class PgTsConfigMap(SQLModel, table=True):
    __tablename__ = 'pg_ts_config_map'
    __table_args__ = (
        PrimaryKeyConstraint('mapcfg', 'maptokentype', 'mapseqno', name='pg_ts_config_map_index'),
        {'schema': 'pg_catalog'}
    )

    mapcfg: Any = Field(sa_column=Column('mapcfg', OID, primary_key=True))
    maptokentype: int = Field(sa_column=Column('maptokentype', Integer, primary_key=True))
    mapseqno: int = Field(sa_column=Column('mapseqno', Integer, primary_key=True))
    mapdict: Any = Field(sa_column=Column('mapdict', OID))


class PgTsDict(SQLModel, table=True):
    __tablename__ = 'pg_ts_dict'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_ts_dict_oid_index'),
        UniqueConstraint('dictname', 'dictnamespace', name='pg_ts_dict_dictname_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    dictname: str = Field(sa_column=Column('dictname', String))
    dictnamespace: Any = Field(sa_column=Column('dictnamespace', OID))
    dictowner: Any = Field(sa_column=Column('dictowner', OID))
    dicttemplate: Any = Field(sa_column=Column('dicttemplate', OID))
    dictinitoption: Optional[str] = Field(default=None, sa_column=Column('dictinitoption', Text))


class PgTsParser(SQLModel, table=True):
    __tablename__ = 'pg_ts_parser'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_ts_parser_oid_index'),
        UniqueConstraint('prsname', 'prsnamespace', name='pg_ts_parser_prsname_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    prsname: str = Field(sa_column=Column('prsname', String))
    prsnamespace: Any = Field(sa_column=Column('prsnamespace', OID))
    prsstart: Any = Field(sa_column=Column('prsstart', NullType))
    prstoken: Any = Field(sa_column=Column('prstoken', NullType))
    prsend: Any = Field(sa_column=Column('prsend', NullType))
    prsheadline: Any = Field(sa_column=Column('prsheadline', NullType))
    prslextype: Any = Field(sa_column=Column('prslextype', NullType))


class PgTsTemplate(SQLModel, table=True):
    __tablename__ = 'pg_ts_template'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_ts_template_oid_index'),
        UniqueConstraint('tmplname', 'tmplnamespace', name='pg_ts_template_tmplname_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    tmplname: str = Field(sa_column=Column('tmplname', String))
    tmplnamespace: Any = Field(sa_column=Column('tmplnamespace', OID))
    tmplinit: Any = Field(sa_column=Column('tmplinit', NullType))
    tmpllexize: Any = Field(sa_column=Column('tmpllexize', NullType))


class PgType(SQLModel, table=True):
    __tablename__ = 'pg_type'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_type_oid_index'),
        UniqueConstraint('typname', 'typnamespace', name='pg_type_typname_nsp_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    typname: str = Field(sa_column=Column('typname', String))
    typnamespace: Any = Field(sa_column=Column('typnamespace', OID))
    typowner: Any = Field(sa_column=Column('typowner', OID))
    typlen: int = Field(sa_column=Column('typlen', SmallInteger))
    typbyval: bool = Field(sa_column=Column('typbyval', Boolean))
    typtype: str = Field(sa_column=Column('typtype', String))
    typcategory: str = Field(sa_column=Column('typcategory', String))
    typispreferred: bool = Field(sa_column=Column('typispreferred', Boolean))
    typisdefined: bool = Field(sa_column=Column('typisdefined', Boolean))
    typdelim: str = Field(sa_column=Column('typdelim', String))
    typrelid: Any = Field(sa_column=Column('typrelid', OID))
    typsubscript: Any = Field(sa_column=Column('typsubscript', NullType))
    typelem: Any = Field(sa_column=Column('typelem', OID))
    typarray: Any = Field(sa_column=Column('typarray', OID))
    typinput: Any = Field(sa_column=Column('typinput', NullType))
    typoutput: Any = Field(sa_column=Column('typoutput', NullType))
    typreceive: Any = Field(sa_column=Column('typreceive', NullType))
    typsend: Any = Field(sa_column=Column('typsend', NullType))
    typmodin: Any = Field(sa_column=Column('typmodin', NullType))
    typmodout: Any = Field(sa_column=Column('typmodout', NullType))
    typanalyze: Any = Field(sa_column=Column('typanalyze', NullType))
    typalign: str = Field(sa_column=Column('typalign', String))
    typstorage: str = Field(sa_column=Column('typstorage', String))
    typnotnull: bool = Field(sa_column=Column('typnotnull', Boolean))
    typbasetype: Any = Field(sa_column=Column('typbasetype', OID))
    typtypmod: int = Field(sa_column=Column('typtypmod', Integer))
    typndims: int = Field(sa_column=Column('typndims', Integer))
    typcollation: Any = Field(sa_column=Column('typcollation', OID))
    typdefaultbin: Optional[Any] = Field(default=None, sa_column=Column('typdefaultbin', NullType))
    typdefault: Optional[str] = Field(default=None, sa_column=Column('typdefault', Text))
    typacl: Optional[Any] = Field(default=None, sa_column=Column('typacl', NullType))

    def get_namespace(self, introspection: "Introspection") -> Optional["PgNamespace"]:
        return introspection.get_namespace(self.typnamespace)

    def get_owner(self, introspection: "Introspection") -> Any:
        return introspection.get_role(self.typowner)

    def get_class(self, introspection: "Introspection") -> Optional["PgClass"]:
        return introspection.get_class(self.typrelid)

    def get_elem_type(self, introspection: "Introspection") -> Optional["PgType"]:
        return introspection.get_type(self.typelem)

    def get_array_type(self, introspection: "Introspection") -> Optional["PgType"]:
        """Returns the array type of this type, if applicable."""
        return introspection.get_type(self.typarray)

    def get_enum_values(self, introspection: "Introspection") -> Optional[list]:
        return introspection.get_enums(self.oid)

    def get_range(self, introspection: "Introspection") -> Optional["PgRange"]:
        """Returns the range type associated with this type, if applicable."""
        return introspection.get_range(self.oid)

    def get_description(self, introspection: "Introspection") -> Optional[str]:
        return introspection.get_description(introspection.PG_TYPE, self.oid)

    def get_tags_and_description(self, introspection: "Introspection") -> Optional[dict]:
        return introspection.get_tags_and_description(introspection.PG_TYPE, self.oid)

    def get_tags(self, introspection: "Introspection") -> Optional[list]:
        tags_and_desc = self.get_tags_and_description(introspection)
        return tags_and_desc.get('tags') if tags_and_desc else None


class PgUserMapping(SQLModel, table=True):
    __tablename__ = 'pg_user_mapping'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_user_mapping_oid_index'),
        UniqueConstraint('umuser', 'umserver', name='pg_user_mapping_user_server_index'),
        {'schema': 'pg_catalog'}
    )

    oid: Any = Field(sa_column=Column('oid', OID, primary_key=True))
    umuser: Any = Field(sa_column=Column('umuser', OID))
    umserver: Any = Field(sa_column=Column('umserver', OID))
    umoptions: Optional[list] = Field(default=None, sa_column=Column('umoptions', ARRAY(Text())))


# TODO: This is technically a view, but we treat it as a table for consistency with other system catalogs.
class PgRoles(SQLModel, table=True):
    __tablename__ = 'pg_roles'
    __table_args__ = (
        PrimaryKeyConstraint('oid', name='pg_roles_oid_index'),
        {'schema': 'pg_catalog'}
    )

    rolname: Optional[str] = Field(default=None, sa_column=Column('rolname', Text))
    rolsuper: Optional[bool] = Field(default=None, sa_column=Column('rolsuper', Boolean))
    rolinherit: Optional[bool] = Field(default=None, sa_column=Column('rolinherit', Boolean))
    rolcreaterole: Optional[bool] = Field(default=None, sa_column=Column('rolcreaterole', Boolean))
    rolcreatedb: Optional[bool] = Field(default=None, sa_column=Column('rolcreatedb', Boolean))
    rolcanlogin: Optional[bool] = Field(default=None, sa_column=Column('rolcanlogin', Boolean))
    rolreplication: Optional[bool] = Field(default=None, sa_column=Column('rolreplication', Boolean))
    rolconnlimit: Optional[int] = Field(default=None, sa_column=Column('rolconnlimit', Integer))
    rolpassword: Optional[str] = Field(default=None, sa_column=Column('rolpassword', Text))
    rolvaliduntil: Optional[Any] = Field(default=None, sa_column=Column('rolvaliduntil', NullType))
    rolbypassrls: Optional[bool] = Field(default=None, sa_column=Column('rolbypassrls', Boolean))
    rolconfig: Optional[list] = Field(default=None, sa_column=Column('rolconfig', ARRAY(Text())))
    oid: Optional[Any] = Field(default=None, sa_column=Column('oid', OID))
