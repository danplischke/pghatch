from dataclasses import dataclass
from typing import List, Dict, Optional, Any, TYPE_CHECKING
from pghatch.introspection.tables import PgRoles

if TYPE_CHECKING:
    from pghatch.introspection.introspection import Introspection

OBJECT_COLUMN = "OBJECT_COLUMN"
OBJECT_TABLE = "OBJECT_TABLE"
OBJECT_SEQUENCE = "OBJECT_SEQUENCE"
OBJECT_DATABASE = "OBJECT_DATABASE"
OBJECT_FUNCTION = "OBJECT_FUNCTION"
OBJECT_LANGUAGE = "OBJECT_LANGUAGE"
OBJECT_LARGEOBJECT = "OBJECT_LARGEOBJECT"
OBJECT_SCHEMA = "OBJECT_SCHEMA"
OBJECT_TABLESPACE = "OBJECT_TABLESPACE"
OBJECT_FDW = "OBJECT_FDW"
OBJECT_FOREIGN_SERVER = "OBJECT_FOREIGN_SERVER"
OBJECT_DOMAIN = "OBJECT_DOMAIN"
OBJECT_TYPE = "OBJECT_TYPE"

AclDefaultObjectType = str  # For type hinting

ACL_SELECT = "r"
ACL_INSERT = "a"
ACL_UPDATE = "w"
ACL_DELETE = "d"
ACL_TRUNCATE = "D"
ACL_REFERENCES = "x"
ACL_TRIGGER = "t"
ACL_CREATE = "C"
ACL_CONNECT = "c"
ACL_CREATE_TEMP = "T"
ACL_MAINTAIN = "m"
ACL_EXECUTE = "X"
ACL_USAGE = "U"
ACL_NO_RIGHTS = ""

ACL_ALL_RIGHTS_RELATION = f"{ACL_INSERT}{ACL_SELECT}{ACL_UPDATE}{ACL_DELETE}{ACL_TRUNCATE}{ACL_REFERENCES}{ACL_TRIGGER}{ACL_MAINTAIN}"
ACL_ALL_RIGHTS_SEQUENCE = f"{ACL_USAGE}{ACL_SELECT}{ACL_UPDATE}"
ACL_ALL_RIGHTS_DATABASE = f"{ACL_CREATE}{ACL_CREATE_TEMP}{ACL_CONNECT}"
ACL_ALL_RIGHTS_FDW = ACL_USAGE
ACL_ALL_RIGHTS_FOREIGN_SERVER = ACL_USAGE
ACL_ALL_RIGHTS_FUNCTION = ACL_EXECUTE
ACL_ALL_RIGHTS_LANGUAGE = ACL_USAGE
ACL_ALL_RIGHTS_LARGEOBJECT = f"{ACL_SELECT}{ACL_UPDATE}"
ACL_ALL_RIGHTS_SCHEMA = f"{ACL_USAGE}{ACL_CREATE}"
ACL_ALL_RIGHTS_TABLESPACE = ACL_CREATE
ACL_ALL_RIGHTS_TYPE = ACL_USAGE

ACL_ALL_RIGHTS_STR = "arwdDxtXUCTcsAm"

# PUBLIC_ROLE as a PgRoles instance
PUBLIC_ROLE = PgRoles(
    rolname="public",
    rolsuper=False,
    rolinherit=False,
    rolcreaterole=False,
    rolcreatedb=False,
    rolcanlogin=False,
    rolreplication=False,
    rolconnlimit=None,
    rolpassword=None,
    rolbypassrls=False,
    rolconfig=None,
    rolvaliduntil=None,
    oid="0",
)

# Permission mapping
ACL_MAP = {
    ACL_SELECT: "select",
    ACL_UPDATE: "update",
    ACL_INSERT: "insert",
    ACL_DELETE: "delete",
    ACL_TRUNCATE: "truncate",
    ACL_REFERENCES: "references",
    ACL_TRIGGER: "trigger",
    ACL_EXECUTE: "execute",
    ACL_USAGE: "usage",
    ACL_CREATE: "create",
    ACL_CONNECT: "connect",
    ACL_CREATE_TEMP: "temporary",
    ACL_MAINTAIN: "maintain",
}

ACL_MAP_ENTRIES = sorted(
    ACL_MAP.items(), key=lambda item: ACL_ALL_RIGHTS_STR.index(item[0])
)


@dataclass
class AclObject:
    role: str
    granter: str
    select: bool = False
    selectGrant: bool = False
    update: bool = False
    updateGrant: bool = False
    insert: bool = False
    insertGrant: bool = False
    delete: bool = False
    deleteGrant: bool = False
    truncate: bool = False
    truncateGrant: bool = False
    references: bool = False
    referencesGrant: bool = False
    trigger: bool = False
    triggerGrant: bool = False
    execute: bool = False
    executeGrant: bool = False
    usage: bool = False
    usageGrant: bool = False
    create: bool = False
    createGrant: bool = False
    connect: bool = False
    connectGrant: bool = False
    temporary: bool = False
    temporaryGrant: bool = False
    maintain: bool = False
    maintainGrant: bool = False


ResolvedPermissions = Dict[str, bool]

# NO_PERMISSIONS
NO_PERMISSIONS = AclObject(role="public", granter="")

# parseIdentifier


def parse_identifier(s: str) -> str:
    if s.startswith('"'):
        if not s.endswith('"'):
            raise ValueError(
                'Invalid identifier - if it starts with " it must also end with "'
            )
        return s[1:-1].replace('""', '"')
    return s


def get_role(introspection: "Introspection", oid: str) -> PgRoles:
    if oid == "0":
        return PUBLIC_ROLE
    for role in introspection.roles:
        if getattr(role, "oid", None) == oid or getattr(role, "_id", None) == oid:
            return role
    raise ValueError(f"Could not find role with identifier '{oid}'")


def get_role_by_name(introspection: "Introspection", name: str) -> PgRoles:
    if name == "public":
        return PUBLIC_ROLE
    for role in introspection.roles:
        if getattr(role, "rolname", None) == name:
            return role
    raise ValueError(f"Could not find role with name '{name}'")


def parse_acl(acl_string: str) -> AclObject:
    if len(acl_string) < 3:
        raise ValueError("Invalid ACL string: too few characters")
    acl = AclObject(**NO_PERMISSIONS.__dict__)
    equals_sign_index = acl_string.find("=")
    if equals_sign_index == -1:
        raise ValueError(f"Could not parse ACL string '{acl_string}' - no '=' symbol")
    if equals_sign_index > 0:
        acl.role = parse_identifier(acl_string[:equals_sign_index])
    i = equals_sign_index
    last_character_index = len(acl_string) - 1
    while i + 1 < len(acl_string):
        i += 1
        char = acl_string[i]
        if char == "/":
            i += 1
            if i == len(acl_string):
                raise ValueError("ACL string should have a granter after the /")
            acl.granter = parse_identifier(acl_string[i:])
            return acl
        perm = ACL_MAP.get(char)
        if perm is None:
            raise ValueError(
                f"Could not parse ACL string '{acl_string}' - unsupported permission '{char}'"
            )
        setattr(acl, perm, True)
        if i < last_character_index and acl_string[i + 1] == "*":
            i += 1
            setattr(acl, f"{perm}Grant", True)
    raise ValueError(
        f"Invalid or unsupported ACL string '{acl_string}' - no '/' character?"
    )


def escape_role(role: str) -> str:
    if '"' in role:
        return f'"{role.replace('"', '""')}"'
    return role


def serialize_acl(acl: AclObject) -> str:
    permissions = ("" if acl.role == "public" else escape_role(acl.role)) + "="
    for char, perm in ACL_MAP_ENTRIES:
        if getattr(acl, f"{perm}Grant"):
            permissions += char + "*"
        elif getattr(acl, perm):
            permissions += char
    permissions += f"/{escape_role(acl.granter)}"
    return permissions


empty_acl_object = parse_acl("=/postgres")


def parse_acls(
    introspection: "Introspection",
    in_acls: Optional[List[str]],
    owner_id: str,
    objtype: str,
) -> List[AclObject]:
    acl_strings = in_acls
    if acl_strings is None:
        owner = get_role(introspection, owner_id)
        if objtype == OBJECT_COLUMN:
            world_default = ACL_NO_RIGHTS
            owner_default = ACL_NO_RIGHTS
        elif objtype == OBJECT_TABLE:
            world_default = ACL_NO_RIGHTS
            owner_default = ACL_ALL_RIGHTS_RELATION
        elif objtype == OBJECT_SEQUENCE:
            world_default = ACL_NO_RIGHTS
            owner_default = ACL_ALL_RIGHTS_SEQUENCE
        elif objtype == OBJECT_DATABASE:
            world_default = f"{ACL_CREATE_TEMP}{ACL_CONNECT}"
            owner_default = ACL_ALL_RIGHTS_DATABASE
        elif objtype == OBJECT_FUNCTION:
            world_default = ACL_EXECUTE
            owner_default = ACL_ALL_RIGHTS_FUNCTION
        elif objtype == OBJECT_LANGUAGE:
            world_default = ACL_USAGE
            owner_default = ACL_ALL_RIGHTS_LANGUAGE
        elif objtype == OBJECT_LARGEOBJECT:
            world_default = ACL_NO_RIGHTS
            owner_default = ACL_ALL_RIGHTS_LARGEOBJECT
        elif objtype == OBJECT_SCHEMA:
            world_default = ACL_NO_RIGHTS
            owner_default = ACL_ALL_RIGHTS_SCHEMA
        elif objtype == OBJECT_TABLESPACE:
            world_default = ACL_NO_RIGHTS
            owner_default = ACL_ALL_RIGHTS_TABLESPACE
        elif objtype == OBJECT_FDW:
            world_default = ACL_NO_RIGHTS
            owner_default = ACL_ALL_RIGHTS_FDW
        elif objtype == OBJECT_FOREIGN_SERVER:
            world_default = ACL_NO_RIGHTS
            owner_default = ACL_ALL_RIGHTS_FOREIGN_SERVER
        elif objtype in (OBJECT_DOMAIN, OBJECT_TYPE):
            world_default = ACL_USAGE
            owner_default = ACL_ALL_RIGHTS_TYPE
        else:
            world_default = ACL_NO_RIGHTS
            owner_default = ACL_NO_RIGHTS
        acl = []
        if world_default != ACL_NO_RIGHTS:
            acl.append(f"={world_default}/{owner.rolname}")
        if owner_default != ACL_NO_RIGHTS:
            acl.append(f"{owner.rolname}={owner_default}/{owner.rolname}")
        acl_strings = acl
    return [parse_acl(s) for s in acl_strings]


Permission = {
    "select": "select",
    "selectGrant": "selectGrant",
    "update": "update",
    "updateGrant": "updateGrant",
    "insert": "insert",
    "insertGrant": "insertGrant",
    "delete": "delete",
    "deleteGrant": "deleteGrant",
    "truncate": "truncate",
    "truncateGrant": "truncateGrant",
    "references": "references",
    "referencesGrant": "referencesGrant",
    "trigger": "trigger",
    "triggerGrant": "triggerGrant",
    "execute": "execute",
    "executeGrant": "executeGrant",
    "usage": "usage",
    "usageGrant": "usageGrant",
    "create": "create",
    "createGrant": "createGrant",
    "connect": "connect",
    "connectGrant": "connectGrant",
    "temporary": "temporary",
    "temporaryGrant": "temporaryGrant",
    "maintain": "maintain",
    "maintainGrant": "maintainGrant",
}


def expand_roles(
    introspection: "Introspection",
    roles: List[PgRoles],
    include_no_inherit: bool = False,
) -> List[PgRoles]:
    all_roles = [PUBLIC_ROLE]

    def add_role(member: PgRoles):
        if member not in all_roles:
            all_roles.append(member)
            if include_no_inherit or getattr(member, "rolinherit", True):
                for am in introspection.auth_members:
                    if getattr(am, "member", None) == getattr(
                        member, "oid", None
                    ) or getattr(am, "member", None) == getattr(member, "_id", None):
                        rol = get_role(introspection, getattr(am, "roleid", None))
                        add_role(rol)

    for r in roles:
        add_role(r)
    return all_roles


def acl_contains_role(
    introspection: "Introspection",
    acl: AclObject,
    role: PgRoles,
    include_no_inherit: bool = False,
) -> bool:
    acl_role = get_role_by_name(introspection, acl.role)
    expanded_roles = expand_roles(introspection, [role], include_no_inherit)
    return acl_role in expanded_roles


def resolve_permissions(
    introspection: "Introspection",
    acls: List[AclObject],
    role: PgRoles,
    include_no_inherit: bool = False,
    is_owner_and_has_no_explicit_acls: bool = False,
) -> ResolvedPermissions:
    expanded_roles = expand_roles(introspection, [role], include_no_inherit)
    is_superuser = any(getattr(r, "rolsuper", False) for r in expanded_roles)
    grant_all = is_superuser or is_owner_and_has_no_explicit_acls
    permissions = {k: grant_all for k in Permission}
    if grant_all:
        return permissions
    for acl in acls:
        if acl_contains_role(introspection, acl, role, include_no_inherit):
            for k in Permission:
                permissions[k] = permissions[k] or getattr(acl, k, False)
    return permissions


def entity_permissions(
    introspection: "Introspection",
    entity: Any,  # Should have get_acl() and get_owner() or getClass().getOwner()
    role: PgRoles,
    include_no_inherit: bool = False,
) -> ResolvedPermissions:
    acls = entity.get_acl()
    owner = (
        entity.get_class().get_owner()
        if getattr(entity, "_type", None) == "PgAttribute"
        and hasattr(entity, "get_class")
        and callable(entity.get_class)
        else entity.get_owner()
        if hasattr(entity, "get_owner") and callable(entity.get_owner)
        else None
    )
    is_owner_and_has_no_explicit_acls = (
        owner is not None
        and owner == role
        and not any(acl.role == getattr(owner, "rolname", None) for acl in acls)
        and (
            getattr(entity, "_type", None) != "PgAttribute"
            or not (
                hasattr(entity, "get_class")
                and callable(entity.get_class)
                and hasattr(entity.get_class(), "get_acl")
                and callable(entity.get_class().get_acl)
                and any(
                    acl.role == getattr(owner, "rolname", None)
                    for acl in entity.get_class().get_acl()
                )
            )
        )
    )
    return resolve_permissions(
        introspection,
        acls,
        role,
        include_no_inherit,
        is_owner_and_has_no_explicit_acls,
    )
