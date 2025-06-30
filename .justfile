



codegen:
    sqlacodegen --generator sqlmodels --schemas pg_catalog,information_schema --outfile ./pgrestcue/introspection/tables.py postgresql://postgres:postgres@localhost/postgres