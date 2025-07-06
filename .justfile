

format:
    ruff format ./pghatch

lint:
    ruff check ./pghatch --fix

codegen:
    sqlacodegen --generator sqlmodels --schemas pg_catalog,information_schema --outfile ./pghatch/introspection/tables.py postgresql://postgres:postgres@localhost/postgres

test:
    pytest --cov=pghatch --cov-report=term-missing --cov-report=html --cov-report=xml