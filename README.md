# 🐣 pghatch

**Automatically create a REST API for your PostgreSQL database**

pghatch is a Python tool that introspects your PostgreSQL database schema and automatically generates a fully-featured REST API using FastAPI. It watches for schema changes in real-time and updates the API endpoints accordingly.

## ✨ Features

- **🔄 Real-time Schema Watching**: Automatically detects database schema changes and updates API endpoints
- **📊 Complete Database Coverage**: Supports tables, views, materialized views, foreign tables, partitioned tables, functions, and procedures
- **🚀 FastAPI Integration**: Built on FastAPI for high performance and automatic OpenAPI documentation
- **🔍 PostgreSQL Introspection**: Deep integration with PostgreSQL system catalogs for accurate schema reflection
- **🧪 Comprehensive Testing**: 100% test coverage with unit, integration, and end-to-end tests
- **🐳 Docker Ready**: Includes Docker Compose setup for easy development

## 🚀 Quick Start

### Prerequisites

- Python 3.12+
- PostgreSQL database
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

### Installation

```bash
# Clone the repository
git clone https://github.com/danplischke/pgrestcue.git
cd pgrestcue

# Install dependencies with uv
uv sync

# Or with pip
pip install -e .
```

### Running with Docker

```bash
# Start PostgreSQL database
docker-compose up -d

# The database will be available at localhost:5432
# Default credentials: postgres/postgres
```

### Start the API Server

```bash
# Run the development server
python -m pghatch

# Or with uvicorn directly
uvicorn pghatch.api:app --reload --host 0.0.0.0 --port 8000
```

The API will be available at `http://localhost:8000` with automatic OpenAPI documentation at `http://localhost:8000/docs`.

## 🏗️ Architecture

pghatch consists of several key components:

### Core Components

- **`pghatch.api`**: FastAPI application entry point
- **`pghatch.router`**: Dynamic router that creates endpoints based on database schema
- **`pghatch.introspection`**: PostgreSQL system catalog introspection utilities
- **`pghatch.router.resolver`**: Resolvers for different database object types (tables, functions, etc.)

### Schema Watching

pghatch uses PostgreSQL event triggers to watch for DDL changes:

- Creates a `pghatch_watch` schema with notification functions
- Sets up event triggers for DDL commands and object drops
- Automatically rebuilds API routes when schema changes are detected
- Maintains connection health with periodic checks

## 🛠️ Development

### Development Tools

This project uses [just](https://github.com/casey/just) as a command runner:

```bash
# Format code
just format

# Lint code
just lint

# Run tests
just test

# Generate introspection models
just codegen
```

### Testing

The project maintains 100% test coverage:

```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov=pghatch --cov-report=html

# Run specific test types
pytest -m unit        # Unit tests only
pytest -m integration # Integration tests only
pytest -m e2e         # End-to-end tests only
```

### Project Structure

```
pghatch/
├── pghatch/
│   ├── __init__.py
│   ├── __main__.py              # Application entry point
│   ├── api.py                   # FastAPI application
│   ├── logging_config.py        # Logging configuration
│   ├── introspection/           # Database introspection
│   │   ├── __init__.py
│   │   ├── acl.py              # Access control lists
│   │   ├── introspection.py    # Main introspection logic
│   │   ├── pgtypes.py          # PostgreSQL type mappings
│   │   └── tables.py           # Generated SQLModel classes
│   └── router/                  # API routing
│       ├── __init__.py
│       ├── router.py           # Schema router with watching
│       └── resolver/           # Object resolvers
│           ├── __init__.py
│           ├── proc_resolver.py    # Function/procedure resolver
│           ├── resolver.py         # Base resolver
│           └── table_resolver.py   # Table/view resolver
├── tests/                       # Test suite
│   ├── unit/                   # Unit tests
│   ├── integration/            # Integration tests
│   └── e2e/                    # End-to-end tests
├── docker-compose.yaml         # PostgreSQL development setup
├── pyproject.toml             # Project configuration
└── .justfile                  # Development commands
```

## 🔧 Configuration

### Database Connection

By default, pghatch connects to:
- Host: `localhost`
- Port: `5432`
- Database: `postgres`
- Username: `postgres`
- Password: `postgres`

### Schema Selection

The API currently introspects the `public` schema. This can be configured in `pghatch/api.py`:

```python
router = SchemaRouter(schema="your_schema_name")
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests (`just test`)
5. Format and lint code (`just format && just lint`)
6. Commit your changes (`git commit -m 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

## 📝 License

This project is licensed under the terms specified in the [LICENSE](LICENSE) file.

## 🙏 Acknowledgments

- Built with [FastAPI](https://fastapi.tiangolo.com/) for the web framework
- Uses [asyncpg](https://github.com/MagicStack/asyncpg) for PostgreSQL connectivity
- Powered by [SQLModel](https://sqlmodel.tiangolo.com/) for data modeling
- Schema introspection inspired by PostgREST and similar tools

---

**Note**: This project is in early development (v0.0.1). APIs and features may change as the project evolves.
