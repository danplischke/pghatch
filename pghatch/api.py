from fastapi import FastAPI
from pghatch.router.router import SchemaRouter

app = FastAPI()
router = SchemaRouter(connection_str="postgres://postgres:postgres@localhost:5432/postgres", schema="public")
app.include_router(router)
