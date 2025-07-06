from fastapi import FastAPI
from pghatch.router.router import SchemaRouter

app = FastAPI()
router = SchemaRouter(schema="public")
app.include_router(router)
