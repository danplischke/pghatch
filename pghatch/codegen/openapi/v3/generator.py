from typing import Dict, Any

from pghatch.codegen.generator import OpenAPIGenerator
from pghatch.codegen.openapi.v3.model import Model


class OpenAPIV3Generator(OpenAPIGenerator):

    def __init__(self, openapi: Model):
        super().__init__()
        self.openapi = openapi

    def add_model(self, model_name: str, fields: Dict[str, Any]) -> None:
        pass

    def add_endpoint(self, path: str, method: str, operation: Dict[str, Any]) -> None:
        pass

    def generate(self):
        pass