from collections.abc import Callable
from typing import Any

from pydantic import create_model as pydantic_create_model, ConfigDict
from pydantic.main import ModelT


def create_model(model_name: str,
                 /,
                 *,
                 __config__: ConfigDict | None = None,
                 __doc__: str | None = None,
                 __base__: type[ModelT] | tuple[type[ModelT], ...] | None = None,
                 __module__: str | None = None,
                 __validators__: dict[str, Callable] | None = None,
                 __cls_kwargs__: dict[str, Any] | None = None,
                 **field_definitions: tuple[str, Any]):
    """
    Wrapper around pydantic's create_model to allow for easier mocking in tests.
    """

    # TODO: register the model in a global registry to check overlap

    return pydantic_create_model(model_name,
                                 __config__=__config__,
                                 __doc__=__doc__,
                                 __base__=__base__,
                                 __module__=__module__,
                                 __validators__=__validators__,
                                 __cls_kwargs__=__cls_kwargs__,
                                 **field_definitions)
