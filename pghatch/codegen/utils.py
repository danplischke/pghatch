import httpx


def endpoint(filter: str,
             limit: int | None = None,
             client: httpx.Client | None = None,
             **kwargs) -> EndpointModel:
    if not client:
        client = httpx.Client()
    response = client.request(method="GET", url="http://localhost:8000/api",
                              params={"filter": filter, "limit": limit})
    response.raise_for_status()

    return EndpointModel.model_validate(response.json())
