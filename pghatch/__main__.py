if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "pghatch.api:app", host="0.0.0.0", port=8000, log_level="info", reload=True, workers=6
    )
