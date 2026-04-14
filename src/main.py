import logging

from fastapi import FastAPI

from src.api.routes.stations import router as stations_router

logging.basicConfig(level=logging.INFO)

app = FastAPI(title="EcoPlant Metrics Service")

app.include_router(stations_router)


@app.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "ok"}