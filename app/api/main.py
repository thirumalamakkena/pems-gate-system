from fastapi import FastAPI

from app.api.routes.metrics import router as metrics_router
from app.api.routes.analytics import router as analytics_router
from app.api.routes.health import router as health_router

app = FastAPI(
    title="PEMS Analytics Platform"
)

app.include_router(metrics_router)
app.include_router(analytics_router)
app.include_router(health_router)