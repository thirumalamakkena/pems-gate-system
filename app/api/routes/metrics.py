from fastapi import APIRouter

from app.repositories.metrics_repository import (
    MetricsRepository
)

router = APIRouter()

repository = MetricsRepository()


@router.get("/metrics")
def get_metrics():

    return repository.get_all_metrics()


@router.get("/metrics/{service_name}")
def get_service_metrics(
    service_name: str
):

    return repository.get_metrics_by_service(
        service_name
    )