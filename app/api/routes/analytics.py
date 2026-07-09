from fastapi import APIRouter

from app.repositories.analytics_repository import (
    AnalyticsRepository
)

router = APIRouter()

repository = AnalyticsRepository()


@router.get("/analytics/hourly")
def hourly():

    return repository.get_hourly_analytics()


@router.get("/analytics/gateway/{pem_id}")
def gateway(
    pem_id: str
):

    return repository.get_gateway_analytics(
        pem_id
    )