from fastapi import APIRouter

router = APIRouter()


@router.get("/")
def health():

    return {
        "status": "UP",
        "service": "PEMS Gateway"
    }