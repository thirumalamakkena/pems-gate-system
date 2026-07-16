from contextlib import asynccontextmanager
import asyncio

from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI

from app.api.routes.metrics import router as metrics_router
from app.api.routes.analytics import router as analytics_router
from app.api.routes.health import router as health_router
from app.api.routes.web_socket import router as web_socket_router

from app.config.kafka import (
    validation_results_consumer
)

from app.config.settings import ACTIVE_CONNECTIONS



# ==========================================
# Global State
# ==========================================

main_loop = None
active_connections = ACTIVE_CONNECTIONS

# ==========================================
# Kafka Validation Result Consumer
# ==========================================

def consume_validation_results():

    print("Listening on validation-results...")

    for message in validation_results_consumer:

        result = message.value

        payload = result.get("payload")
        pem_id = payload.get("pemId")

        websocket = active_connections.get(pem_id)

        if websocket:

            try:

                asyncio.run_coroutine_threadsafe(
                    websocket.send_json(payload),
                    main_loop
                )

                print(f"Sent Result -> {pem_id}")

            except Exception as e:

                print(f"Push Error: {e}")


# ==========================================
# FastAPI Lifespan
# ==========================================

@asynccontextmanager
async def lifespan(app: FastAPI):

    global main_loop

    main_loop = asyncio.get_running_loop()

    print("Starting Gateway...")

    asyncio.create_task(
        asyncio.to_thread(
            consume_validation_results
        )
    )

    yield

    print("Stopping Gateway...")
    validation_results_consumer.close()


# ==========================================
# FastAPI App
# ==========================================

app = FastAPI(
    title="PEMS Gateway",
    lifespan=lifespan
)


# ==========================================
# CORS
# ==========================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"]
)



app.include_router(metrics_router)
app.include_router(analytics_router)
app.include_router(health_router)
app.include_router(web_socket_router)


