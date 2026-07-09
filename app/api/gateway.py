from contextlib import asynccontextmanager
from datetime import datetime, UTC
import asyncio

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from app.config.kafka import (
    producer,
    validation_results_consumer
)

from app.config.settings import (
    QR_SCAN_TOPIC
)

from app.utils.event_id import (
    generate_event_id
)


# ==========================================
# Global State
# ==========================================

active_connections = {}
main_loop = None


# ==========================================
# Kafka Validation Result Consumer
# ==========================================

def consume_validation_results():

    print("Listening on validation-results...")

    for message in validation_results_consumer:

        result = message.value

        pem_id = result.get("pemId")

        websocket = active_connections.get(pem_id)

        if websocket:

            try:

                asyncio.run_coroutine_threadsafe(
                    websocket.send_json(result),
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


# ==========================================
# Health Check
# ==========================================

@app.get("/")
def health():

    return {
        "status": "UP",
        "service": "PEMS Gateway"
    }


# ==========================================
# WebSocket Endpoint
# ==========================================

@app.websocket("/ws/{pem_id}")
async def websocket_endpoint(
    websocket: WebSocket,
    pem_id: str
):

    await websocket.accept()

    active_connections[pem_id] = websocket

    print(f"{pem_id} Connected")

    try:

        while True:

            data = await websocket.receive_json()

            print(f"Received -> {data}")

            event = {

                "eventId": generate_event_id(),

                "userId": data["userId"],

                "pemId": pem_id,

                "scanTimestamp": datetime.now().isoformat(),

                "source": "QR_SCANNER"
            }

            producer.send(
                QR_SCAN_TOPIC,
                value=event
            )

            producer.flush()

            print(f"Produced -> {event['eventId']}")

    except WebSocketDisconnect:

        print(f"{pem_id} Disconnected")

        active_connections.pop(
            pem_id,
            None
        )

    except Exception as e:

        print(f"WebSocket Error: {e}")

        active_connections.pop(
            pem_id,
            None
        )