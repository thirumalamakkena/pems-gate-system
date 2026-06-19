from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from kafka import KafkaProducer, KafkaConsumer

from datetime import datetime, UTC
import asyncio
import json
import uuid


# ==========================================
# Global State
# ==========================================

active_connections = {}
main_loop = None

# ==========================================
# Kafka Producer
# ==========================================

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ==========================================
# Kafka Result Consumer
# ==========================================

def consume_validation_results():

    consumer = KafkaConsumer(
        "validation-results",
        bootstrap_servers="localhost:9092",
        group_id="gateway-group",
        auto_offset_reset="latest",
        value_deserializer=lambda x: json.loads(
            x.decode("utf-8")
        )
    )

    print("Listening on validation-results...")

    for message in consumer:

        result = message.value

        pem_id = result.get("pemId")

        websocket = active_connections.get(pem_id)

        if websocket:

            try:

                asyncio.run_coroutine_threadsafe(
                    websocket.send_json(result),
                    main_loop
                )

                print(
                    f"Sent result to {pem_id}: {result}"
                )

            except Exception as e:

                print(
                    f"Push Error: {e}"
                )

# ==========================================
# Lifespan
# ==========================================

@asynccontextmanager
async def lifespan(app: FastAPI):

    global main_loop

    main_loop = asyncio.get_running_loop()

    print("Starting Gateway...")

    consumer_task = asyncio.to_thread(
        consume_validation_results
    )

    asyncio.create_task(consumer_task)

    yield

    print("Stopping Gateway...")


app = FastAPI(
    lifespan=lifespan
)

# ==========================================
# CORS
# ==========================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# ==========================================
# Health Check
# ==========================================

@app.get("/")
def health():

    return {
        "status": "running",
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

    print(
        f"Incoming WebSocket: {pem_id}"
    )

    await websocket.accept()

    active_connections[pem_id] = websocket

    print(
        f"{pem_id} Connected"
    )

    try:

        while True:

            data = await websocket.receive_json()

            print(
                f"Received from {pem_id}: {data}"
            )

            event = {
                "eventId": str(uuid.uuid4()),
                "userId": data["userId"],
                "pemId": pem_id,
                "timestamp": datetime.now(
                    UTC
                ).isoformat()
            }

            producer.send(
                "qr-scans",
                value=event
            )

            producer.flush()

            print(
                f"Produced: {event}"
            )

    except WebSocketDisconnect:

        print(
            f"{pem_id} Disconnected"
        )

        active_connections.pop(
            pem_id,
            None
        )

    except Exception as e:

        print(
            f"WebSocket Error: {e}"
        )

        active_connections.pop(
            pem_id,
            None
        )