from fastapi import APIRouter
from fastapi import  WebSocket, WebSocketDisconnect
from app.utils.event_builder import EventBuilder

from app.config.settings import ACTIVE_CONNECTIONS

from app.config.kafka import (
    producer
)

from app.config.settings import (
    QR_SCAN_TOPIC
)

from app.utils.event_id import (
    generate_event_id
)

router = APIRouter()

active_connections = ACTIVE_CONNECTIONS

@router.websocket("/ws/{pem_id}")
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


            event = EventBuilder.qr_scan_event(
                event_id=generate_event_id(),
                user_id=data["userId"],
                pem_id=pem_id,
                event_time=data["eventTime"],
                source=data["source"]
            )

            producer.send(
                QR_SCAN_TOPIC,
                value=event
            )

            producer.flush()
            event_id = event["payload"]
            print(f"Produced -> {event_id}")

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