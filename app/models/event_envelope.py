from dataclasses import dataclass, asdict
from typing import Any
from datetime import datetime


@dataclass
class EventEnvelope:

    metadata: dict

    payload: dict

    def to_dict(self) -> dict:
        return asdict(self)