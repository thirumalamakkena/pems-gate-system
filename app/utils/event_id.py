from itertools import count
from datetime import datetime

_counter = count(1)

def generate_event_id():

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    sequence = next(_counter)

    return f"EVT-{timestamp}-{sequence:03d}"