from datetime import datetime

dt = datetime.now()

print(dt.isoformat(timespec="milliseconds"))