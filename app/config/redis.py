import redis
from app.config.settings import (REDIS_PORT,REDIS_HOST)

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True
)