import json

from app.config.redis import redis_client


class CacheService:

    USER_TTL = 600       # 10 minutes

    GATEWAY_TTL = 1800   # 30 minutes

    # -----------------------
    # User Cache
    # -----------------------

    def get_user(self, user_id):

        key = f"user:{user_id}"

        data = redis_client.get(key)

        if data:

            return json.loads(data), True

        return None, False

    def set_user(self, user_id, user):

        key = f"user:{user_id}"

        redis_client.setex(
            key,
            self.USER_TTL,
            json.dumps(user)
        )

    # -----------------------
    # Gateway Cache
    # -----------------------

    def get_gateway(self, gateway_id):

        key = f"gateway:{gateway_id}"

        data = redis_client.get(key)

        if data:

            return json.loads(data), True

        return None, False

    def set_gateway(self, gateway_id, gateway):

        key = f"gateway:{gateway_id}"

        redis_client.setex(
            key,
            self.GATEWAY_TTL,
            json.dumps(gateway)
        )