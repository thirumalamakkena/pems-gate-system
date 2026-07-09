from app.repositories.user_repository import (
    UserRepository
)

from app.repositories.gateway_repository import (
    GatewayRepository
)


class ValidationService:

    def __init__(self):
        self.user_repository = UserRepository()
        self.gateway_repository = GatewayRepository()
        self.user_cache = {}
        self.gateway_cache = {}

    def validate_scan(
        self,
        user_id,
        pem_id
    ):
        if user_id in self.user_cache:

            
            hit_or_miss = True
            user = self.user_cache[user_id]

        else:
            hit_or_miss = False

            user = self.user_repository.find_by_user_id(
                user_id
            )

            if user:
                self.user_cache[user_id] = user

       

        if not user:
            return {
                "status": "INVALID_USER",
                "hitOrMiss": hit_or_miss
            }

        if user["status"] != "ACTIVE":
            return {
                "status": "INACTIVE_USER",
                "hitOrMiss": hit_or_miss
            }

       

        if pem_id in self.gateway_cache:

            gateway = self.gateway_cache[pem_id]

        else:


            gateway = self.gateway_repository.find_by_gateway_id(pem_id)

            if gateway:
                self.gateway_cache[pem_id] = gateway



        if not gateway:
            return {
                "status": "GATEWAY_NOT_FOUND",
                "hitOrMiss": hit_or_miss
            }

        if gateway["status"] != "ACTIVE":
            return {
                "status": "GATEWAY_DISABLED",
                "hitOrMiss": hit_or_miss
            }

        if pem_id not in user["allowedPems"]:
            return {
                "status":
                "UNAUTHORIZED_GATEWAY",
                "hitOrMiss": hit_or_miss
            }

        return {
            "status": "VALID",
            "hitOrMiss": hit_or_miss
        }