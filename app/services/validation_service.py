from app.repositories.user_repository import (
    UserRepository
)

from app.repositories.gateway_repository import (
    GatewayRepository
)

from app.services.cache_service import CacheService


class ValidationService:

    def __init__(self):
        self.user_repository = UserRepository()
        self.gateway_repository = GatewayRepository()
        self.cache_service = CacheService()
        self.cache_hit = 0
        self.cache_miss = 0

    def validate(
        self,
        user_id,
        pem_id
    ):
        user, hit = self.cache_service.get_user(user_id)

        if hit:
            self.cache_hit += 1

        else:
            self.cache_miss += 1

            user = self.user_repository.find_by_user_id(user_id)

        if user:
            self.cache_service.set_user(user_id, user)


       
       
        if not user:
            return {
                "status": "INVALID_USER",
                "cacheHit": self.cache_hit,
                "cacheMiss": self.cache_miss
            }

        if user["status"] != "ACTIVE":
            return {
                "status": "INACTIVE_USER",
                "cacheHit": self.cache_hit,
                "cacheMiss": self.cache_miss
            }

       

        gateway, hit = self.cache_service.get_gateway(pem_id)

        if hit:
            self.cache_hit += 1

        else:
            self.cache_miss += 1

            gateway = self.gateway_repository.find_by_gateway_id(pem_id)

        if gateway:
            self.cache_service.set_gateway(pem_id, gateway)




        if not gateway:
            return {
                "status": "GATEWAY_NOT_FOUND",
                "cacheHit": self.cache_hit,
                "cacheMiss": self.cache_miss
            }

        if gateway["status"] != "ACTIVE":
            return {
                "status": "GATEWAY_DISABLED",
                "cacheHit": self.cache_hit,
                "cacheMiss": self.cache_miss
            }

        if pem_id not in user["allowedPems"]:
            return {
                "status":
                "UNAUTHORIZED_GATEWAY",
                "cacheHit": self.cache_hit,
                "cacheMiss": self.cache_miss
            }

        return {
            "status": "VALID",
            "cacheHit": self.cache_hit,
            "cacheMiss": self.cache_miss
        }