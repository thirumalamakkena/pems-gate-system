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
        self.user_cache_hit= False
        self.gateway_cache_hit = False


    def validate(
        self,
        user_id,
        pem_id
    ):
        user, hit = self.cache_service.get_user(user_id)

        if hit:
            self.user_cache_hit = True

        else:

            user = self.user_repository.find_by_user_id(user_id)

        if user:
            self.cache_service.set_user(user_id, user)


       
       
        if not user:
            return {
                "status": "INVALID_USER"
            }

        if user["status"] != "ACTIVE":
            return {
                "status": "INACTIVE_USER"
            }

       

        gateway, hit = self.cache_service.get_gateway(pem_id)

        if hit:
            self.gateway_cache_hit = True

        else:

            gateway = self.gateway_repository.find_by_gateway_id(pem_id)

        if gateway:
            self.cache_service.set_gateway(pem_id, gateway)




        if not gateway:
            return {
                "status": "GATEWAY_NOT_FOUND"
            }

        if gateway["status"] != "ACTIVE":
            return {
                "status": "GATEWAY_DISABLED"
            }

        if pem_id not in user["allowedPems"]:
            return {
                "status":
                "UNAUTHORIZED_GATEWAY"
            }

        return {
            "status": "VALID"
        }
    
    def get_cache_hits(self):
        cache_hits = {
            "user":{
                "userCacheHit" : self.user_cache_hit
            },
            "gateway":{
                "gatewayCacheHit" : self.gateway_cache_hit
            }
        }

        self.user_cache_hit = self.gateway_cache_hit = False

        return cache_hits