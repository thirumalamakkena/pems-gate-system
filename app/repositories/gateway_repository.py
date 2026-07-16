from app.config.database import db

class GatewayRepository:

    def __init__(self):
        self.collection = db["pems_gateways"]

    def find_by_gateway_id(
        self,
        gateway_id
    ):
        return self.collection.find_one(
            {"gatewayId": gateway_id}
        )

    def get_active_gateways(self):
        return list(
            self.collection.find(
                {"status": "ACTIVE"}
            )
        )