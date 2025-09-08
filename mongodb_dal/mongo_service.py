from pymongo.errors import PyMongoError
from mongodb_dal.mongo_connection import ConnectionDB


class MongoService:

    def __init__(self, db_name, collection_name):
        self.conn = ConnectionDB(db_name, collection_name)

    # ---------- CREATE ----------
    def insert_one(self, data):
        try:
            result = self.conn.collection.insert_one(data)
            return str(result)
        except PyMongoError as e:
            raise RuntimeError(f"Insert failed: {e}")