import os
from pymongo import MongoClient
from datetime import datetime, timezone

MONGO_URI = os.getenv("MONGO_URI")

class MongoDB:
    def __init__(self, connection_string = os.getenv("MONGO_URI"), db_name = "Opi"):
        self.client = MongoClient(connection_string)
        self.db = self.client[db_name]
        self.collection = self.db["youtube_comments"]

    def save_analysis(self, video_id, channel, video_title,comment_data, analysis_result):
        # Armazena nas coleções o comentário original junto com análise de IA
        document = {
            "video_id": video_id,
            "channel": channel,
            "video_title": video_title,
            "author": comment_data["author"],
            "comment": comment_data["text"],
            "analysis_result": analysis_result,
            "created_at": datetime.now(timezone.utc),
        }

        try:
            result = self.collection.insert_one(document)
            return result.inserted_id
        except Exception as e:
            print(f"Error to save on MongoDB: {e}")
            return None

    def close(self):
        if self.client:
            self.client.close()

