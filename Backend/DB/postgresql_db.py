import os
import psycopg2
from psycopg2.extras import Json
from datetime import datetime, timezone

class PostregresDB:
    def __init__(self, connection_string=None):

        self.connection_string = connection_string or os.getenv('POSTGRES_URL') or os.getenv('POSTREGRES_URL')
        self.connection = self._connect()
        self.create_table()

    def _connect(self):
        try:
            return psycopg2.connect(self.connection_string)
        except Exception as e:
            print(f'Error to connect to PostgreSQL: {e}')
            raise

    def create_table(self):
        # Criação das tabelas relacionais.
        query = """
        CREATE TABLE IF NOT EXISTS youtube_comments (
            id SERIAL PRIMARY KEY,
            video_id VARCHAR(50) NOT NULL,
            video_title TEXT,
            channel VARCHAR(255),
            author VARCHAR(255),
            comment TEXT,
            sentiment VARCHAR(50),
            trust FLOAT,
            reason TEXT,
            analysis_result JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        with self.connection.cursor() as cursor:
            cursor.execute(query)
            self.connection.commit()

    def save_analysis(self, video_id, channel, video_title, comment_data, analysis_result):
        query = """
        INSERT INTO youtube_comments (video_id, video_title, channel, author, comment, sentiment, trust, reason, created_at, analysis_result)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """

        # Garante que o nível de confiança (trust) seja um número decimal
        try:
            trust_value = float(analysis_result.get("trust", 0.0))
        except (ValueError, TypeError):
            trust_value = 0.0

        values = (
            video_id,
            video_title,
            channel,
            comment_data.get('author', 'N/A'),
            comment_data.get('text', ''),
            analysis_result.get("sentiment", "N/A"),
            trust_value,
            analysis_result.get("reason", "N/A"),
            datetime.now(timezone.utc),
            Json(analysis_result)
        )

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, values)
                insert_id = cursor.fetchone()[0]
            self.connection.commit()
            return insert_id
        except Exception as e:
            print(f'Error to save on PostgreSQL: {e}')
            self.connection.rollback() # Desfaz a transação em caso de erro.
            return None

    def close(self):
        if self.connection:
            self.connection.close()
