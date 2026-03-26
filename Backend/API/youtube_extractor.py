
from googleapiclient.discovery import build

class YoutubeExtractor:
    def __init__(self, api_key):
        self.youtube = build('youtube', 'v3', developerKey=api_key)

    def search_videos(self, query, max_results=5):
        #Procura vídeos no YouTube com base em uma string de busca.
        try:
            request = self.youtube.search().list(
                q=query,
                part="snippet",
                type="video",
                maxResults=max_results
            )
            response = request.execute()

            videos = []
            for item in response.get("items", []):
                videos.append({
                    "video_id": item["id"]["videoId"],
                    "title": item["snippet"]["title"],
                    "channel": item["snippet"]["channelTitle"],
                    "published_at": item["snippet"]["publishedAt"]
                })
            return videos

        except Exception as e:
            print(f"Error on search: {e}")

            return []

    # Função de listar comentários
    def get_comments(self ,video_id, max_results_per_page=100, max_pages=3):
        """
        Extrai comentários de um vídeo específico com suporte a paginação.
        max_results_per_page= limite de API por requisiçãO (100).
        max_pages= Limite de páginas de busca (3).
        """
        comments = []
        next_page_token = None
        pages_fetched = 0

        try:
            while pages_fetched < max_pages:
                request = self.youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=max_results_per_page,
                    textFormat="plainText",
                    pageToken=next_page_token  # Envia o token da página atual
                )
                response = request.execute()

                for item in response.get('items', []):
                    text = item['snippet']['topLevelComment']['snippet']['textDisplay']
                    author = item['snippet']['topLevelComment']['snippet']['authorDisplayName']
                    comments.append({
                        'author': author,
                        'text': text
                    })

                # Atualiza o token para a próxima página
                next_page_token = response.get('nextPageToken')
                pages_fetched += 1

                # Se a API não retornar um next_page_token, acabaram os comentários
                if not next_page_token:
                    break

            return comments  # O return deve permanecer no laço do while

        except Exception as e:
            print(f"Error to collect comments: {e}")

            return []
