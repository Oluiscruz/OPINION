import os
import random
import time
import pandas as pd
from API.ia_analyser import OpiAnalyser
from API.youtube_extractor import YoutubeExtractor
from DB.postgresql_db import PostregresDB
from DB.mongodb import MongoDB
from dotenv import load_dotenv

# Chaves API
load_dotenv()
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
POSTREGRES_URL = os.getenv('POSTREGRES_URL')
MONGO_URI = os.getenv('MONGO_URI')

# Função para dividir a lista de comments em pequenos pedaços
def chunck_list(data, size):
    for i in range (0, len(data), size):
        yield data[i:i+size]

# Função principal
def main():
    extractor = YoutubeExtractor(YOUTUBE_API_KEY)
    analyser = OpiAnalyser(GEMINI_API_KEY)
    db = PostregresDB(POSTREGRES_URL)
    mongo_db = MongoDB(MONGO_URI)

    # 1. Pesquisar vídeos
    theme = input(f'Insert a theme of search: ')
    videos_found = extractor.search_videos(theme, max_results=5)

    if not videos_found:
        print("\nNo videos found")
        return

    # 2. Visualização dos dados com Pandas
    df_videos = pd.DataFrame(videos_found)
    print(f'\n--- Videos Found ---')
    print(df_videos[['title', 'channel', 'video_id']])

    # 3. Seleção de vídeo
    selected_video_id = videos_found[0]['video_id']
    selected_channel = videos_found[0]['channel']
    print(f'\nAnalyzing the first video: {videos_found[0]["title"]} ({selected_video_id})')

    # 4. Extração e análise com paginação
    comments = extractor.get_comments(selected_video_id, max_results_per_page=100, max_pages=3)
    if not comments:
        print("\nNo comments found or error in extraction")
        return

    # DataFrame para os comentários também
    df_comments = pd.DataFrame(comments)
    batch_size = 10
    results = []

    # 5. Análise de Sentimento e Persistência
    print(f"Starting analysis of {len(comments)} comments...")

    # Divide os comentários em blocos
    for batch in chunck_list(comments, batch_size):
        texts_to_analyse = [str(c) for c in batch]

        max_retries = 5 # Máximo de tentativas
        base_delay = 2 # Tempo em segundos
        batch_results = None

        """
        O laço a seguir estabele um Timeout para caso haja erros de Rate Limit.
        Muitos Lotes com comentários grandes serão enviados para a IA analisar, por isso, 
        um tempo entre cada lote é importante para a API respirar.
        """
        for attempt in range(max_retries):
            try:
                # Chamada única da API para multiplos comentários
                batch_results = analyser.analyse_sentiment(texts_to_analyse)
                # Caso retorne sucesso, sai do loop de retries
                break

            except Exception as e:
                error_text = str(e).lower()
                is_rate_limit = any(token in error_text for token in [
                    "429", "rate", "quota", "resource_exhausted", "too many requests"
                ])

                # Se não for rate limit, não faz retry infinito
                if not is_rate_limit:
                    print(f"Error not related to rate limit: {e}")
                    break

                # se chegou aqui, rsultou em rate limit.
                if attempt < max_retries - 1:
                        wait_time = (base_delay * (2 ** attempt)) + random.uniform(0, 1)
                        print(f'Rate limit detected, Attempt {attempt + 1}/{max_retries} in {wait_time:.1f}s...')
                        time.sleep(wait_time)
                else:
                    print(f"Fails after: {max_retries} attempts on batch: {e}")

        # Se não conseguiu resultado válido, pula esse batch
        if not batch_results:
            print(f"Skipping batch com {len(batch)} comentários por falha na análise.")
            continue
        # Evita desalinhamento entre comentários e respostas da IA
        if len(batch_results) != len(batch):
            print(f"Warning: batch with {len(batch)} comments returned {len(batch_results)} analyses. Skipping this batch.")
            continue

        # Itera sobre o lote original e o resultado da IA simultâneamente
        for comment_data, analysis in zip(batch, batch_results):
            try:
                # Salva no DB
                db.save_analysis(
                    selected_video_id,
                    selected_channel,
                    videos_found[0]["title"],
                    comment_data,
                    analysis
                )
                results.append(analysis)
            except Exception as e:
                print(f"Error on saving analysis on DB: {e}")
                continue

    print(f"Processed {len(results)} from {len(comments)}...")

    # 6. Exibir resumo final com pandas
    df_results = pd.DataFrame(results)
    df_final = pd.concat(
        [
            df_comments.reset_index(drop=True),
            df_results.reset_index(drop=True)
        ],
        axis=1)

    print(f'\n--- Summary of Sentiment Analysis (Top 10) --- ')
    print(df_final[['author', 'sentiment', 'trust']].head(10))

    # Encerra conexão com banco ao finalizar
    db.close()
    mongo_db.close()

# Executa
if __name__ == "__main__":
    main()
