import json

from google import genai

class OpiAnalyser:
    def __init__(self, api_key):

        if not api_key:
            raise ValueError(
                "GEMINI_API_KEY não encontrada. Defina a variável de ambiente. "
                "ou passe api_key ao inicializar OpiAnalyser."
            )

        self.client = genai.Client(api_key=api_key)

# Função para analisar agrupamento de sentimentos para não atingir limite da IA
    def analyse_sentiment(self, comments_list):
        # Envia a lista de comentários para o Gemini
        formatted_comments = "\n".join(str(i) for i in enumerate(comments_list))

        prompt = f"""
        Analise o sentimento dos seguintes comentários de redes sociais.
        Retorne um array JSON contendo exatamente {len(comments_list)} objetos, na mesma ordem dos IDs fornecidos.
        
        Comentários:
        {formatted_comments}
        
        Esquema de cada objeto no array:
        {{"sentiment": "positivo|negativo|neutro", "trust": "0.0-1.0", "reason": "breve explicação"}}
        """

        try:
            response = self.client.models.generate_content(
                model="gemini-3-flash-preview",
                contents=prompt,
                config={
                    "system_instruction": "Você é um especialista em análise de sentimentos. Responda apenas com o array JSON.",
                    "response_mime_type": "application/json"
                }
            )
            return json.loads(response.text)
        except Exception as e:
            print(f'Error to process the list: {e}')
            return f"Error analyzing sentiment with Gemini API: {e and len(comments_list)}"
