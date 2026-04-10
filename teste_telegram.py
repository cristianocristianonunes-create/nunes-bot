from dotenv import load_dotenv
import os, requests

load_dotenv('C:/robo-trade/.env')

token = os.getenv('TELEGRAM_TOKEN')
chat_id = os.getenv('TELEGRAM_CHAT_ID')

print('Token:', token[:20] + '...' if token else 'VAZIO')
print('Chat ID:', chat_id if chat_id else 'VAZIO')

if not token or not chat_id:
    print('ERRO: Token ou Chat ID nao configurado no .env')
else:
    r = requests.post(
        f'https://api.telegram.org/bot{token}/sendMessage',
        json={'chat_id': chat_id, 'text': 'Teste do robo!'}
    )
    print('Status:', r.status_code)
    print('Resposta:', r.text)
