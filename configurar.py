# -*- coding: utf-8 -*-
"""
Configurador do Nunes — preenche o .env interativamente.
Execute uma vez antes de rodar o nunes.py.
"""

import os

ENV_FILE = os.path.join(os.path.dirname(__file__), ".env")

def perguntar(texto, padrao=None):
    if padrao:
        resposta = input(f"{texto} [{padrao}]: ").strip()
        return resposta if resposta else padrao
    else:
        while True:
            resposta = input(f"{texto}: ").strip()
            if resposta:
                return resposta
            print("  Campo obrigatorio. Tente novamente.")

print("=" * 50)
print("       CONFIGURADOR DO NUNES")
print("=" * 50)
print()

print("--- Binance ---")
api_key    = perguntar("API Key da Binance")
api_secret = perguntar("API Secret da Binance")

print()
print("--- Telegram ---")
print("(Deixe em branco para desativar alertas)")
telegram_token   = input("Token do Telegram: ").strip()
telegram_chat_id = input("Chat ID do Telegram: ").strip()

print()
print("--- Configuracoes do Bot ---")
modo        = perguntar("Modo (real/simulacao)", "real")
alavancagem = perguntar("Alavancagem (10/20)", "20")
risco       = perguntar("Risco por trade em % (ex: 0.01 = 1%)", "0.01")
meta_ciclo  = perguntar("Meta de lucro por ciclo em %", "5.0")
perda_diaria = perguntar("Limite de perda diaria em %", "5.0")

conteudo = f"""# Configuracao do Nunes
BINANCE_API_KEY={api_key}
BINANCE_API_SECRET={api_secret}

# Telegram (deixe vazio para desativar)
TELEGRAM_TOKEN={telegram_token}
TELEGRAM_CHAT_ID={telegram_chat_id}

# Configuracoes do bot
MODO={modo}
ALAVANCAGEM={alavancagem}
RISCO_POR_TRADE={risco}
RACIO_MARGEM_MAX=8.0
LIMITE_PERDA_DIARIA={perda_diaria}
META_CICLO_PCT={meta_ciclo}
META_CICLO_FASE2_USD=50.0
META_CICLO_FASE2_MIN=1000.0
"""

with open(ENV_FILE, "w", encoding="utf-8") as f:
    f.write(conteudo)

print()
print("=" * 50)
print("  Configuracao salva em .env com sucesso!")
print("  Para iniciar o bot: python nunes.py")
print("=" * 50)
