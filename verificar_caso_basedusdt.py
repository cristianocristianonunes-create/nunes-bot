#!/usr/bin/env python3
"""
Verifica o caso de estudo BASEDUSDT: o que aconteceu após o momento
em que o usuário cogitou 3x mas esperou.

Roda periodicamente para avaliar:
- Se o preço caiu (SHORT teria lucrado → 3x teria dado certo)
- Se o preço subiu (SHORT piorou → foi certo esperar)
"""
import json
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from binance.client import Client

load_dotenv()
client = Client(os.getenv("BINANCE_API_KEY"), os.getenv("BINANCE_API_SECRET"))

# Carrega o caso
with open("aprendizados.json", "r", encoding="utf-8") as f:
    dados = json.load(f)

caso = None
for d in dados:
    if d.get("tipo") == "caso_estudo_3x_nao_feito" and d.get("symbol") == "BASEDUSDT":
        caso = d
        break

if not caso:
    print("Caso BASEDUSDT nao encontrado")
    exit()

preco_momento = caso["preco"]
direcao = caso["direcao"]
timestamp = caso["timestamp"]

# Preco atual
ticker = client.futures_symbol_ticker(symbol="BASEDUSDT")
preco_atual = float(ticker["price"])

# Movimento desde o momento
if direcao == "SHORT":
    mov_pct = (preco_momento - preco_atual) / preco_momento * 100
else:
    mov_pct = (preco_atual - preco_momento) / preco_momento * 100

# Delta de tempo
dt_inicio = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
delta = datetime.now(timezone.utc) - dt_inicio
horas = delta.total_seconds() / 3600

print("=" * 70)
print(f"CASO DE ESTUDO BASEDUSDT {direcao}")
print("=" * 70)
print(f"Momento: {timestamp}")
print(f"Ha: {horas:.1f} horas")
print(f"Preco no momento: {preco_momento}")
print(f"Preco agora:      {preco_atual}")
print(f"Movimento a favor do {direcao}: {mov_pct:+.2f}%")
print()

if mov_pct > 0.5:
    print("VEREDICTO PARCIAL: 3x TERIA DADO CERTO")
    print(f"  Preco se moveu {mov_pct:.2f}% a favor do {direcao}")
    print(f"  Score alto + instinto estavam certos")
elif mov_pct < -0.5:
    print("VEREDICTO PARCIAL: 3x TERIA DADO ERRADO")
    print(f"  Preco foi {abs(mov_pct):.2f}% CONTRA o {direcao}")
    print(f"  Esperar foi a decisao certa — sistema de continuidade funcionou")
else:
    print("VEREDICTO: INDEFINIDO (preco lateralizando)")
    print(f"  Movimento de apenas {mov_pct:.2f}% — ainda nao deu para avaliar")

print()
print("Rode esse script novamente em algumas horas para atualizar.")
