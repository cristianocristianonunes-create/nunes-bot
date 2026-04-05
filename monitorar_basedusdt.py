#!/usr/bin/env python3
"""
Monitor continuo do caso BASEDUSDT.
Compara o momento inicial (quando usuario cogitou 3x) com a evolucao do preco.
Roda 1 vez e mostra o status. Pode ser executado quantas vezes quiser.
"""
import json
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from binance.client import Client

load_dotenv()
client = Client(os.getenv("BINANCE_API_KEY"), os.getenv("BINANCE_API_SECRET"))

# Carrega os checkpoints do BASEDUSDT
with open("aprendizados.json", "r", encoding="utf-8") as f:
    dados = json.load(f)

checkpoints = [d for d in dados if d.get("symbol") == "BASEDUSDT" and "caso_estudo" in d.get("tipo", "")]

if not checkpoints:
    print("Nenhum caso BASEDUSDT encontrado")
    exit()

# Preco atual
ticker = client.futures_symbol_ticker(symbol="BASEDUSDT")
preco_atual = float(ticker["price"])

print("=" * 75)
print("MONITOR BASEDUSDT — CASO DE ESTUDO")
print("=" * 75)
print(f"Preco ATUAL: {preco_atual:.6f}")
print(f"Momento:     {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
print()
print("Historico de checkpoints:")
print("-" * 75)

for i, cp in enumerate(checkpoints, 1):
    dt = datetime.fromisoformat(cp["timestamp"].replace("Z", "+00:00"))
    delta = datetime.now(timezone.utc) - dt
    horas = delta.total_seconds() / 3600
    preco_cp = cp["preco"]
    direcao = cp["direcao"]

    # Movimento desde este checkpoint (a favor do SHORT)
    if direcao == "SHORT":
        mov_pct = (preco_cp - preco_atual) / preco_cp * 100
    else:
        mov_pct = (preco_atual - preco_cp) / preco_cp * 100

    status = ""
    if mov_pct > 1.0:
        status = "++ 3x teria sido otimo"
    elif mov_pct > 0.3:
        status = "+  3x teria lucrado"
    elif mov_pct > -0.3:
        status = "=  neutro"
    elif mov_pct > -1.0:
        status = "-  3x teria prejuizo leve"
    else:
        status = "-- 3x teria falhado"

    print(f"  CP{i} | {cp['timestamp'][11:19]} UTC | {horas:.1f}h atras | Preco: {preco_cp:.6f} | mov: {mov_pct:+.2f}% {status}")

print()
print("Leitura do usuario:")
print("-" * 75)
# Pega a ultima leitura registrada
ultimo = checkpoints[-1]
if "leitura_usuario_v2" in ultimo:
    print(f"  {ultimo['leitura_usuario_v2']}")
elif "leitura_usuario" in ultimo:
    print(f"  {ultimo['leitura_usuario']}")

print()
print("Decisao conjunta: NAO FAZER 3x")
print("Hipotese em teste: MA99 subindo + MA25 lateral + MA7 desacelerando = suporte")

# Veredicto geral
cp_original = checkpoints[0]
mov_total = (cp_original["preco"] - preco_atual) / cp_original["preco"] * 100  # a favor do SHORT
print()
print("=" * 75)
if mov_total > 0.5:
    print(f"VEREDICTO ATUAL: SHORT estaria lucrando (+{mov_total:.2f}%)")
    print("  O instinto de entrar estava correto — perdeu-se oportunidade")
elif mov_total < -0.5:
    print(f"VEREDICTO ATUAL: SHORT estaria perdendo ({mov_total:.2f}%)")
    print("  Decisao de nao entrar foi correta — padrao de suporte confirmou")
else:
    print(f"VEREDICTO ATUAL: INDEFINIDO (movimento de apenas {mov_total:+.2f}%)")
    print("  Preco lateralizando, aguardar mais tempo")

print("=" * 75)
