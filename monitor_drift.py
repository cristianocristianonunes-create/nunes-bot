#!/usr/bin/env python3
"""Monitor DRIFTUSDT SHORT — avisa quando MA7 cruzar MA25 no 5min"""
import os
import time
import requests
import pandas as pd
from binance.client import Client
from dotenv import load_dotenv

load_dotenv()
client = Client(os.getenv("BINANCE_API_KEY"), os.getenv("BINANCE_API_SECRET"))
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def telegram(msg):
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"},
                timeout=10,
            )
        except Exception:
            pass

def check():
    k5 = client.futures_klines(symbol="DRIFTUSDT", interval="5m", limit=30)
    df = pd.DataFrame(k5, columns=['ts','o','h','l','c','v','tc','qv','nt','tbv','tqv','ig'])
    df['c'] = df['c'].astype(float)
    df['ma7'] = df['c'].rolling(7).mean()
    df['ma25'] = df['c'].rolling(25).mean()

    cur = df.iloc[-1]
    prev = df.iloc[-2]
    preco = cur['c']
    ma7 = cur['ma7']
    ma25 = cur['ma25']
    gap = (ma7 - ma25) / ma25 * 100

    ma7_p = prev['ma7']
    ma25_p = prev['ma25']
    gap_prev = (ma7_p - ma25_p) / ma25_p * 100

    # SHORT: cruzamento = MA7 era >= MA25, agora MA7 < MA25
    cruzou = ma7_p >= ma25_p and ma7 < ma25

    return preco, ma7, ma25, gap, gap_prev, cruzou

print("Monitor DRIFT SHORT iniciado. Checando a cada 30s...")
print("Esperando MA7 cruzar abaixo de MA25 no 5min.")
print()

ja_avisou = False

while True:
    try:
        preco, ma7, ma25, gap, gap_prev, cruzou = check()
        conv = "CONVERGINDO" if abs(gap) < abs(gap_prev) else "divergindo"
        status = "A FAVOR" if ma7 < ma25 else "CONTRA"

        ts = time.strftime("%H:%M:%S")
        print(f"[{ts}] DRIFT {preco:.6f} | MA7={ma7:.6f} MA25={ma25:.6f} | gap {gap:+.3f}% {conv} | {status}")

        if cruzou and not ja_avisou:
            print()
            print("=" * 50)
            print("  !!! MA7 CRUZOU ABAIXO DE MA25 — SINAL DE 3x !!!")
            print("=" * 50)
            print()
            telegram(
                "<b>DRIFT SHORT — MA7 CRUZOU MA25!</b>\n"
                f"Preco: {preco:.6f}\n"
                f"MA7: {ma7:.6f} < MA25: {ma25:.6f}\n"
                f"Gap: {gap:+.3f}%\n\n"
                "O bot deve disparar o 3x automaticamente se score >= 50.\n"
                "Monitore o Telegram do Nunes."
            )
            ja_avisou = True

        if ja_avisou and ma7 >= ma25:
            # Resetou — pode cruzar de novo
            ja_avisou = False
            print(f"[{ts}] Cruzamento perdido, resetando monitor...")

    except Exception as e:
        print(f"Erro: {e}")

    time.sleep(30)
