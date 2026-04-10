#!/usr/bin/env python3
"""Monitor DRIFT — avisa quando cenario ficar favoravel pra all-in SHORT"""
import os, sys, time, requests
from binance.client import Client
from dotenv import load_dotenv
import pandas as pd

load_dotenv()
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)
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
    preco = float(client.futures_symbol_ticker(symbol="DRIFTUSDT")["price"])

    resultados = {}

    for tf, interval, limit in [("1min", "1m", 30), ("3min", "3m", 30), ("5min", "5m", 30), ("15min", "15m", 30), ("1h", "1h", 30)]:
        k = client.futures_klines(symbol="DRIFTUSDT", interval=interval, limit=limit)
        df = pd.DataFrame(k, columns=['ts','o','h','l','c','v','tc','qv','nt','tbv','tqv','ig'])
        df['c'] = df['c'].astype(float)
        df['v'] = df['v'].astype(float)
        df['ma7'] = df['c'].rolling(7).mean()
        df['ma25'] = df['c'].rolling(25).mean()
        c = df.iloc[-1]
        cp = df.iloc[-2]

        ma7 = c['ma7']
        ma25 = c['ma25']
        favor = ma7 < ma25  # SHORT quer ma7 < ma25
        gap = (ma7 - ma25) / ma25 * 100
        gap_prev = (cp['ma7'] - cp['ma25']) / cp['ma25'] * 100
        convergindo = abs(gap) < abs(gap_prev)

        resultados[tf] = {
            "favor": favor,
            "gap": gap,
            "convergindo": convergindo,
        }

    # RSI 5min
    k5 = client.futures_klines(symbol="DRIFTUSDT", interval="5m", limit=20)
    df5 = pd.DataFrame(k5, columns=['ts','o','h','l','c','v','tc','qv','nt','tbv','tqv','ig'])
    df5['c'] = df5['c'].astype(float)
    delta = df5['c'].diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / loss
    rsi = float((100 - (100 / (1 + rs))).iloc[-1])

    return preco, resultados, rsi

print("Monitor DRIFT all-in iniciado. Checando a cada 60s.")
print("Procurando: 5min cruzar + RSI alto + 1min a favor")
print()

avisou_alerta = False
avisou_go = False
inicio = time.time()

while time.time() - inicio < 14400:  # 4 horas
    try:
        preco, r, rsi = check()
        ts = time.strftime("%H:%M:%S")

        # Contar sinais
        favor_count = sum(1 for tf in r.values() if tf["favor"])
        conv_count = sum(1 for tf in r.values() if tf["convergindo"])

        # Status compacto
        status = []
        for tf in ["1min", "3min", "5min", "15min", "1h"]:
            s = "v" if r[tf]["favor"] else "x"
            c = "^" if r[tf]["convergindo"] else ""
            status.append(f"{tf}:{s}{c}")

        linha = f"[{ts}] {preco:.5f} | RSI:{rsi:.0f} | {' | '.join(status)} | {favor_count}/5 favor"
        print(linha)

        # ALERTA: 5min cruzou a favor
        if r["5min"]["favor"] and not avisou_alerta:
            telegram(
                f"<b>DRIFT: 5min CRUZOU a favor SHORT!</b>\n"
                f"Preco: {preco:.5f}\n"
                f"RSI: {rsi:.0f}\n"
                f"1min: {'a favor' if r['1min']['favor'] else 'contra'}\n"
                f"3min: {'a favor' if r['3min']['favor'] else 'contra'}\n"
                f"15min: {'a favor' if r['15min']['favor'] else 'contra'}\n"
                f"1h: {'a favor' if r['1h']['favor'] else 'contra'}\n\n"
                f"5min cruzou — monitorando se sustenta."
            )
            avisou_alerta = True
            print(f"  >>> ALERTA: 5min cruzou!")

        # Se 5min perdeu o cruzamento, reseta
        if not r["5min"]["favor"] and avisou_alerta:
            avisou_alerta = False
            avisou_go = False

        # GO: 5min + 3min + 1min a favor + RSI > 65
        if (r["5min"]["favor"] and r["3min"]["favor"] and r["1min"]["favor"]
                and rsi > 65 and not avisou_go):
            telegram(
                f"<b>DRIFT: MOMENTO FAVORAVEL!</b>\n"
                f"Preco: {preco:.5f}\n"
                f"RSI: {rsi:.0f} (sobrecomprado)\n"
                f"1min + 3min + 5min TODOS a favor SHORT\n"
                f"1h: {'a favor' if r['1h']['favor'] else 'contra ({:.1f}%)'.format(r['1h']['gap'])}\n\n"
                f"Se quiser all-in, este eh o melhor momento micro.\n"
                f"Risco: 1h ainda {'contra' if not r['1h']['favor'] else 'a favor'}."
            )
            avisou_go = True
            print(f"  >>> GO! 1min+3min+5min alinhados + RSI {rsi:.0f}")

        # SUPER GO: tudo alinhado incluindo 1h
        if (r["5min"]["favor"] and r["1h"]["favor"] and rsi > 60):
            telegram(
                f"<b>DRIFT: TODOS ALINHADOS!</b>\n"
                f"Preco: {preco:.5f}\n"
                f"RSI: {rsi:.0f}\n"
                f"1min + 3min + 5min + 1h TODOS a favor SHORT\n\n"
                f"Melhor cenario possivel para 3x.\n"
                f"Bot deve disparar automaticamente (score + 1h ok)."
            )
            print(f"  >>> SUPER GO! Todos alinhados!")
            # Nao precisa mais monitorar
            break

    except Exception as e:
        print(f"Erro: {e}")

    time.sleep(60)

print("Monitor encerrado.")
