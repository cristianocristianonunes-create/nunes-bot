#!/usr/bin/env python3
"""Monitor pos-pump DRIFT — registra comportamento do preco apos spike de baleia"""
import os, sys, time, json
from datetime import datetime
from binance.client import Client
from dotenv import load_dotenv
import pandas as pd
import requests

load_dotenv()
sys.stdout.reconfigure(encoding='utf-8')
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

LOG_FILE = "pump_drift_log.json"
preco_pump = None
preco_max_pos_pump = 0
preco_min_pos_pump = 999
registros = []
inicio = time.time()
avisou_devolucao = False
avisou_continuacao = False

print("Monitor pos-pump DRIFT iniciado")
print("Registrando comportamento a cada 30s por 2 horas")
print()

while time.time() - inicio < 7200:  # 2 horas
    try:
        # Preco atual
        ticker = client.futures_symbol_ticker(symbol="DRIFTUSDT")
        preco = float(ticker["price"])

        if preco_pump is None:
            preco_pump = preco
            print(f"Preco referencia (pos-pump): {preco_pump:.6f}")

        if preco > preco_max_pos_pump:
            preco_max_pos_pump = preco
        if preco < preco_min_pos_pump:
            preco_min_pos_pump = preco

        var_desde_pump = ((preco - preco_pump) / preco_pump) * 100

        # Volume 1min
        k1 = client.futures_klines(symbol="DRIFTUSDT", interval="1m", limit=5)
        vol_ultimo = float(k1[-1][7])  # quoteAssetVolume
        vol_penultimo = float(k1[-2][7])

        # MAs
        k5 = client.futures_klines(symbol="DRIFTUSDT", interval="5m", limit=30)
        df5 = pd.DataFrame(k5, columns=['ts','o','h','l','c','v','tc','qv','nt','tbv','tqv','ig'])
        df5['c'] = df5['c'].astype(float)
        df5['ma7'] = df5['c'].rolling(7).mean()
        df5['ma25'] = df5['c'].rolling(25).mean()
        c5 = df5.iloc[-1]
        ma7_5 = c5['ma7']
        ma25_5 = c5['ma25']
        ma_5min_favor_short = ma7_5 < ma25_5
        gap_5 = (ma7_5 - ma25_5) / ma25_5 * 100

        k1h = client.futures_klines(symbol="DRIFTUSDT", interval="1h", limit=30)
        df1h = pd.DataFrame(k1h, columns=['ts','o','h','l','c','v','tc','qv','nt','tbv','tqv','ig'])
        df1h['c'] = df1h['c'].astype(float)
        df1h['ma7'] = df1h['c'].rolling(7).mean()
        df1h['ma25'] = df1h['c'].rolling(25).mean()
        c1h = df1h.iloc[-1]
        ma_1h_favor_short = c1h['ma7'] < c1h['ma25']
        gap_1h = (c1h['ma7'] - c1h['ma25']) / c1h['ma25'] * 100

        # Buy/sell pressure
        trades = client.futures_recent_trades(symbol="DRIFTUSDT", limit=200)
        vol_buy = sum(float(t['quoteQty']) for t in trades if not t['isBuyerMaker'])
        vol_sell = sum(float(t['quoteQty']) for t in trades if t['isBuyerMaker'])
        buy_ratio = vol_buy / (vol_buy + vol_sell) * 100 if (vol_buy + vol_sell) > 0 else 50

        minutos = (time.time() - inicio) / 60
        ts = datetime.now().strftime("%H:%M:%S")

        registro = {
            "ts": ts,
            "min": round(minutos, 1),
            "preco": preco,
            "var_pct": round(var_desde_pump, 2),
            "vol_1min_usd": round(vol_ultimo),
            "buy_pct": round(buy_ratio, 1),
            "ma5_gap": round(gap_5, 3),
            "ma5_favor_short": ma_5min_favor_short,
            "ma1h_gap": round(gap_1h, 3),
            "ma1h_favor_short": ma_1h_favor_short,
        }
        registros.append(registro)

        status_5 = "SHORT" if ma_5min_favor_short else "contra"
        status_1h = "SHORT" if ma_1h_favor_short else "contra"

        print(f"[{ts}] +{minutos:.0f}min | {preco:.5f} ({var_desde_pump:+.2f}%) | vol ${vol_ultimo:,.0f} | buy {buy_ratio:.0f}% | 5m:{status_5}({gap_5:+.2f}%) | 1h:{status_1h}({gap_1h:+.2f}%)")

        # Alertas
        if var_desde_pump <= -3 and not avisou_devolucao:
            telegram(
                f"<b>DRIFT: pump devolvendo!</b>\n"
                f"Preco caiu {var_desde_pump:.1f}% desde o spike\n"
                f"Max pos-pump: {preco_max_pos_pump:.6f}\n"
                f"Agora: {preco:.6f}\n"
                f"5min: {'a favor SHORT' if ma_5min_favor_short else 'contra'}\n"
                f"1h: {'a favor SHORT' if ma_1h_favor_short else 'contra'}"
            )
            avisou_devolucao = True

        if var_desde_pump >= 5 and not avisou_continuacao:
            telegram(
                f"<b>DRIFT: pump continuando!</b>\n"
                f"Preco subiu mais {var_desde_pump:.1f}% apos spike\n"
                f"Cuidado — SHORT sob pressao"
            )
            avisou_continuacao = True

        # Salva log a cada registro
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "pump_preco": preco_pump,
                "pump_time": datetime.now().strftime("%Y-%m-%d"),
                "max_pos_pump": preco_max_pos_pump,
                "min_pos_pump": preco_min_pos_pump,
                "registros": registros
            }, f, indent=2, ensure_ascii=False)

    except Exception as e:
        print(f"Erro: {e}")

    time.sleep(30)

# Resumo final
devolucao = ((preco_min_pos_pump - preco_pump) / preco_pump) * 100
continuacao = ((preco_max_pos_pump - preco_pump) / preco_pump) * 100
print()
print("=" * 50)
print("RESUMO POS-PUMP (2h)")
print(f"Preco pump: {preco_pump:.6f}")
print(f"Max: {preco_max_pos_pump:.6f} ({continuacao:+.2f}%)")
print(f"Min: {preco_min_pos_pump:.6f} ({devolucao:+.2f}%)")
print(f"Final: {preco:.6f} ({var_desde_pump:+.2f}%)")
print(f"Veredicto: {'DEVOLVEU' if var_desde_pump < -1 else 'SEGUROU' if abs(var_desde_pump) <= 1 else 'CONTINUOU SUBINDO'}")

telegram(
    f"<b>DRIFT pump — resumo 2h</b>\n"
    f"Preco pump: {preco_pump:.6f}\n"
    f"Max: {preco_max_pos_pump:.6f} ({continuacao:+.1f}%)\n"
    f"Min: {preco_min_pos_pump:.6f} ({devolucao:+.1f}%)\n"
    f"Final: {preco:.6f} ({var_desde_pump:+.1f}%)\n"
    f"{'DEVOLVEU' if var_desde_pump < -1 else 'SEGUROU' if abs(var_desde_pump) <= 1 else 'CONTINUOU'}"
)
