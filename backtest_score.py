#!/usr/bin/env python3
"""
Backtest do sistema de score 3x nos 15 DCAs manuais historicos.
Reconstroi o candle exato no momento do 3x manual e calcula o score.
"""
import os
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
from binance.client import Client

load_dotenv()
client = Client(os.getenv("BINANCE_API_KEY"), os.getenv("BINANCE_API_SECRET"))

# Lista dos 3x manuais (timestamp BRT, symbol, direcao inferida)
DCAS_MANUAIS = [
    ("2026-04-04 12:53:16", "SIGNUSDT",  "LONG"),
    ("2026-04-04 15:27:38", "ARIAUSDT",  "LONG"),
    ("2026-04-04 15:27:42", "SOLVUSDT",  "LONG"),
    ("2026-04-04 15:27:47", "NEARUSDT",  "LONG"),
    ("2026-04-04 15:27:51", "OPNUSDT",   "SHORT"),
    ("2026-04-04 16:42:29", "SOLVUSDT",  "LONG"),
    ("2026-04-04 17:21:23", "SOLVUSDT",  "LONG"),
    ("2026-04-04 17:59:35", "ONDOUSDT",  "SHORT"),
    ("2026-04-04 17:59:46", "KITEUSDT",  "LONG"),
    ("2026-04-04 18:18:23", "NEARUSDT",  "LONG"),
    ("2026-04-04 23:33:23", "RENDERUSDT","SHORT"),
    ("2026-04-05 16:52:36", "ARIAUSDT",  "LONG"),
]


def calcular_volume_profile(df, n_buckets=24):
    if len(df) < 10:
        return {"valid": False}
    pmin, pmax = df["low"].min(), df["high"].max()
    if pmax <= pmin:
        return {"valid": False}
    bucket = (pmax - pmin) / n_buckets
    buckets = [0.0] * n_buckets
    for _, r in df.iterrows():
        lo, hi, v = r["low"], r["high"], r["volume"]
        if hi <= lo:
            continue
        i_lo = max(0, int((lo - pmin) / bucket))
        i_hi = min(n_buckets - 1, int((hi - pmin) / bucket))
        n = i_hi - i_lo + 1
        if n > 0:
            vp = v / n
            for i in range(i_lo, i_hi + 1):
                buckets[i] += vp
    poc = buckets.index(max(buckets))
    total = sum(buckets)
    alvo = total * 0.70
    acum = buckets[poc]
    lo_i = hi_i = poc
    while acum < alvo and (lo_i > 0 or hi_i < n_buckets - 1):
        vl = buckets[lo_i - 1] if lo_i > 0 else 0
        vh = buckets[hi_i + 1] if hi_i < n_buckets - 1 else 0
        if vl >= vh and lo_i > 0:
            lo_i -= 1
            acum += vl
        elif hi_i < n_buckets - 1:
            hi_i += 1
            acum += vh
        else:
            break
    return {
        "poc": pmin + (poc + 0.5) * bucket,
        "val": pmin + lo_i * bucket,
        "vah": pmin + (hi_i + 1) * bucket,
        "valid": True,
    }


def get_candles_historicos(symbol, interval, dt_target, limit=60):
    """Pega candles ate o momento exato do 3x manual."""
    # Converte BRT (UTC-3) para UTC
    dt_brt = datetime.strptime(dt_target, "%Y-%m-%d %H:%M:%S")
    end_ms = int(dt_brt.timestamp() * 1000) + 3 * 3600 * 1000  # BRT -> UTC

    klines = client.futures_klines(symbol=symbol, interval=interval, endTime=end_ms, limit=limit)
    df = pd.DataFrame(klines, columns=[
        "time", "open", "high", "low", "close", "volume",
        "ct", "qav", "trades", "tbav", "tqav", "ignore"
    ])
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = df[c].astype(float)
    return df


def score_historico(symbol, direcao, dt_target):
    """Calcula o score 3x no momento historico exato."""
    detalhes = {}
    score = 0
    try:
        df5 = get_candles_historicos(symbol, "5m", dt_target, limit=60)
        df5["ma7"] = df5["close"].rolling(7).mean()
        df5["ma25"] = df5["close"].rolling(25).mean()
        df5["vol_media"] = df5["volume"].rolling(20).mean()

        c3 = df5.iloc[-3]
        c2 = df5.iloc[-2]
        c1 = df5.iloc[-1]

        # 1. CANDLE 1 CRUZOU (20)
        if direcao == "LONG":
            cruzou = c3["ma7"] <= c3["ma25"] and c2["ma7"] > c2["ma25"]
        else:
            cruzou = c3["ma7"] >= c3["ma25"] and c2["ma7"] < c2["ma25"]
        if cruzou:
            score += 20
            detalhes["c1_cruzou"] = "SIM (20)"
        else:
            if direcao == "LONG" and c1["ma7"] > c1["ma25"]:
                score += 15
                detalhes["c1_cruzou"] = "recente (15)"
            elif direcao == "SHORT" and c1["ma7"] < c1["ma25"]:
                score += 15
                detalhes["c1_cruzou"] = "recente (15)"
            else:
                detalhes["c1_cruzou"] = "NAO (0)"

        # 2. CANDLE 2 SEPARANDO (20)
        if direcao == "LONG":
            sep2 = c2["ma7"] - c2["ma25"]
            sep1 = c1["ma7"] - c1["ma25"]
        else:
            sep2 = c2["ma25"] - c2["ma7"]
            sep1 = c1["ma25"] - c1["ma7"]
        if sep1 > sep2:
            score += 20
            detalhes["c2_separando"] = "SIM (20)"
        else:
            detalhes["c2_separando"] = "NAO (0)"

        # 3. CORPO FORTE (15)
        rng = c1["high"] - c1["low"]
        corpo = abs(c1["close"] - c1["open"])
        corpo_pct = (corpo / rng * 100) if rng > 0 else 0
        if corpo_pct >= 60:
            score += 15
            detalhes["corpo"] = f"forte {corpo_pct:.0f}% (15)"
        elif corpo_pct >= 40:
            score += 8
            detalhes["corpo"] = f"medio {corpo_pct:.0f}% (8)"
        else:
            detalhes["corpo"] = f"fraco {corpo_pct:.0f}% (0)"

        # 4. COR DO CANDLE (10)
        verde = c1["close"] > c1["open"]
        verm = c1["close"] < c1["open"]
        if direcao == "LONG" and verde:
            score += 10
            detalhes["cor"] = "verde (10)"
        elif direcao == "SHORT" and verm:
            score += 10
            detalhes["cor"] = "vermelho (10)"
        else:
            detalhes["cor"] = "contra (0)"

        # 5. 1MIN ALINHADO (10 + 5)
        df1 = get_candles_historicos(symbol, "1m", dt_target, limit=30)
        df1["ma7"] = df1["close"].rolling(7).mean()
        df1["ma25"] = df1["close"].rolling(25).mean()
        m1 = df1.iloc[-1]
        m2 = df1.iloc[-2]
        if direcao == "LONG":
            alin = m1["ma7"] > m1["ma25"]
            acel = (m1["ma7"] - m1["ma25"]) > (m2["ma7"] - m2["ma25"])
        else:
            alin = m1["ma7"] < m1["ma25"]
            acel = (m1["ma25"] - m1["ma7"]) > (m2["ma25"] - m2["ma7"])
        if alin:
            score += 10
            if acel:
                score += 5
                detalhes["1min"] = "alinhado+acel (15)"
            else:
                detalhes["1min"] = "alinhado (10)"
        else:
            detalhes["1min"] = "contra (0)"

        # 6. FIBONACCI (10)
        hi20 = df5["high"].iloc[-20:].max()
        lo20 = df5["low"].iloc[-20:].min()
        preco = df5["close"].iloc[-1]
        fib_382 = lo20 + (hi20 - lo20) * 0.382
        fib_618 = lo20 + (hi20 - lo20) * 0.618
        if direcao == "LONG" and preco >= fib_382:
            score += 10
            detalhes["fib"] = "favor (10)"
        elif direcao == "SHORT" and preco <= fib_618:
            score += 10
            detalhes["fib"] = "favor (10)"
        else:
            detalhes["fib"] = "contra (0)"

        # 7. VOLUME CANDLE (10)
        vol_a = c1["volume"]
        vol_m = c1["vol_media"] if pd.notna(c1["vol_media"]) else 0
        if vol_m > 0:
            r = vol_a / vol_m
            if r >= 1.2:
                score += 10
                detalhes["vol"] = f"{r:.1f}x (10)"
            elif r >= 0.8:
                score += 5
                detalhes["vol"] = f"{r:.1f}x (5)"
            else:
                detalhes["vol"] = f"{r:.1f}x (0)"
        else:
            detalhes["vol"] = "sem_dados"

        # 8. VOLUME PROFILE (10)
        vp = calcular_volume_profile(df5.tail(50), 24)
        if vp["valid"]:
            val, vah, poc = vp["val"], vp["vah"], vp["poc"]
            range_va = vah - val if vah > val else 1
            if direcao == "LONG":
                dist = (preco - val) / range_va
                if dist <= 0.25:
                    score += 10
                    detalhes["vp"] = "VAL (10)"
                elif dist <= 0.5 and preco < poc:
                    score += 7
                    detalhes["vp"] = "abaixo POC (7)"
                elif preco < vah:
                    score += 3
                    detalhes["vp"] = "dentro VA (3)"
                else:
                    detalhes["vp"] = "acima VAH (0)"
            else:
                dist = (vah - preco) / range_va
                if dist <= 0.25:
                    score += 10
                    detalhes["vp"] = "VAH (10)"
                elif dist <= 0.5 and preco > poc:
                    score += 7
                    detalhes["vp"] = "acima POC (7)"
                elif preco > val:
                    score += 3
                    detalhes["vp"] = "dentro VA (3)"
                else:
                    detalhes["vp"] = "abaixo VAL (0)"

    except Exception as e:
        detalhes["erro"] = str(e)
    return score, detalhes


print("=" * 80)
print("BACKTEST DO SCORE 3x — 12 DCAs MANUAIS HISTORICOS")
print("=" * 80)
print(f"{'#':<3} {'Symbol':<14} {'Dir':<6} {'Data':<20} {'Score':<10}")
print("-" * 80)

scores = []
for i, (dt, sym, dir) in enumerate(DCAS_MANUAIS, 1):
    # Não testa ativos com nome em chinês (caracteres especiais)
    try:
        score, det = score_historico(sym, dir, dt)
        scores.append((sym, dir, dt, score, det))
        status = "[PASSA 93]" if score >= 93 else "[PASSA 85]" if score >= 85 else "[PASSA 75]" if score >= 75 else ""
        print(f"{i:<3} {sym:<14} {dir:<6} {dt:<20} {score}/110 {status}")
    except Exception as e:
        print(f"{i:<3} {sym:<14} {dir:<6} {dt:<20} ERRO: {e}")

print()
print("=" * 80)
print("RESUMO")
print("=" * 80)
if scores:
    scores_vals = [s[3] for s in scores]
    print(f"Total testados: {len(scores)}")
    print(f"Score medio:    {sum(scores_vals)/len(scores_vals):.1f}/110")
    print(f"Score maximo:   {max(scores_vals)}/110")
    print(f"Score minimo:   {min(scores_vals)}/110")
    print()
    print(f"Passariam em gatilho 93 (atual): {sum(1 for s in scores_vals if s >= 93)}/{len(scores)} ({sum(1 for s in scores_vals if s >= 93)/len(scores)*100:.0f}%)")
    print(f"Passariam em gatilho 85:         {sum(1 for s in scores_vals if s >= 85)}/{len(scores)} ({sum(1 for s in scores_vals if s >= 85)/len(scores)*100:.0f}%)")
    print(f"Passariam em gatilho 75:         {sum(1 for s in scores_vals if s >= 75)}/{len(scores)} ({sum(1 for s in scores_vals if s >= 75)/len(scores)*100:.0f}%)")
    print(f"Passariam em gatilho 65:         {sum(1 for s in scores_vals if s >= 65)}/{len(scores)} ({sum(1 for s in scores_vals if s >= 65)/len(scores)*100:.0f}%)")
    print()
    print("DETALHES DOS MELHORES (top 5):")
    scores_sorted = sorted(scores, key=lambda x: -x[3])
    for sym, dir, dt, sc, det in scores_sorted[:5]:
        print(f"\n  {sym} {dir} @ {dt}: {sc}/110")
        for k, v in det.items():
            print(f"    {k}: {v}")

    print()
    print("DETALHES DOS PIORES (bottom 3):")
    for sym, dir, dt, sc, det in scores_sorted[-3:]:
        print(f"\n  {sym} {dir} @ {dt}: {sc}/110")
        for k, v in det.items():
            print(f"    {k}: {v}")
