#!/usr/bin/env python3
"""Análise técnica completa do DRIFTUSDT SHORT"""
from binance.client import Client
from dotenv import load_dotenv
import pandas as pd
import os

load_dotenv()
client = Client(os.getenv("BINANCE_API_KEY"), os.getenv("BINANCE_API_SECRET"))

symbol = "DRIFTUSDT"
entrada_short = 0.0262704172156

def get_df(interval, limit=50):
    k = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
    df = pd.DataFrame(k, columns=['ts','open','high','low','close','vol','tc','qv','nt','tbv','tqv','ig'])
    for c in ['open','high','low','close','vol']:
        df[c] = df[c].astype(float)
    df['ma7'] = df['close'].rolling(7).mean()
    df['ma25'] = df['close'].rolling(25).mean()
    return df

def rsi(df, period=14):
    delta = df['close'].diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

df5 = get_df('5m', 50)
df1 = get_df('1m', 30)
df15 = get_df('15m', 50)
df1h = get_df('1h', 50)
df4h = get_df('4h', 50)

df5['rsi'] = rsi(df5)

c5 = df5.iloc[-1]
c5p = df5.iloc[-2]
c1m = df1.iloc[-1]
c15 = df15.iloc[-1]
c1h = df1h.iloc[-1]
c4h = df4h.iloc[-1]

preco = c5['close']

# Fibonacci 5min
h20 = df5['high'].iloc[-20:].max()
l20 = df5['low'].iloc[-20:].min()
rng = h20 - l20
fib = {
    '0.236': h20 - rng * 0.236,
    '0.382': h20 - rng * 0.382,
    '0.500': h20 - rng * 0.500,
    '0.618': h20 - rng * 0.618,
    '0.786': h20 - rng * 0.786,
}

# Fibonacci 1h
h1h = df1h['high'].iloc[-20:].max()
l1h = df1h['low'].iloc[-20:].min()
rng1h = h1h - l1h
fib1h = {
    '0.236': h1h - rng1h * 0.236,
    '0.382': h1h - rng1h * 0.382,
    '0.500': h1h - rng1h * 0.500,
    '0.618': h1h - rng1h * 0.618,
}

# Volume Profile
prices = df5['close'].values
volumes = df5['vol'].values
n_b = 24
pmin, pmax = prices.min(), prices.max()
bs = (pmax - pmin) / n_b if pmax > pmin else 1
vp = [0.0] * n_b
for p, v in zip(prices, volumes):
    idx = min(int((p - pmin) / bs), n_b - 1)
    vp[idx] += v
poc_idx = vp.index(max(vp))
poc = pmin + (poc_idx + 0.5) * bs
total = sum(vp)
cum = 0
val_idx = 0
for i in range(n_b):
    cum += vp[i]
    if cum >= total * 0.2:
        val_idx = i; break
cum = 0
vah_idx = n_b - 1
for i in range(n_b - 1, -1, -1):
    cum += vp[i]
    if cum >= total * 0.2:
        vah_idx = i; break
val = pmin + val_idx * bs
vah = pmin + (vah_idx + 1) * bs

# BTC
btc_k = client.futures_klines(symbol='BTCUSDT', interval='1h', limit=5)
btc_p = [float(k[4]) for k in btc_k]
btc_var = ((btc_p[-1] - btc_p[0]) / btc_p[0]) * 100

# Volume
vol_media = df5['vol'].rolling(20).mean().iloc[-1]
vol_atual = df5['vol'].iloc[-1]
vol_ratio = vol_atual / vol_media if vol_media > 0 else 0

rsi_val = df5['rsi'].iloc[-1]

print("=" * 55)
print("  DRIFTUSDT SHORT — ANALISE TECNICA COMPLETA")
print("=" * 55)
print(f"Preco atual: {preco:.6f}")
print(f"Entrada:     {entrada_short:.6f}")
print(f"Variacao:    {((preco - entrada_short) / entrada_short) * 100:+.1f}% (contra o short)")
print()

print("--- MEDIAS MOVEIS (SHORT quer MA7 < MA25) ---")
for nome, df_t in [("1min", df1), ("5min", df5), ("15min", df15), ("1h", df1h), ("4h", df4h)]:
    c = df_t.iloc[-1]
    cp = df_t.iloc[-2]
    if pd.isna(c['ma7']) or pd.isna(c['ma25']):
        print(f"  {nome:5s}: dados insuficientes")
        continue
    gap = (c['ma7'] - c['ma25']) / c['ma25'] * 100
    gap_prev = (cp['ma7'] - cp['ma25']) / cp['ma25'] * 100
    favor = "A FAVOR" if c['ma7'] < c['ma25'] else "CONTRA"
    conv = "convergindo" if abs(gap) < abs(gap_prev) else "divergindo"
    print(f"  {nome:5s}: MA7={c['ma7']:.6f} MA25={c['ma25']:.6f} | gap {gap:+.3f}% {conv} | {favor}")
print()

print("--- RSI 14 (5min) ---")
if rsi_val > 70:
    status = "SOBRECOMPRADO — correcao provavel (BOM pra short)"
elif rsi_val > 60:
    status = "ALTO — pressao compradora forte (cuidado)"
elif rsi_val < 30:
    status = "SOBREVENDIDO — pode pular (RUIM pra short)"
elif rsi_val < 40:
    status = "BAIXO — fraqueza (bom pra short)"
else:
    status = "NEUTRO"
print(f"  RSI: {rsi_val:.1f} | {status}")
print()

print("--- FIBONACCI 5min (ultimos 20 candles) ---")
print(f"  High: {h20:.6f} | Low: {l20:.6f}")
for nivel, valor in fib.items():
    marca = " <-- PRECO AQUI" if abs(preco - valor) / preco < 0.003 else ""
    print(f"  {nivel}: {valor:.6f}{marca}")
print(f"  Preco {preco:.6f} esta {((preco - l20) / rng * 100):.0f}% do range")
pos_fib = "ACIMA de 0.618 (topo — bom pra short)" if preco >= fib['0.618'] else \
          "ENTRE 0.5-0.618" if preco >= fib['0.500'] else \
          "ENTRE 0.382-0.5" if preco >= fib['0.382'] else \
          "ABAIXO de 0.382 (fundo — ruim pra short)"
print(f"  Posicao: {pos_fib}")
print()

print("--- FIBONACCI 1h (ultimos 20 candles) ---")
print(f"  High: {h1h:.6f} | Low: {l1h:.6f}")
for nivel, valor in fib1h.items():
    marca = " <-- PRECO AQUI" if abs(preco - valor) / preco < 0.005 else ""
    print(f"  {nivel}: {valor:.6f}{marca}")
print(f"  Preco {preco:.6f} esta {((preco - l1h) / rng1h * 100):.0f}% do range 1h")
print()

print("--- VOLUME PROFILE ---")
print(f"  POC (pico volume): {poc:.6f}")
print(f"  VAL (suporte):     {val:.6f}")
print(f"  VAH (resistencia): {vah:.6f}")
if preco > vah:
    vp_txt = "ACIMA do VAH — zona de distribuicao (BOM pra short)"
elif preco > poc:
    vp_txt = "ACIMA do POC — forca compradora (cuidado)"
elif preco > val:
    vp_txt = "DENTRO da Value Area"
else:
    vp_txt = "ABAIXO do VAL — fraqueza (bom pra short se ja ta caindo)"
print(f"  Preco: {vp_txt}")
print()

print("--- VOLUME ---")
print(f"  Candle atual: {vol_atual:.0f} | Media 20: {vol_media:.0f} | Ratio: {vol_ratio:.2f}x")
print()

print("--- BTC ---")
btc_txt = "SUBINDO (contra short crypto)" if btc_var > 1 else "CAINDO (ajuda short)" if btc_var < -1 else "LATERAL"
print(f"  Variacao 5h: {btc_var:+.2f}% | {btc_txt}")
print()

print("--- CANDLE 5min ATUAL ---")
cor = "VERMELHO (a favor short)" if c5['close'] < c5['open'] else "VERDE (contra short)"
corpo = abs(c5['close'] - c5['open'])
range_c = c5['high'] - c5['low']
corpo_pct = (corpo / range_c * 100) if range_c > 0 else 0
print(f"  Cor: {cor} | Corpo: {corpo_pct:.0f}% do range")
print()

# Veredicto
print("=" * 55)
print("  VEREDICTO PARA 3x SHORT")
print("=" * 55)
favor = []
contra = []

if c5['ma7'] < c5['ma25']:
    favor.append("MA 5min a favor (MA7 < MA25)")
else:
    contra.append("MA 5min contra (MA7 > MA25)")

gap_now = abs((c5['ma7'] - c5['ma25']) / c5['ma25'] * 100)
gap_prv = abs((c5p['ma7'] - c5p['ma25']) / c5p['ma25'] * 100)
if c5['ma7'] > c5['ma25'] and gap_now < gap_prv:
    favor.append(f"MAs convergindo ({gap_prv:.3f}% -> {gap_now:.3f}%)")
elif c5['ma7'] > c5['ma25']:
    contra.append(f"MAs divergindo ({gap_prv:.3f}% -> {gap_now:.3f}%)")

if c1m['ma7'] < c1m['ma25']:
    favor.append("1min alinhado")
else:
    contra.append("1min contra")

if c15['ma7'] < c15['ma25']:
    favor.append("15min alinhado")
else:
    contra.append("15min contra")

if c1h['ma7'] < c1h['ma25']:
    favor.append("1h alinhado")
else:
    contra.append("1h contra")

if not pd.isna(c4h['ma7']) and not pd.isna(c4h['ma25']):
    if c4h['ma7'] < c4h['ma25']:
        favor.append("4h alinhado")
    else:
        contra.append("4h contra")

if rsi_val > 70:
    favor.append(f"RSI {rsi_val:.0f} sobrecomprado")
elif rsi_val > 60:
    contra.append(f"RSI {rsi_val:.0f} alto mas nao sobrecomprado")
elif rsi_val < 40:
    favor.append(f"RSI {rsi_val:.0f} baixo (fraqueza)")

if preco >= fib['0.618']:
    favor.append("Preco acima Fib 0.618 (zona de reversao)")
elif preco <= fib['0.382']:
    contra.append("Preco abaixo Fib 0.382 (fundo)")

if preco > vah:
    favor.append("Preco acima VAH (distribuicao)")
elif preco > poc:
    contra.append("Preco acima POC (compradores dominam)")

if btc_var < -1:
    favor.append(f"BTC caindo {btc_var:+.1f}%")
elif btc_var > 1:
    contra.append(f"BTC subindo {btc_var:+.1f}%")

print()
for f in favor:
    print(f"  + {f}")
for c in contra:
    print(f"  - {c}")
print()
print(f"  A FAVOR: {len(favor)} | CONTRA: {len(contra)}")
print()
if len(favor) >= 5:
    print("  >>> MOMENTO BOM para 3x SHORT")
elif len(favor) >= 3 and len(favor) > len(contra):
    print("  >>> MOMENTO RAZOAVEL — pode esperar confirmacao")
else:
    print("  >>> NAO E HORA — esperar MAs alinharem")
