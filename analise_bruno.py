from binance.client import Client
from dotenv import load_dotenv
import os, pandas as pd

load_dotenv()
client = Client(os.getenv('BINANCE_API_KEY'), os.getenv('BINANCE_API_SECRET'))

for sym, preco_bruno, hora in [('ETCUSDT', 8.24, '05:00'), ('TRXUSDT', 0.3174, 'agora')]:
    print(f'=== {sym} (Bruno: LONG a {preco_bruno} - {hora}) ===')

    # 5min
    df5 = pd.DataFrame(client.futures_klines(symbol=sym, interval='5m', limit=100),
        columns=['time','open','high','low','close','volume','ct','qav','trades','tbbav','tbqav','ignore'])
    df5['close'] = df5['close'].astype(float)
    df5['high'] = df5['high'].astype(float)
    df5['low'] = df5['low'].astype(float)
    df5['volume'] = df5['volume'].astype(float)
    df5['ma7'] = df5['close'].rolling(7).mean()
    df5['ma25'] = df5['close'].rolling(25).mean()
    df5['ma99'] = df5['close'].rolling(99).mean()

    preco = df5['close'].iloc[-1]
    ma7 = df5['ma7'].iloc[-1]
    ma25 = df5['ma25'].iloc[-1]
    ma99 = df5['ma99'].iloc[-1]
    var_desde = (preco - preco_bruno) / preco_bruno * 100

    # MA99 slope
    ma99_a = df5['ma99'].iloc[-4]
    slope99 = (ma99 - ma99_a) / ma99_a * 100 if ma99_a > 0 else 0

    # MA7 slope
    ma7_a = df5['ma7'].iloc[-4]
    slope7 = (ma7 - ma7_a) / ma7_a * 100 if ma7_a > 0 else 0

    # MA7 acelerando
    diff_now = ma7 - ma25
    diff_prev = df5['ma7'].iloc[-3] - df5['ma25'].iloc[-3]

    # Range
    rng = (df5['high'].tail(20).max() - df5['low'].tail(20).min()) / preco * 100

    # 1H
    df1h = pd.DataFrame(client.futures_klines(symbol=sym, interval='1h', limit=30),
        columns=['time','open','high','low','close','volume','ct','qav','trades','tbbav','tbqav','ignore'])
    df1h['close'] = df1h['close'].astype(float)
    df1h['ma7'] = df1h['close'].rolling(7).mean()
    df1h['ma25'] = df1h['close'].rolling(25).mean()
    ma7_1h = df1h['ma7'].iloc[-1]
    ma25_1h = df1h['ma25'].iloc[-1]

    # 4H
    df4h = pd.DataFrame(client.futures_klines(symbol=sym, interval='4h', limit=30),
        columns=['time','open','high','low','close','volume','ct','qav','trades','tbbav','tbqav','ignore'])
    df4h['close'] = df4h['close'].astype(float)
    df4h['ma7'] = df4h['close'].rolling(7).mean()
    df4h['ma25'] = df4h['close'].rolling(25).mean()
    ma7_4h = df4h['ma7'].iloc[-1]
    ma25_4h = df4h['ma25'].iloc[-1]

    # Ticker
    t = client.futures_ticker(symbol=sym)
    var24 = float(t['priceChangePercent'])
    vol24 = float(t['quoteVolume']) / 1000000

    print(f'  Preco agora: {preco} | Bruno: {preco_bruno} | Desde alerta: {var_desde:+.2f}%')
    print(f'  Var 24h: {var24:+.1f}% | Vol 24h: ${vol24:.0f}M')
    print(f'  ---')
    print(f'  4H:  MA7 {">" if ma7_4h > ma25_4h else "<"} MA25')
    print(f'  1H:  MA7 {">" if ma7_1h > ma25_1h else "<"} MA25')
    print(f'  5min: MA7 {">" if ma7 > ma25 else "<"} MA25 | Preco {">" if preco > ma7 else "<"} MA7')
    print(f'  MA99 slope: {slope99:+.4f}% ({"subindo" if slope99 > 0.01 else ("caindo" if slope99 < -0.01 else "lateral")})')
    print(f'  MA7 slope: {slope7:+.4f}%')
    print(f'  MA7 acelerando: {"SIM" if diff_now > diff_prev else "NAO"}')
    print(f'  Range 20 candles: {rng:.2f}%')
    print(f'  Preco vs MA99: {"ACIMA" if preco > ma99 else "ABAIXO"}')
    print()
