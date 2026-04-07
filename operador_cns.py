#!/usr/bin/env python3
"""
Operador CNS v2 — Opera como o Claude ao vivo
Roda 24/7. Ciclo a cada 1 min. A cada 15 min faz análise completa:
- Busca novas entradas (cruzamentos frescos)
- Trava lucro (ROI alto + volume esfriando)
- Equilibra margem (posições desequilibradas + MA favor)
- Dispara 3x (posição <= -50% + score >= 50 + 5min favor)
- Stop relativo pós-3x (corta se piorar além do limite)
"""
import os
import sys
import time
import json
import logging
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
from binance.client import Client
from binance.exceptions import BinanceAPIException

load_dotenv()
client = Client(os.getenv("BINANCE_API_KEY"), os.getenv("BINANCE_API_SECRET"))

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
RISCO_POR_TRADE = float(os.getenv("RISCO_POR_TRADE", "0.03"))
ALAVANCAGEM = int(os.getenv("ALAVANCAGEM", "20"))
RACIO_MAX = float(os.getenv("RACIO_MARGEM_MAX", "8.0"))
SCORE_MINIMO_3X = 50  # licao EDGEUSDT: score 43 falhou, 53+ funcionou

LOG_FILE = "C:/robo-trade/operador_cns.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("operador")

# Tracking
pico_roi = {}
dca_ativo = {}  # symbol -> {"roi_entrada": float, "stop": float, "timestamp": float}
acoes_executadas = []


def telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        import requests
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10
        )
    except Exception:
        pass


def get_racio():
    acc = client.futures_account()
    maint = float(acc.get("totalMaintMargin", 0))
    total_bal = float(acc.get("totalMarginBalance", 0))
    return (maint / total_bal * 100) if total_bal > 0 else 0


def get_saldo():
    return float(client.futures_account()["totalWalletBalance"])


def posicoes_abertas():
    pos = client.futures_position_information()
    return [p for p in pos if float(p["positionAmt"]) != 0]


def calcular_roi(p):
    m = float(p.get("positionInitialMargin", 0))
    pnl = float(p.get("unRealizedProfit", p.get("unrealizedProfit", 0)))
    return (pnl / m * 100) if m > 0 else 0


def get_ma(symbol, interval, limit=30):
    kl = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
    df = pd.DataFrame(kl, columns=["time","open","high","low","close","volume","ct","qav","trades","tbav","tqav","ignore"])
    for col in ["open","high","low","close","volume"]:
        df[col] = df[col].astype(float)
    df["ma7"] = df["close"].rolling(7).mean()
    df["ma25"] = df["close"].rolling(25).mean()
    return df


def get_precisao(symbol):
    info = client.futures_exchange_info()
    for s in info["symbols"]:
        if s["symbol"] == symbol:
            for f in s["filters"]:
                if f["filterType"] == "LOT_SIZE":
                    step = float(f["stepSize"])
                    return 0 if step >= 1 else len(f["stepSize"].rstrip("0").split(".")[-1])
    return 0


def fechar_parcial(symbol, pct, motivo):
    try:
        pos = client.futures_position_information(symbol=symbol)
        for p in pos:
            amt = float(p["positionAmt"])
            if amt == 0:
                continue
            side = "SELL" if amt > 0 else "BUY"
            prec = get_precisao(symbol)
            qty = round(abs(amt) * pct, prec)
            if qty <= 0:
                return False
            client.futures_create_order(symbol=symbol, side=side, type="MARKET", quantity=qty, reduceOnly=True)
            pnl = float(p.get("unRealizedProfit", p.get("unrealizedProfit", 0)))
            roi = calcular_roi(p)
            log.info(f"FECHADO {pct*100:.0f}% {symbol} | ROI {roi:+.1f}% | PnL ${pnl*pct:+.2f} | {motivo}")
            acoes_executadas.append(f"{symbol} {pct*100:.0f}% fechado: {motivo}")
            telegram(f"<b>Operador: {motivo}</b>\n{symbol} | ROI {roi:+.1f}% | Fechado {pct*100:.0f}%")
            return True
    except Exception as e:
        log.error(f"Erro fechar {symbol}: {e}")
    return False


def disparar_3x(symbol, direcao, p):
    """Dispara DCA dinâmico numa posição."""
    try:
        margem = float(p.get("positionInitialMargin", 0))
        pnl = float(p.get("unRealizedProfit", p.get("unrealizedProfit", 0)))
        roi = calcular_roi(p)
        saldo = get_saldo()

        perda = abs(pnl)
        fator = 1.0 - (abs(roi) / 100 / ALAVANCAGEM)
        adicional = (perda / (ALAVANCAGEM * 0.005)) - (fator * margem)
        adicional = max(adicional, margem * 2.0)
        cap = min(0.80, 0.30 + abs(roi) * 0.001)
        adicional = min(adicional, saldo * cap)
        adicional = round(adicional, 2)

        margem_total = margem + adicional
        rec = perda / (ALAVANCAGEM * (fator * margem + adicional)) * 100
        stop_delta = max(2.0, min(10.0, (saldo * 0.02 / margem_total) * 100))

        preco = float(client.futures_symbol_ticker(symbol=symbol)["price"])
        prec = get_precisao(symbol)
        side = "BUY" if direcao == "LONG" else "SELL"
        qty = round((adicional * ALAVANCAGEM) / preco, prec)

        if qty <= 0:
            return False

        client.futures_create_order(symbol=symbol, side=side, type="MARKET", quantity=qty)

        log.info(f"3x {symbol} {direcao} | +${adicional:.2f} ({adicional/saldo*100:.0f}%) | breakeven {rec:.2f}%")
        acoes_executadas.append(f"3x {symbol} +${adicional:.2f}")
        telegram(
            f"<b>3x EM ACAO! {symbol}</b>\n"
            f"{direcao} | +${adicional:.2f} ({adicional/saldo*100:.0f}% banca)\n"
            f"Breakeven: {rec:.2f}%\n"
            f"A virada comeca agora! Monitorando a cada 5s."
        )

        # MONITORAMENTO INTENSO POS-3x: a cada 5 segundos
        # Regra: cruzou zero = VENDE IMEDIATO. Sem trailing, sem esperar.
        # Licao REZUSDT: subiu +24% e devolveu tudo em 1 min.
        # Com DCA pesado, qualquer lucro eh lucro BOM.
        roi_entrada = calcular_roi(p)
        stop_val = roi_entrada - stop_delta
        log.info(f"  Vigiando {symbol} a cada 5s | entrada {roi_entrada:+.1f}% | stop {stop_val:.1f}%")

        inicio_3x = time.time()
        while time.time() - inicio_3x < 600:  # max 10 min de vigia intensiva
            try:
                pos_check = client.futures_position_information(symbol=symbol)
                for pc in pos_check:
                    if float(pc["positionAmt"]) == 0:
                        continue
                    mc = float(pc.get("positionInitialMargin", 0))
                    pnlc = float(pc.get("unRealizedProfit", pc.get("unrealizedProfit", 0)))
                    roic = (pnlc / mc * 100) if mc > 0 else 0

                    # LUCRO! Cruzou zero → vende 90% IMEDIATO
                    if roic > 0:
                        log.info(f"  {symbol} LUCRO {roic:+.1f}% ${pnlc:+.2f} → VENDENDO 90% AGORA!")
                        fechar_parcial(symbol, 0.90, f"3x lucro imediato {roic:+.1f}%")
                        telegram(
                            f"<b>3x LUCRO REALIZADO! {symbol}</b>\n"
                            f"{direcao} | ROI: {roic:+.1f}% | ${pnlc*0.9:+.2f}\n"
                            f"Vendido no primeiro positivo. Formiguinha!"
                        )
                        return True

                    # STOP: piorou alem do limite
                    if roic <= stop_val:
                        log.warning(f"  {symbol} STOP {roic:+.1f}% <= {stop_val:.1f}%")
                        fechar_parcial(symbol, 0.90, f"3x stop {roic:+.1f}%")
                        return False

                    elapsed = int(time.time() - inicio_3x)
                    if elapsed % 15 == 0:
                        log.info(f"  [{elapsed}s] {symbol} ROI {roic:+.1f}%")
            except Exception:
                pass
            time.sleep(1)  # check a cada 1 segundo — nao pode perder o time

        # 10 min sem resolver — deixa o ciclo normal cuidar
        log.info(f"  {symbol} 10 min sem resolver — monitoramento normal assume")
        dca_ativo[symbol] = {
            "roi_entrada": roi_entrada,
            "stop": stop_val,
            "timestamp": time.time(),
            "adicional": adicional,
        }
        return True
    except Exception as e:
        log.error(f"Erro 3x {symbol}: {e}")
    return False


def buscar_novas_entradas():
    """Busca cruzamentos frescos no mercado e abre posições."""
    try:
        saldo = get_saldo()
        racio = get_racio()
        if racio >= RACIO_MAX:
            log.info(f"Novas entradas bloqueadas: racio {racio:.1f}%")
            return

        pos = posicoes_abertas()
        abertos = {p["symbol"] for p in pos}
        if len(pos) >= 15:
            log.info(f"Max posicoes atingido: {len(pos)}/15")
            return

        # Equilibrar antes de abrir novas
        alvo_margem = saldo * RISCO_POR_TRADE
        deseq = [p for p in pos if float(p.get("positionInitialMargin", 0)) < alvo_margem * 0.5
                 and float(p["positionAmt"]) != 0 and p["symbol"] not in dca_ativo]
        if deseq:
            log.info(f"Prioridade: {len(deseq)} posicoes desequilibradas — equilibrar primeiro")
            return

        tickers = client.futures_ticker()
        tickers_sorted = sorted(tickers, key=lambda x: float(x.get("quoteVolume", 0)), reverse=True)
        info = client.futures_exchange_info()
        valid = {s["symbol"] for s in info["symbols"] if s["status"] == "TRADING" and s["symbol"].endswith("USDT")}

        sinais = []
        for t in tickers_sorted[:100]:
            sym = t["symbol"]
            if sym in abertos or sym not in valid:
                continue
            try:
                df = get_ma(sym, "5m", 30)
                if pd.isna(df["ma25"].iloc[-1]):
                    continue
                c1, c2, c3 = df.iloc[-1], df.iloc[-2], df.iloc[-3]
                vol = c1["volume"] / df["volume"].iloc[-10:].mean() if df["volume"].iloc[-10:].mean() > 0 else 0

                cruzou_l = c3["ma7"] <= c3["ma25"] and c1["ma7"] > c1["ma25"]
                cruzou_s = c3["ma7"] >= c3["ma25"] and c1["ma7"] < c1["ma25"]
                verde = c1["close"] > c1["open"]
                verm = c1["close"] < c1["open"]
                sep_l = (c1["ma7"] - c1["ma25"]) > (c2["ma7"] - c2["ma25"])
                sep_s = (c1["ma25"] - c1["ma7"]) > (c2["ma25"] - c2["ma7"])

                if cruzou_l and verde and sep_l:
                    sinais.append((sym, "LONG", vol))
                elif cruzou_s and verm and sep_s:
                    sinais.append((sym, "SHORT", vol))
            except Exception:
                pass

        sinais.sort(key=lambda x: -x[2])
        margem = saldo * RISCO_POR_TRADE

        for sym, dire, vol in sinais[:3]:  # max 3 novas por ciclo
            if len(posicoes_abertas()) >= 15 or get_racio() >= RACIO_MAX:
                break
            try:
                preco = float(client.futures_symbol_ticker(symbol=sym)["price"])
                prec = get_precisao(sym)
                side = "BUY" if dire == "LONG" else "SELL"
                qty = round((margem * ALAVANCAGEM) / preco, prec)
                if qty > 0:
                    client.futures_create_order(symbol=sym, side=side, type="MARKET", quantity=qty)
                    log.info(f"NOVA ENTRADA {sym} {dire} | ${margem:.2f} margem | vol {vol:.1f}x")
                    acoes_executadas.append(f"Nova entrada {sym} {dire}")
                    telegram(f"<b>Nova entrada: {sym} {dire}</b>\nMargem: ${margem:.2f} | Vol: {vol:.1f}x")
            except Exception as e:
                log.warning(f"Erro entrada {sym}: {e}")
    except Exception as e:
        log.error(f"Erro buscar entradas: {e}")


def ciclo_rapido():
    """Ciclo a cada 1 min — monitora 3x ativos e trailing."""
    try:
        abertas = posicoes_abertas()
        pnl_total = sum(float(p.get("unRealizedProfit", p.get("unrealizedProfit", 0))) for p in abertas)

        for p in abertas:
            symbol = p["symbol"]
            amt = float(p["positionAmt"])
            if amt == 0:
                continue
            direcao = "LONG" if amt > 0 else "SHORT"
            roi = calcular_roi(p)
            margem = float(p.get("positionInitialMargin", 0))
            pnl = float(p.get("unRealizedProfit", p.get("unrealizedProfit", 0)))

            # Atualiza pico
            if symbol not in pico_roi or roi > pico_roi[symbol]:
                pico_roi[symbol] = roi
            pico = pico_roi.get(symbol, roi)

            # --- STOP RELATIVO POS-3x ---
            if symbol in dca_ativo:
                info_3x = dca_ativo[symbol]
                if roi <= info_3x["stop"]:
                    log.warning(f"STOP 3x {symbol} | ROI {roi:+.1f}% <= {info_3x['stop']:.1f}%")
                    fechar_parcial(symbol, 0.90, f"Stop 3x ({roi:+.1f}%)")
                    dca_ativo.pop(symbol, None)
                    pico_roi.pop(symbol, None)
                    continue

                # Piso zero: se cruzou zero e voltou negativo, corta
                if pico > 0 and roi <= 0:
                    log.warning(f"PISO ZERO {symbol} | cruzou zero (pico {pico:+.1f}%) e voltou {roi:+.1f}%")
                    fechar_parcial(symbol, 0.90, f"Piso zero violado ({pico:+.1f}% -> {roi:+.1f}%)")
                    dca_ativo.pop(symbol, None)
                    pico_roi.pop(symbol, None)
                    continue

                # Lucro: trailing se ROI > 0 e pico >= 1%
                if pico >= 1.0 and roi > 0:
                    if pico < 3.0:
                        stop_trail = 0.0
                    elif pico < 10.0:
                        stop_trail = pico * 0.5
                    elif pico < 20.0:
                        stop_trail = pico - 6.0
                    elif pico < 30.0:
                        stop_trail = pico - 8.0
                    else:
                        stop_trail = pico - 10.0

                    if roi <= stop_trail:
                        log.info(f"TRAILING 3x {symbol} | pico {pico:+.1f}% stop {stop_trail:.1f}% roi {roi:+.1f}%")
                        fechar_parcial(symbol, 0.90, f"Trailing 3x pico {pico:+.0f}% saida {roi:+.1f}%")
                        dca_ativo.pop(symbol, None)
                        pico_roi.pop(symbol, None)
                        continue

            # --- TRAVAR LUCRO: pico >= 25% e caiu 15pp+ com volume esfriando ---
            elif pico >= 25 and roi > 0 and (pico - roi) >= 15:
                try:
                    df1 = get_ma(symbol, "1m", 10)
                    vol_a = df1["volume"].iloc[-1]
                    vol_m = df1["volume"].iloc[-5:].mean()
                    if vol_a < vol_m * 0.7:
                        fechar_parcial(symbol, 0.90, f"Lucro travado pico {pico:+.0f}% vol esfriou")
                        pico_roi.pop(symbol, None)
                        continue
                except Exception:
                    pass

        # Log resumo
        pos_pos = sum(1 for p in abertas if float(p.get("unRealizedProfit", p.get("unrealizedProfit", 0))) >= 0)
        destaques = []
        for p in abertas:
            roi = calcular_roi(p)
            pnl = float(p.get("unRealizedProfit", p.get("unrealizedProfit", 0)))
            if abs(roi) >= 20 or abs(pnl) >= 0.50 or p["symbol"] in dca_ativo:
                tag = " [3x]" if p["symbol"] in dca_ativo else ""
                destaques.append(f"{p['symbol'][:-4]} {roi:+.0f}%{tag}")
        if destaques:
            saldo = get_saldo()
            log.info(f"${saldo:.0f} | PnL ${pnl_total:+.2f} | {pos_pos}/{len(abertas)} | {' | '.join(destaques)}")

    except Exception as e:
        log.error(f"Erro ciclo rapido: {e}")


def ciclo_completo():
    """Ciclo a cada 15 min — análise completa + novas entradas + 3x."""
    try:
        abertas = posicoes_abertas()
        saldo = get_saldo()
        racio = get_racio()
        pnl_total = sum(float(p.get("unRealizedProfit", p.get("unrealizedProfit", 0))) for p in abertas)
        alvo_margem = saldo * RISCO_POR_TRADE

        log.info(f"=== ANALISE COMPLETA === Saldo ${saldo:.2f} | PnL ${pnl_total:+.2f} | Racio {racio:.1f}% | {len(abertas)} pos")

        for p in abertas:
            symbol = p["symbol"]
            amt = float(p["positionAmt"])
            if amt == 0:
                continue
            direcao = "LONG" if amt > 0 else "SHORT"
            roi = calcular_roi(p)
            margem = float(p.get("positionInitialMargin", 0))
            pnl = float(p.get("unRealizedProfit", p.get("unrealizedProfit", 0)))

            # Equilibrar margem se MA favor
            if margem < alvo_margem * 0.5 and symbol not in dca_ativo and racio < RACIO_MAX:
                try:
                    df5 = get_ma(symbol, "5m", 30)
                    c1 = df5.iloc[-1]
                    if pd.isna(c1["ma25"]):
                        continue
                    if direcao == "LONG":
                        ma_ok = c1["ma7"] > c1["ma25"]
                    else:
                        ma_ok = c1["ma7"] < c1["ma25"]
                    if ma_ok:
                        falta = alvo_margem - margem
                        if falta > 0.50:
                            preco = float(client.futures_symbol_ticker(symbol=symbol)["price"])
                            prec = get_precisao(symbol)
                            side = "BUY" if direcao == "LONG" else "SELL"
                            qty = round((falta * ALAVANCAGEM) / preco, prec)
                            if qty > 0 and get_racio() < RACIO_MAX:
                                client.futures_create_order(symbol=symbol, side=side, type="MARKET", quantity=qty)
                                log.info(f"TOPUP {symbol} +${falta:.2f} | MA favor")
                                acoes_executadas.append(f"Topup {symbol} +${falta:.2f}")
                                telegram(f"<b>Topup: {symbol}</b>\n{direcao} | +${falta:.2f} | MA favor")
                except Exception:
                    pass

            # 3x se ROI <= -50% e score bom e 5min favor
            if roi <= -50.0 and symbol not in dca_ativo:
                try:
                    # Score via calcular_score_3x do nunes
                    sys.path.insert(0, "C:/robo-trade")
                    from nunes import calcular_score_3x
                    score, _ = calcular_score_3x(client, symbol, direcao)

                    if score >= SCORE_MINIMO_3X:
                        # Confirma 5min favor
                        df5 = get_ma(symbol, "5m", 30)
                        c5 = df5.iloc[-1]
                        if direcao == "LONG":
                            ma5_favor = c5["ma7"] > c5["ma25"]
                        else:
                            ma5_favor = c5["ma7"] < c5["ma25"]

                        if ma5_favor:
                            log.info(f"3x CANDIDATO {symbol} | ROI {roi:+.0f}% | Score {score} | 5min favor")
                            # Um por vez — so se nao tem outro 3x ativo
                            if not dca_ativo:
                                disparar_3x(symbol, direcao, p)
                            else:
                                log.info(f"  Aguardando — ja tem 3x ativo: {list(dca_ativo.keys())}")
                        else:
                            log.info(f"  {symbol} score {score} mas 5min contra — aguardando")
                    else:
                        log.info(f"  {symbol} ROI {roi:+.0f}% score {score} < {SCORE_MINIMO_3X} — aguardando")
                except Exception as e:
                    log.warning(f"  Erro score {symbol}: {e}")

            log.info(f"  {symbol} {direcao} ${margem:.2f} ROI {roi:+.1f}% ${pnl:+.2f}")

        # Buscar novas entradas
        buscar_novas_entradas()

    except Exception as e:
        log.error(f"Erro ciclo completo: {e}")


def main():
    log.info("=" * 50)
    log.info("OPERADOR CNS v2 INICIADO")
    log.info("Ciclo rapido: 1 min | Ciclo completo: 15 min")
    log.info(f"Score minimo 3x: {SCORE_MINIMO_3X} | 3x um por vez")
    log.info("=" * 50)
    telegram("<b>Operador CNS v2 iniciado</b>\nMonitoramento 24/7 ativo.\n1min: trailing/stop\n15min: analise + entradas + 3x")

    ultimo_completo = 0

    while True:
        try:
            agora = time.time()

            # Ciclo rapido: a cada 1 min
            ciclo_rapido()

            # Ciclo completo: a cada 15 min
            if agora - ultimo_completo >= 900:
                ciclo_completo()
                ultimo_completo = agora

            time.sleep(60)

        except KeyboardInterrupt:
            log.info("Operador encerrado pelo usuario.")
            telegram("<b>Operador CNS encerrado</b>")
            break
        except Exception as e:
            log.error(f"Erro geral: {e}")
            time.sleep(30)


if __name__ == "__main__":
    main()
