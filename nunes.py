#!/usr/bin/env python3
"""
Robô de trade - Binance Futuros
Estratégia: EMA Multi-Timeframe + RSI
Sessão: Asiática (00h-08h UTC)
"""

import os
import time
import logging
import threading
import json
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from dotenv import load_dotenv
from binance.client import Client
from binance.exceptions import BinanceAPIException
import pandas as pd
import requests

load_dotenv()

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
API_KEY            = os.getenv("BINANCE_API_KEY")
API_SECRET         = os.getenv("BINANCE_API_SECRET")
TELEGRAM_TOKEN     = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

# Copy trading: API read-only da conta master (opcional)
MASTER_API_KEY     = os.getenv("MASTER_API_KEY", "")
MASTER_API_SECRET  = os.getenv("MASTER_API_SECRET", "")

MODO               = os.getenv("MODO", "simulacao")
ESTRATEGIA         = os.getenv("ESTRATEGIA", "swing")  # "swing", "scalping" ou "hibrido"
ALAVANCAGEM        = int(os.getenv("ALAVANCAGEM", "20"))
RISCO_POR_TRADE    = float(os.getenv("RISCO_POR_TRADE", "0.02"))
RACIO_MARGEM_MAX   = float(os.getenv("RACIO_MARGEM_MAX", "6.0"))  # % máximo do Rácio de Margem da Binance
STOP_LOSS_ROI      = float(os.getenv("STOP_LOSS_ROI", "2.0"))
TAKE_PROFIT_ROI    = float(os.getenv("TAKE_PROFIT_ROI", "5.0"))
SCALP_TP           = float(os.getenv("SCALP_TP", "5.0"))    # take profit scalping (% ROI)
SCALP_SL           = float(os.getenv("SCALP_SL", "-3.0"))   # stop loss scalping (% ROI)

MAX_POSICOES          = 30   # limite de segurança absoluto — controle dinâmico abaixo


def limites_por_saldo(saldo: float) -> tuple[int, float]:
    """
    Retorna (max_posicoes, risco_por_trade) baseado no saldo atual.
    Mais saldo = mais posições = mais proteção por diversificação.
    Risco total máximo ~10% do saldo. Rácio de Margem é a trava final.
    """
    if ESTRATEGIA == "scalping":
        return 4, 0.013
    # Saldo baixo: menos posições, mais risco por trade (senão margem é irrisória)
    if saldo < 20:
        return 3, 0.08    # 3 posições, 8% risco (~$1.10 margem com $14)
    elif saldo < 50:
        return 5, 0.05    # 5 posições, 5% risco
    elif saldo < 100:
        return 8, 0.03    # 8 posições, 3% risco
    # Saldo normal: Rácio de Margem é a trava
    return 30, 0.007
TOP_PARES             = 326  # quantos pares por volume monitorar (50% do mercado)
THREADS_VARREDURA     = 10   # pares analisados em paralelo
TIMEOUT_SEM_ENTRADA   = 600  # segundos sem entrada para liberar camada 2 (10 min)
INTERVALO_POSICOES    = 15   # segundos entre verificação de posições (rápido para pegar 3x)
INTERVALO_ENTRADAS    = 60   # segundos entre busca de novas entradas (2H timeframe)
ROI_MIN_REVERSAO      = 20.0 # ROI mínimo para monitorar reversão (%)
LIMITE_PERDA_DIARIA   = float(os.getenv("LIMITE_PERDA_DIARIA", "5.0"))  # % máximo de perda no dia
ROI_STOP_LOSS_MAX     = float(os.getenv("ROI_STOP_LOSS_MAX", "-100.0"))  # fecha posição se ROI abaixo disso
RESUMO_HORA           = 22   # hora do resumo diário (horário local)
DCA_ANTECIPADO_ROI    = -5.0  # ROI mínimo para DCA (qualquer negativo com sinal de MA)
STOP_TEMPO_HORAS      = 24.0    # horas sem recuperação após DCA para fechar
STOP_SEM_SINAL_HORAS  = 12.0   # horas sem nenhum sinal de MA para fechar (sem DCA)
META_CICLO_PCT        = float(os.getenv("META_CICLO_PCT", "5.0"))   # meta de lucro por ciclo (%)
META_CICLO_FASE2_USD  = float(os.getenv("META_CICLO_FASE2_USD", "50.0"))  # meta fixa em USDT após $1.000
META_CICLO_FASE2_MIN  = float(os.getenv("META_CICLO_FASE2_MIN", "1000.0")) # saldo para ativar fase 2

# ---------------------------------------------------------------------------
# Modo Bruno: ativos recorrentes do Bruno + detecção de volume anormal
# ---------------------------------------------------------------------------
PARES_BRUNO = [
    "ATOMUSDT", "MASKUSDT", "ICXUSDT", "SKLUSDT", "ALICEUSDT",
    "NEOUSDT", "QTUMUSDT", "FILUSDT", "APTUSDT", "JASMYUSDT",
    "ETCUSDT", "ENJUSDT", "AXSUSDT", "KSMUSDT", "AVAXUSDT",
    "GALAUSDT", "BATUSDT", "PEOPLEUSDT", "KNCUSDT", "XTZUSDT",
    "ONDOUSDT", "ANKRUSDT", "DASHUSDT", "BONKUSDT", "SNXUSDT",
    "STORJUSDT", "TONUSDT", "ZRXUSDT", "1INCHUSDT", "TRXUSDT",
]
BRUNO_VOLUME_MULT = 3.0   # volume atual >= 3x a média = spike
BRUNO_HORARIO_INICIO = 1  # 01:00 BRT
BRUNO_HORARIO_FIM    = 9  # 09:00 BRT

# Pares correlacionados com BTC — SHORT bloqueado quando BTC em alta
PARES_BTC_CORRELATOS  = {"DOGEUSDT", "XRPUSDT", "XLMUSDT", "SOLUSDT", "ADAUSDT",
                          "DOTUSDT", "MATICUSDT", "LINKUSDT", "AVAXUSDT", "BNBUSDT"}

# ---------------------------------------------------------------------------
# Logging com cores no terminal
# ---------------------------------------------------------------------------
import os as _os
_os.system("")  # ativa suporte a ANSI no Windows

class ColorFormatter(logging.Formatter):
    VERDE   = "\033[92m"
    AMARELO = "\033[93m"
    VERMELHO= "\033[91m"
    AZUL    = "\033[94m"
    CIANO   = "\033[96m"
    RESET   = "\033[0m"

    def format(self, record):
        msg = super().format(record)
        txt = record.getMessage()
        # Posições individuais — cor pelo ROI
        if "ROI" in txt and "|" in txt:
            if "[EM DCA]" in txt:
                return f"{self.AMARELO}{msg}{self.RESET}"
            try:
                roi_part = [p for p in txt.split("|") if "ROI" in p][0]
                roi_val = float(roi_part.replace("ROI", "").replace("%", "").strip())
                if roi_val >= 0:
                    return f"{self.VERDE}{msg}{self.RESET}"
                else:
                    return f"{self.VERMELHO}{msg}{self.RESET}"
            except Exception:
                pass
        # Outros eventos
        if "trailing" in txt.lower() or "saida" in txt.lower() or "fechando" in txt.lower():
            return f"{self.AMARELO}{msg}{self.RESET}"
        if "ERRO" in txt or "Erro" in txt or "URGENTE" in txt or "ALERTA" in txt:
            return f"{self.VERMELHO}{msg}{self.RESET}"
        if "ORDEM ABERTA" in txt or "Sinal" in txt or "DCA aplicado" in txt:
            return f"{self.VERDE}{msg}{self.RESET}"
        if "Posicoes abertas" in txt or "Robo iniciado" in txt or "Resumo" in txt:
            return f"{self.AZUL}{msg}{self.RESET}"
        return msg

_console = logging.StreamHandler()
_console.setFormatter(ColorFormatter("%(asctime)s [%(levelname)s] %(message)s"))

_arquivo = logging.FileHandler("robo.log", encoding="utf-8")
_arquivo.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

logging.basicConfig(level=logging.INFO, handlers=[_arquivo, _console])
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------
def telegram(msg: str) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as e:
        log.warning(f"Telegram erro: {e}")

# ---------------------------------------------------------------------------
# Comandos Telegram
# ---------------------------------------------------------------------------
bot_ativo = True
bot_inicio = time.time()  # timestamp de quando o bot iniciou
ultimo_update_id = 0

def get_updates() -> list:
    global ultimo_update_id
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
            params={"offset": ultimo_update_id + 1, "timeout": 1},
            timeout=5,
        )
        updates = r.json().get("result", [])
        if updates:
            ultimo_update_id = updates[-1]["update_id"]
        return updates
    except Exception:
        return []


def processar_comandos(client: Client) -> None:
    global bot_ativo, dca_ativo
    updates = get_updates()
    for u in updates:
        msg = u.get("message", {})
        texto = msg.get("text", "").strip().lower()
        chat = str(msg.get("chat", {}).get("id", ""))

        if chat != str(TELEGRAM_CHAT_ID):
            continue

        if texto == "/status":
            abertas = posicoes_abertas(client)
            if not abertas:
                telegram("Nenhuma posicao aberta no momento.")
            else:
                margem_total = sum(float(p.get("positionInitialMargin", 0)) for p in abertas)
                racio = get_racio_margem(client)
                usd_brl = get_usd_brl(client)
                pnl_total   = 0
                positivas   = 0
                negativas   = 0
                linhas_pos  = []
                linhas_neg  = []
                for p in abertas:
                    amt  = float(p["positionAmt"])
                    lado = "LONG" if amt > 0 else "SHORT"
                    pnl  = float(p.get("unrealizedProfit", p.get("unRealizedProfit", 0)))
                    roi  = calcular_roi(p)
                    pnl_total += pnl
                    sinal  = "+" if pnl >= 0 else ""
                    pnl_brl = f" (R${pnl * usd_brl:+.2f})" if usd_brl > 0 else ""
                    emoji  = "🟢" if pnl >= 0 else "🔴"
                    linha  = f"{emoji} {p['symbol']} | {lado} | {sinal}{pnl:.2f} USDT{pnl_brl} | ROI: {roi:+.1f}%"
                    if pnl >= 0:
                        positivas += 1
                        linhas_pos.append(linha)
                    else:
                        negativas += 1
                        linhas_neg.append(linha)
                sinal_total   = "+" if pnl_total >= 0 else ""
                pnl_total_brl = f" (R${pnl_total * usd_brl:+.2f})" if usd_brl > 0 else ""
                linhas = [f"<b>Posicoes abertas ({len(abertas)}) | 🟢 {positivas} positivas | 🔴 {negativas} negativas | Racio: {racio:.2f}%</b>\n"]
                linhas += linhas_pos + linhas_neg
                linhas.append(f"\n<b>PnL Total: {sinal_total}{pnl_total:.2f} USDT{pnl_total_brl}</b>")
                linhas.append(f"Margem em uso: ${margem_total:.2f} USDT ({racio:.2f}%)")
                telegram("\n".join(linhas))

        elif texto == "/saldo":
            banca = get_banca(client)
            telegram(f"Saldo disponivel: <b>${banca:.2f} USDT</b>")

        elif texto == "/parar":
            telegram("Encerrando o Nunes...")
            bot_ativo = False

        elif texto == "/iniciar":
            telegram("Nunes esta ativo e operando.")

        elif texto.startswith("/dca"):
            partes = texto.split()
            if len(partes) != 2:
                telegram("Uso: /dca SYMBOL\nEx: /dca FHEUSDT")
            else:
                symbol = partes[1].upper()
                try:
                    abertas = posicoes_abertas(client)
                    posicao = next((p for p in abertas if p["symbol"] == symbol), None)
                    if not posicao:
                        telegram(f"{symbol} nao encontrado nas posicoes abertas.")
                    else:
                        amt     = float(posicao["positionAmt"])
                        direcao = "LONG" if amt > 0 else "SHORT"
                        roi     = calcular_roi(posicao)
                        # Verifica cruzamento com momentum antes de executar
                        if not ma_cruza_favor(client, symbol, direcao):
                            telegram(
                                f"<b>DCA bloqueado: {symbol}</b>\n"
                                f"{direcao} | ROI: {roi:+.1f}%\n"
                                f"MA7 ainda nao cruzou a MA25 com momentum a favor.\n"
                                f"Aguarde o cruzamento ou use /dca {symbol} forcado para ignorar."
                            )
                        else:
                            banca = get_banca(client)
                            telegram(f"Sinal confirmado. Aplicando DCA em {symbol} | ROI: {roi:+.1f}%")
                            aplicar_dca(client, posicao, banca)
                            dca_ativo = symbol
                except Exception as e:
                    telegram(f"Erro ao aplicar DCA em {symbol}: {e}")

        elif texto.startswith("/dca") and "forcado" in texto:
            partes = texto.split()
            symbol = partes[1].upper() if len(partes) >= 2 else None
            if not symbol:
                telegram("Uso: /dca SYMBOL forcado")
            else:
                try:
                    abertas = posicoes_abertas(client)
                    posicao = next((p for p in abertas if p["symbol"] == symbol), None)
                    if not posicao:
                        telegram(f"{symbol} nao encontrado.")
                    else:
                        roi   = calcular_roi(posicao)
                        banca = get_banca(client)
                        telegram(f"DCA forcado em {symbol} | ROI: {roi:+.1f}% (sem verificar MA)")
                        aplicar_dca(client, posicao, banca)
                        dca_ativo = symbol
                except Exception as e:
                    telegram(f"Erro: {e}")

        elif texto.startswith("/canceldca"):
            partes = texto.split()
            if len(partes) != 2:
                telegram("Uso: /canceldca SYMBOL\nEx: /canceldca GUSDT")
            else:
                symbol = partes[1].upper()
                try:
                    abertas = posicoes_abertas(client)
                    posicao = next((p for p in abertas if p["symbol"] == symbol), None)
                    if not posicao:
                        telegram(f"{symbol} nao encontrado.")
                    elif symbol not in dca_aplicado:
                        telegram(f"{symbol} nao tem DCA ativo.")
                    else:
                        roi = calcular_roi(posicao)
                        amt = float(posicao["positionAmt"])
                        side = "SELL" if amt > 0 else "BUY"
                        client.futures_create_order(
                            symbol=symbol, side=side, type="MARKET",
                            quantity=abs(amt), reduceOnly=True
                        )
                        dca_aplicado.discard(symbol)
                        peak_roi.pop(symbol, None)
                        ma_reverteu.pop(symbol, None)
                        if dca_ativo == symbol:
                            dca_ativo = None
                        telegram(f"DCA cancelado e posicao fechada: {symbol} | ROI final: {roi:+.1f}%")
                except Exception as e:
                    telegram(f"Erro ao cancelar DCA {symbol}: {e}")

        elif texto.startswith("/fecharlista"):
            # Fecha múltiplas posições: /fecharlista WCTUSDT SKLUSDT BCHUSDT
            partes = texto.split()[1:]
            if not partes:
                telegram("Uso: /fecharlista SYMBOL1 SYMBOL2 SYMBOL3\nEx: /fecharlista WCTUSDT SKLUSDT BCHUSDT")
            else:
                abertas = posicoes_abertas(client)
                resultados = []
                for symbol in [p.upper() for p in partes]:
                    posicao = next((p for p in abertas if p["symbol"] == symbol), None)
                    if not posicao:
                        resultados.append(f"{symbol}: nao encontrado")
                    else:
                        try:
                            amt  = float(posicao["positionAmt"])
                            lado = "LONG" if amt > 0 else "SHORT"
                            side = "SELL" if lado == "LONG" else "BUY"
                            roi  = calcular_roi(posicao)
                            client.futures_create_order(
                                symbol=symbol, side=side, type="MARKET",
                                quantity=abs(amt), reduceOnly=True
                            )
                            resultados.append(f"{symbol}: fechado | ROI: {roi:+.1f}%")
                        except Exception as e:
                            resultados.append(f"{symbol}: erro — {e}")
                telegram("<b>Resultado:</b>\n" + "\n".join(resultados))

        elif texto.startswith("/fechar"):
            partes = texto.split()
            if len(partes) != 2:
                telegram("Uso: /fechar SYMBOL\nEx: /fechar FHEUSDT")
            else:
                symbol = partes[1].upper()
                try:
                    abertas = posicoes_abertas(client)
                    posicao = next((p for p in abertas if p["symbol"] == symbol), None)
                    if not posicao:
                        telegram(f"{symbol} nao encontrado nas posicoes abertas.")
                    else:
                        amt    = float(posicao["positionAmt"])
                        lado   = "LONG" if amt > 0 else "SHORT"
                        side   = "SELL" if lado == "LONG" else "BUY"
                        qty    = abs(amt)
                        roi    = calcular_roi(posicao)
                        client.futures_create_order(
                            symbol=symbol, side=side, type="MARKET",
                            quantity=qty, reduceOnly=True
                        )
                        telegram(f"Posicao fechada: {lado} {symbol}\nROI final: {roi:+.1f}%")
                except Exception as e:
                    telegram(f"Erro ao fechar {symbol}: {e}")

        elif texto == "/fechartudo":
            abertas = posicoes_abertas(client)
            if not abertas:
                telegram("Nenhuma posicao aberta.")
            else:
                telegram(f"Fechando {len(abertas)} posicoes...")
                resultados = []
                for p in abertas:
                    symbol = p["symbol"]
                    amt    = float(p["positionAmt"])
                    lado   = "LONG" if amt > 0 else "SHORT"
                    side   = "SELL" if lado == "LONG" else "BUY"
                    roi    = calcular_roi(p)
                    try:
                        client.futures_create_order(
                            symbol=symbol, side=side, type="MARKET",
                            quantity=abs(amt), reduceOnly=True
                        )
                        resultados.append(f"{symbol} {lado} ROI:{roi:+.1f}%")
                        time.sleep(0.5)
                    except BinanceAPIException as e:
                        resultados.append(f"{symbol}: ERRO — {e}")
                usd_brl = get_usd_brl(client)
                saldo_novo = get_saldo_total(client)
                telegram(
                    f"<b>Todas as posicoes fechadas</b>\n\n" +
                    "\n".join(resultados) +
                    f"\n\nSaldo: ${saldo_novo:.2f} (R${saldo_novo * usd_brl:.2f})"
                )

        elif texto.startswith("/forcar"):
            partes = texto.split()
            if len(partes) != 3 or partes[2] not in ("long", "short"):
                telegram("Uso: /forcar SYMBOL LONG ou /forcar SYMBOL SHORT\nEx: /forcar SOLUSDT LONG")
            else:
                symbol  = partes[1].upper()
                direcao = partes[2].upper()
                try:
                    preco = float(client.futures_symbol_ticker(symbol=symbol)["price"])
                    banca = get_banca(client)
                    telegram(f"Forcando entrada {direcao} em {symbol} a {preco}...")
                    abrir_posicao(client, symbol, direcao, preco, banca)
                except Exception as e:
                    telegram(f"Erro ao forcar {symbol}: {e}")

        elif texto == "/lucro":
            try:
                inicio_ms = int(bot_inicio * 1000)
                usd_brl   = get_usd_brl(client)
                incomes = client.futures_income_history(
                    incomeType="REALIZED_PNL",
                    startTime=inicio_ms,
                    limit=1000
                )
                lucro_total  = sum(float(i["income"]) for i in incomes)
                trades_count = len(incomes)
                lucros = [float(i["income"]) for i in incomes if float(i["income"]) > 0]
                perdas = [float(i["income"]) for i in incomes if float(i["income"]) < 0]
                brl_str    = f" (R${lucro_total * usd_brl:+.2f})" if usd_brl > 0 else ""
                inicio_str = datetime.fromtimestamp(bot_inicio).strftime("%d/%m %H:%M")

                # Projeção Bruno Aguiar: banca inicial * 1.55 ^ (ciclos/10)
                # Cada 10 ciclos = +55% sobre a banca
                saldo_total  = get_saldo_total(client)
                banca_inicio = saldo_total - lucro_total
                ciclos       = trades_count
                projecao_bruno = [
                    (0,   banca_inicio),
                    (10,  banca_inicio * (1.55 ** 1)),
                    (20,  banca_inicio * (1.55 ** 2)),
                    (30,  banca_inicio * (1.55 ** 3)),
                    (50,  banca_inicio * (1.55 ** 5)),
                    (100, banca_inicio * (1.55 ** 10)),
                    (128, banca_inicio * (1.55 ** 12.8)),
                ]
                # Acha em qual etapa da projeção está
                etapa_atual = max((c for c, _ in projecao_bruno if c <= ciclos), default=0)
                prox_etapa  = min((c for c, _ in projecao_bruno if c > ciclos), default=128)
                meta_prox   = dict(projecao_bruno)[prox_etapa]

                msg = (
                    f"<b>Lucro desde {inicio_str}</b>\n\n"
                    f"PnL realizado: {lucro_total:+.2f} USDT{brl_str}\n"
                    f"Saldo atual: ${saldo_total:.2f}\n"
                    f"Trades fechados: {ciclos}\n"
                    f"Positivos: {len(lucros)} | Negativos: {len(perdas)}\n"
                )
                if lucros:
                    msg += f"Melhor trade: +{max(lucros):.2f} USDT\n"
                if perdas:
                    msg += f"Pior trade: {min(perdas):.2f} USDT\n"

                msg += (
                    f"\n<b>Projecao Bruno Aguiar</b>\n"
                    f"Voce esta no ciclo {ciclos}\n"
                    f"Meta proxima ({prox_etapa} ciclos): ${meta_prox:.2f}\n"
                    f"Para chegar a $1M: 128 ciclos\n\n"
                    f"Projecao completa:\n"
                )
                for ciclo, valor in projecao_bruno:
                    marcador = ">>> " if ciclo == etapa_atual else "    "
                    msg += f"{marcador}{ciclo} ciclos: ${valor:.2f}\n"

                telegram(msg)
            except Exception as e:
                telegram(f"Erro ao buscar lucro: {e}")

        elif texto.startswith("/topup"):
            partes = texto.split()
            executar = len(partes) > 1 and partes[1] == "sim"
            abertas = posicoes_abertas(client)

            if not abertas:
                telegram("Nenhuma posicao aberta.")
            else:
                saldo = get_saldo_total(client)
                margem_add = saldo * 0.01  # 1% do saldo
                aptos = []
                ignorados = []

                for p in abertas:
                    symbol  = p["symbol"]
                    amt     = float(p["positionAmt"])
                    direcao = "LONG" if amt > 0 else "SHORT"
                    roi     = calcular_roi(p)

                    margem_atual = float(p.get("positionInitialMargin", 0))
                    margem_target = saldo * RISCO_POR_TRADE
                    if margem_atual >= margem_target * 0.8:
                        ignorados.append(f"  {symbol}: margem ${margem_atual:.2f} ja atualizada")
                        continue

                    if roi <= 0:
                        ignorados.append(f"  {symbol}: ROI {roi:+.1f}% (negativo)")
                        continue

                    try:
                        df5 = get_candles(client, symbol, Client.KLINE_INTERVAL_5MINUTE, limit=30)
                        df5["ma7"]  = df5["close"].rolling(7).mean()
                        df5["ma25"] = df5["close"].rolling(25).mean()
                        c1 = df5.iloc[-1]
                        ma_favor = (direcao == "LONG" and c1["ma7"] > c1["ma25"]) or \
                                   (direcao == "SHORT" and c1["ma7"] < c1["ma25"])
                    except Exception:
                        ma_favor = False

                    if not ma_favor:
                        ignorados.append(f"  {symbol}: ROI {roi:+.1f}% (MA contra)")
                        continue

                    aptos.append((p, symbol, direcao, roi, margem_add))

                if not aptos:
                    msg = "<b>Topup — nenhuma posição apta</b>\n\n"
                    if ignorados:
                        msg += "Ignoradas:\n" + "\n".join(ignorados)
                    telegram(msg)
                elif not executar:
                    # Apenas mostra — não executa
                    linhas = [f"  ✅ {s}: {d} | ROI {r:+.1f}% | +${m:.2f}" for _, s, d, r, m in aptos]
                    msg = (
                        f"<b>Topup disponível (+1% = ${margem_add:.2f}/posição)</b>\n\n"
                        f"Aptas:\n" + "\n".join(linhas)
                    )
                    if ignorados:
                        msg += "\n\nIgnoradas:\n" + "\n".join(ignorados)
                    msg += "\n\nPara executar: /topup sim"
                    telegram(msg)
                else:
                    # Executa topup
                    resultados = []
                    for p, symbol, direcao, roi, margem in aptos:
                        try:
                            # Verifica rácio antes de cada topup
                            racio_atual = get_racio_margem(client)
                            if racio_atual >= RACIO_MARGEM_MAX:
                                resultados.append(f"  ⛔ {symbol}: Racio {racio_atual:.2f}% >= limite {RACIO_MARGEM_MAX:.0f}% — topup interrompido")
                                break

                            preco     = float(client.futures_ticker(symbol=symbol)["lastPrice"])
                            alav      = alavancagem_dinamica(saldo)
                            step      = get_step_size(client, symbol)
                            qty_add   = arredondar_quantidade((margem * alav) / preco, step)
                            if qty_add <= 0:
                                resultados.append(f"  {symbol}: quantidade muito pequena")
                                continue
                            side = "BUY" if direcao == "LONG" else "SELL"
                            if MODO == "real":
                                client.futures_create_order(
                                    symbol=symbol, side=side,
                                    type="MARKET", quantity=qty_add
                                )
                            resultados.append(f"  ✅ {symbol}: +{qty_add} ({direcao}) | ROI {roi:+.1f}%")
                        except Exception as e:
                            resultados.append(f"  ❌ {symbol}: erro — {e}")

                    msg = "<b>Topup executado</b>\n\n" + "\n".join(resultados)
                    telegram(msg)
                    log.info(f"Topup executado: {len(aptos)} posicoes")

        elif texto == "/ajuda":
            telegram(
                "<b>Comandos disponíveis:</b>\n"
                "/status — posicoes abertas com PnL\n"
                "/lucro — PnL realizado desde que o bot iniciou\n"
                "/saldo — saldo disponivel\n"
                "/topup — mostra posicoes aptas para topup (ROI+ e MA a favor)\n"
                "/topup sim — executa topup em todas as posicoes aptas\n"
                "/dca SYMBOL — DCA manual (verifica MA antes)\n"
                "/dca SYMBOL forcado — DCA sem verificar MA\n"
                "/canceldca SYMBOL — cancela DCA e fecha posicao\n"
                "/fechar SYMBOL — fecha posicao 100%\n"
                "/fechartudo — fecha todas as posicoes abertas\n"
                "/fecharlista S1 S2 S3 — fecha multiplas posicoes\n"
                "/forcar SYMBOL LONG|SHORT — abre posicao manualmente\n"
                "/parar — encerra o robo\n"
                "/iniciar — verifica se esta ativo"
            )


# ---------------------------------------------------------------------------
# Horários de sessão
# ---------------------------------------------------------------------------
def horario_asiatico() -> bool:
    """00h-08h UTC = 21h-05h BRT. Melhor sessão para a estratégia MA."""
    hora = datetime.now(timezone.utc).hour
    return 0 <= hora < 8

def horario_occidental() -> bool:
    """17h-21h UTC = 14h-18h BRT. Abertura americana — maior ruído."""
    hora = datetime.now(timezone.utc).hour
    return 17 <= hora < 21

def sessao_atual() -> str:
    if horario_asiatico():
        return "ASIATICA"
    if horario_occidental():
        return "AMERICANA"
    return "EUROPEIA"

# ---------------------------------------------------------------------------
# Dados de mercado
# ---------------------------------------------------------------------------
def get_candles(client: Client, symbol: str, interval: str, limit: int = 100) -> pd.DataFrame:
    klines = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
    df = pd.DataFrame(klines, columns=[
        "time", "open", "high", "low", "close", "volume",
        "close_time", "qav", "trades", "tbav", "tqav", "ignore"
    ])
    df["close"]  = df["close"].astype(float)
    df["high"]   = df["high"].astype(float)
    df["low"]    = df["low"].astype(float)
    df["volume"] = df["volume"].astype(float)
    return df


# ---------------------------------------------------------------------------
# Copy trading: consulta conta master
# ---------------------------------------------------------------------------
_master_client = None
_master_cache: dict[str, dict] = {}   # symbol -> {"direcao": "LONG"/"SHORT", "roi": float}
_master_cache_ts: float = 0
_bruno_alertados: dict[str, float] = {}  # symbol -> timestamp do último alerta Bruno


def detectar_sinais_bruno(client: Client, simbolos_abertos: list[str]) -> list[tuple]:
    """
    Modo Bruno: detecta volume anormal nos ativos favoritos do Bruno.
    Só opera LONG. Foca no horário 01:00-09:00 BRT.
    Retorna lista de (symbol, "LONG", "alta", preco, "BRUNO").
    """
    global _bruno_alertados
    hora_local = datetime.now().hour

    sinais = []
    for symbol in PARES_BRUNO:
        if symbol in simbolos_abertos:
            continue
        # Não repetir alerta do mesmo ativo em menos de 4 horas
        if symbol in _bruno_alertados and time.time() - _bruno_alertados[symbol] < 14400:
            continue

        try:
            # Pega candles de 1H para comparar volume
            df = get_candles(client, symbol, Client.KLINE_INTERVAL_1HOUR, limit=25)
            vol_media = df["volume"].iloc[:-1].mean()  # média das últimas 24h (excluindo atual)
            vol_atual = df["volume"].iloc[-1]

            if vol_media <= 0:
                continue

            # Volume spike: atual >= 3x a média
            vol_ratio = vol_atual / vol_media
            if vol_ratio < BRUNO_VOLUME_MULT:
                continue

            # Confirma que preço está acima da MA25 no 1H (tendência de alta)
            df["ma7"]  = df["close"].rolling(7).mean()
            df["ma25"] = df["close"].rolling(25).mean()
            ma7  = df["ma7"].iloc[-1]
            ma25 = df["ma25"].iloc[-1]
            preco = df["close"].iloc[-1]

            if ma7 <= ma25:
                continue  # só LONG quando MA7 > MA25 no 1H

            if preco <= ma7:
                continue  # preço tem que estar acima da MA7

            preco_ticker = float(client.futures_symbol_ticker(symbol=symbol)["price"])

            # Prioriza horário Bruno (01-09h BRT) mas aceita fora também se volume for muito alto
            if BRUNO_HORARIO_INICIO <= hora_local <= BRUNO_HORARIO_FIM:
                min_vol = BRUNO_VOLUME_MULT
            else:
                min_vol = BRUNO_VOLUME_MULT * 2  # fora do horário, exige 6x

            if vol_ratio >= min_vol:
                sinais.append((symbol, "LONG", "alta", preco_ticker, "BRUNO"))
                _bruno_alertados[symbol] = time.time()
                log.info(f"  [BRUNO] {symbol}: volume {vol_ratio:.1f}x a media | MA7 > MA25 1H | LONG")
                telegram(
                    f"<b>[BRUNO] Sinal detectado: {symbol}</b>\n"
                    f"Volume: {vol_ratio:.1f}x a media horaria\n"
                    f"MA7 > MA25 no 1H | Preco: {preco_ticker}\n"
                    f"Padrao de acumulacao detectado."
                )

        except Exception:
            continue

    return sinais

def get_master_positions() -> dict[str, dict]:
    """
    Retorna posições abertas da conta master com ROI positivo.
    Cache de 60 segundos para não sobrecarregar a API.
    Retorna {} se MASTER_API_KEY não configurado.
    """
    global _master_client, _master_cache, _master_cache_ts
    if not MASTER_API_KEY or not MASTER_API_SECRET:
        return {}

    if time.time() - _master_cache_ts < 60:
        return _master_cache

    try:
        if _master_client is None:
            _master_client = Client(MASTER_API_KEY, MASTER_API_SECRET)

        posicoes = _master_client.futures_position_information()
        resultado = {}
        for p in posicoes:
            amt = float(p["positionAmt"])
            if amt == 0:
                continue
            symbol = p["symbol"]
            direcao = "LONG" if amt > 0 else "SHORT"
            pnl = float(p.get("unrealizedProfit", p.get("unRealizedProfit", 0)))
            margem = float(p.get("initialMargin", p.get("positionInitialMargin", 0)))
            roi = (pnl / margem * 100) if margem > 0 else 0
            if roi > 0:  # só copia posições positivas
                resultado[symbol] = {"direcao": direcao, "roi": roi}

        _master_cache = resultado
        _master_cache_ts = time.time()
        return resultado
    except Exception as e:
        log.debug(f"Erro ao consultar master: {e}")
        return _master_cache  # retorna cache anterior se der erro


def get_top_pares(client: Client, n: int = TOP_PARES) -> list[str]:
    """
    Lista híbrida: pares de alto volume operacional primeiro, voláteis como complemento.
    Pares líquidos têm padrões técnicos mais confiáveis e menor slippage.
    """
    EXCLUIR = {"BTCUSDT", "ETHUSDT", "XAGUSDT", "XAUUSDT", "BTCDOMUSDT", "DEFIUSDT"}

    # Pares prioritários — alta liquidez e volume operacional consistente
    PARES_PRIORITARIOS = [
        "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT",
        "AVAXUSDT", "LINKUSDT", "DOTUSDT", "NEARUSDT", "SUIUSDT",
        "APTUSDT", "INJUSDT", "ARBUSDT", "OPUSDT", "ATOMUSDT",
        "LTCUSDT", "BCHUSDT", "ETCUSDT", "FILUSDT", "ICPUSDT",
        "AAVEUSDT", "UNIUSDT", "CRVUSDT", "MKRUSDT", "SNXUSDT",
        "TRXUSDT", "XLMUSDT", "VETUSDT", "HBARUSDT", "TONUSDT",
    ]

    tickers = client.futures_ticker()
    ticker_map = {t["symbol"]: t for t in tickers if t["symbol"].endswith("USDT")}

    # Filtra ilíquidos (mínimo 5M USDT de volume nas 24h)
    VOLUME_MINIMO = 5_000_000

    # 1. Prioritários disponíveis com volume suficiente
    prioritarios = [
        s for s in PARES_PRIORITARIOS
        if s in ticker_map
        and s not in EXCLUIR
        and float(ticker_map[s]["quoteVolume"]) >= VOLUME_MINIMO
    ]

    # 2. Complementares — voláteis que não estão na lista prioritária
    complementares = [
        t for t in tickers
        if t["symbol"].endswith("USDT")
        and t["symbol"] not in EXCLUIR
        and t["symbol"] not in PARES_PRIORITARIOS
        and float(t["quoteVolume"]) >= VOLUME_MINIMO
    ]
    for t in complementares:
        variacao    = abs(float(t["priceChangePercent"]))
        vol_24h     = float(t["quoteVolume"])
        t["_score"] = variacao * (vol_24h / 1_000_000)
    complementares.sort(key=lambda x: x["_score"], reverse=True)
    complementares_symbols = [t["symbol"] for t in complementares]

    # Reordena TODOS por variação 24h (maior movimento = mais momentum)
    todos = prioritarios + [s for s in complementares_symbols if s not in prioritarios]
    todos_com_var = []
    for s in todos:
        if s in ticker_map:
            var = abs(float(ticker_map[s]["priceChangePercent"]))
            todos_com_var.append((s, var))
    todos_com_var.sort(key=lambda x: x[1], reverse=True)

    # Top movers primeiro — são os que têm tendência real
    pares = [s for s, v in todos_com_var[:n]]
    top5 = [(s, v) for s, v in todos_com_var[:5]]
    top5_txt = " | ".join([f"{s} {v:.1f}%" for s, v in top5])
    log.info(f"Pares selecionados: {len(pares)} | Top movers: {top5_txt}")
    return pares


def tendencia_btc(client: Client) -> str:
    """
    Retorna a tendência do BTC no 5min como referência do mercado.
    'alta' → prioriza LONG nas alts
    'baixa' → prioriza SHORT nas alts
    'lateral' → opera nos dois lados
    """
    df = get_candles(client, "BTCUSDT", Client.KLINE_INTERVAL_5MINUTE, limit=30)
    df["ma7"]  = df["close"].rolling(7).mean()
    df["ma25"] = df["close"].rolling(25).mean()
    ultima = df.iloc[-1]
    diff_pct = abs(ultima["ma7"] - ultima["ma25"]) / ultima["ma25"] * 100
    if diff_pct < 0.1:  # MAs muito próximas = lateral
        return "lateral"
    if ultima["ma7"] > ultima["ma25"]:
        return "alta"
    return "baixa"


def tendencia_1h(client: Client, symbol: str) -> tuple[str, bool]:
    """
    Retorna ('alta'|'baixa', confirmado).
    confirmado = True se MA7 e MA25 estão claramente separadas no 1H (spread >= 0.3%).
    Usado no modo swing: 1H (direção) → 5min (alinhamento) → M1 (gatilho)
    """
    df = get_candles(client, symbol, Client.KLINE_INTERVAL_1HOUR, limit=30)
    df["ma7"]  = df["close"].rolling(7).mean()
    df["ma25"] = df["close"].rolling(25).mean()
    ultima = df.iloc[-1]
    direcao = "alta" if ultima["ma7"] > ultima["ma25"] else "baixa"
    spread = abs(ultima["ma7"] - ultima["ma25"]) / ultima["ma25"] if ultima["ma25"] > 0 else 0
    confirmado = spread >= 0.003
    return direcao, confirmado


def tendencia_5min(client: Client, symbol: str) -> tuple[str, bool]:
    """
    Retorna ('alta'|'baixa', confirmado).
    confirmado = True se MA7 e MA25 estão separadas no 5min (spread >= 0.2%).
    Usado no modo scalping: 5min (direção) → M1 (gatilho)
    Spread menor (0.2%) que 1H (0.3%) para capturar mais sinais rápidos.
    """
    df = get_candles(client, symbol, Client.KLINE_INTERVAL_5MINUTE, limit=30)
    df["ma7"]  = df["close"].rolling(7).mean()
    df["ma25"] = df["close"].rolling(25).mean()
    ultima = df.iloc[-1]
    direcao = "alta" if ultima["ma7"] > ultima["ma25"] else "baixa"
    spread = abs(ultima["ma7"] - ultima["ma25"]) / ultima["ma25"] if ultima["ma25"] > 0 else 0
    confirmado = spread >= 0.002
    return direcao, confirmado


# Controle de DCA, trailing stop e alertas
dca_log:          dict[str, float] = {}
dca_ativo:        str | None = None
dca_aplicado:     set = set()       # symbols com pelo menos 1 DCA
dca_contagem:     dict[str, int] = {}  # quantos 3x já foram aplicados por symbol
peak_roi:         dict[str, float] = {}
roi_anterior:     dict[str, float] = {}   # ROI do ciclo anterior para detectar queda acelerada
alerta_liq_log:   dict[str, float] = {}   # timestamp do último alerta de liquidação por symbol
alerta_dca_log:   dict[str, float] = {}   # timestamp do último alerta de aproximação do DCA
posicao_abertura: dict[str, float] = {}   # timestamp de quando cada posição foi detectada
ma_reverteu:      dict[str, float] = {}   # symbol -> ROI no momento em que a MA reverteu contra a posição
alerta_80_log:         dict[str, float] = {}   # timestamp do último alerta de -80% por symbol
sinal_ma_detectado:    dict[str, float] = {}   # timestamp da última vez que MA cruzou a favor por symbol
roi_no_dca:            dict[str, float] = {}   # ROI no momento em que o DCA foi aplicado
alerta_ciclo_risco_ts: float           = 0.0  # timestamp do último alerta de ciclo em risco
posicoes_herdadas:     set[str]        = set() # symbols herdados de ciclos anteriores (negativos não fechados)
margem_registrada:     dict[str, float] = {}  # margem inicial registrada por symbol para detectar DCA manual
topup_recente:         dict[str, float] = {}  # symbols que tiveram topup recente (ignora na detecção DCA)
posicoes_bruno:        set[str]        = set() # symbols que entraram pelo modo Bruno
parcial_500:           set[str]        = set() # symbols que já tiveram saída parcial em +500%

ESTADO_FILE = "C:/robo-trade/estado_bot.json"

def meta_formiguinha(ciclos_positivos: int) -> float:
    """
    Plano formiguinha: meta progressiva baseada em ciclos consecutivos positivos.
    Começa em 2%, sobe 0.5% a cada 5 ciclos positivos, teto em 4%.
    """
    incrementos = ciclos_positivos // 5
    meta = 2.0 + incrementos * 0.5
    return min(meta, 4.0)


def salvar_estado(ciclo_num=None, saldo_ciclo_inicio=None, ciclos_positivos=None):
    """Persiste peak_roi, dca_aplicado, herdadas e controle de ciclo em disco."""
    try:
        dados = {
            "peak_roi": peak_roi,
            "dca_aplicado": list(dca_aplicado),
            "dca_contagem": dca_contagem,
            "margem_registrada": margem_registrada,
            "posicoes_herdadas": list(posicoes_herdadas),
        }
        if ciclo_num is not None:
            dados["ciclo_num"] = ciclo_num
        if saldo_ciclo_inicio is not None:
            dados["saldo_ciclo_inicio"] = saldo_ciclo_inicio
        if ciclos_positivos is not None:
            dados["ciclos_positivos"] = ciclos_positivos
        with open(ESTADO_FILE, "w") as f:
            json.dump(dados, f)
    except Exception as e:
        log.warning(f"Erro ao salvar estado: {e}")

def analise_grafico_3x(client: Client, symbol: str, direcao: str) -> str:
    """
    Gera análise detalhada de 1min e 5min para acompanhar o 3x.
    Retorna texto formatado para Telegram.
    """
    try:
        # 5min
        df5 = get_candles(client, symbol, Client.KLINE_INTERVAL_5MINUTE, limit=30)
        df5["ma7"]  = df5["close"].rolling(7).mean()
        df5["ma25"] = df5["close"].rolling(25).mean()
        ma7_5  = df5["ma7"].iloc[-1]
        ma25_5 = df5["ma25"].iloc[-1]
        diff_5 = ma7_5 - ma25_5
        preco  = df5["close"].iloc[-1]
        trend_5 = "subindo" if diff_5 > 0 else "caindo"

        # 1min
        df1 = get_candles(client, symbol, Client.KLINE_INTERVAL_1MINUTE, limit=30)
        df1["ma7"]  = df1["close"].rolling(7).mean()
        df1["ma25"] = df1["close"].rolling(25).mean()
        ma7_1  = df1["ma7"].iloc[-1]
        ma25_1 = df1["ma25"].iloc[-1]
        diff_1 = ma7_1 - ma25_1
        trend_1 = "subindo" if diff_1 > 0 else "caindo"

        # Fibonacci
        high_20 = df5["high"].iloc[-20:].max()
        low_20  = df5["low"].iloc[-20:].min()
        fib_382 = low_20 + (high_20 - low_20) * 0.382
        fib_500 = low_20 + (high_20 - low_20) * 0.500
        fib_618 = low_20 + (high_20 - low_20) * 0.618

        return (
            f"\n<b>Grafico 5min:</b>\n"
            f"  MA7: {ma7_5:.6f} | MA25: {ma25_5:.6f}\n"
            f"  MA7 {'>' if ma7_5 > ma25_5 else '<'} MA25 ({trend_5})\n"
            f"\n<b>Grafico 1min:</b>\n"
            f"  MA7: {ma7_1:.6f} | MA25: {ma25_1:.6f}\n"
            f"  MA7 {'>' if ma7_1 > ma25_1 else '<'} MA25 ({trend_1})\n"
            f"\n<b>Fibonacci:</b>\n"
            f"  38.2%: {fib_382:.6f}\n"
            f"  50.0%: {fib_500:.6f}\n"
            f"  61.8%: {fib_618:.6f}\n"
            f"  Preco: {preco:.6f}"
        )
    except Exception:
        return "\n(erro ao gerar analise de grafico)"


def carregar_estado():
    """Restaura peak_roi, dca_aplicado, herdadas e ciclo do disco ao iniciar."""
    global peak_roi, dca_aplicado, dca_contagem, margem_registrada, posicoes_herdadas
    try:
        with open(ESTADO_FILE, "r") as f:
            dados = json.load(f)
        peak_roi          = dados.get("peak_roi", {})
        dca_aplicado      = set(dados.get("dca_aplicado", []))
        dca_contagem      = dados.get("dca_contagem", {})
        margem_registrada = dados.get("margem_registrada", {})
        posicoes_herdadas = set(dados.get("posicoes_herdadas", []))
        ciclo_salvo          = dados.get("ciclo_num", 1)
        saldo_salvo          = dados.get("saldo_ciclo_inicio", None)
        ciclos_pos_salvo     = dados.get("ciclos_positivos", 0)
        log.info(f"Estado restaurado: {len(peak_roi)} picos | {len(dca_aplicado)} DCAs | Ciclo {ciclo_salvo} | Herdadas: {len(posicoes_herdadas)} | Ciclos positivos: {ciclos_pos_salvo}")
        if dca_aplicado:
            for s in dca_aplicado:
                n = dca_contagem.get(s, 1)
                log.info(f"  3x ativo: {s} (#{n})")
            telegram(
                f"<b>3x ativos restaurados:</b>\n"
                + "\n".join([f"  {s} (#{dca_contagem.get(s, 1)})" for s in dca_aplicado])
                + "\nBot vai monitorar saida inteligente (+3%/+10%)."
            )
        return ciclo_salvo, saldo_salvo, ciclos_pos_salvo
    except FileNotFoundError:
        return 1, None, 0
    except Exception as e:
        log.warning(f"Erro ao carregar estado: {e}")
        return 1, None, 0

def calcular_roi(posicao: dict) -> float:
    """
    Calcula ROI igual ao exibido no app da Binance.
    Usa positionInitialMargin diretamente da API de conta.
    """
    pnl    = float(posicao.get("unrealizedProfit", 0))
    margem = float(posicao.get("positionInitialMargin", 0))
    if margem == 0:
        return 0.0
    return (pnl / margem) * 100


def ma_cruza_favor(client: Client, symbol: str, direcao: str) -> bool:
    """
    Critério do Bruno para 3x — compara 1min E 5min antes de aplicar:
    1. 5min: MA7 cruzou MA25 (confirmado, separando, não lateral)
    2. 1min: MA7 TAMBÉM acima da MA25 (ambos timeframes concordam)
    3. Fibonacci: preço acima de 38.2% do swing (tendência)
    Só aplica 3x quando 1min E 5min estão alinhados.
    """
    # --- 5min: cruzamento confirmado ---
    df5 = get_candles(client, symbol, Client.KLINE_INTERVAL_5MINUTE, limit=50)
    df5["ma7"]  = df5["close"].rolling(7).mean()
    df5["ma25"] = df5["close"].rolling(25).mean()
    c4 = df5.iloc[-4]
    c3 = df5.iloc[-3]
    c2 = df5.iloc[-2]
    c1 = df5.iloc[-1]

    if direcao == "LONG":
        cruzou_5m = c4["ma7"] <= c4["ma25"] and c3["ma7"] > c3["ma25"]
        confirmou_5m = c2["ma7"] > c2["ma25"] and (c2["ma7"] - c2["ma25"]) > (c3["ma7"] - c3["ma25"])
        nao_lateral_5m = (c1["ma7"] - c1["ma25"]) >= (c2["ma7"] - c2["ma25"])
        alinhado_5m = c1["ma7"] > c1["ma25"]  # MA7 acima agora
    else:
        cruzou_5m = c4["ma7"] >= c4["ma25"] and c3["ma7"] < c3["ma25"]
        confirmou_5m = c2["ma7"] < c2["ma25"] and (c2["ma25"] - c2["ma7"]) > (c3["ma25"] - c3["ma7"])
        nao_lateral_5m = (c1["ma25"] - c1["ma7"]) >= (c2["ma25"] - c2["ma7"])
        alinhado_5m = c1["ma7"] < c1["ma25"]

    # 5min precisa ter cruzado OU estar alinhado (cruzamento pode ter sido há alguns candles)
    ok_5m = (cruzou_5m and confirmou_5m and nao_lateral_5m) or alinhado_5m

    if not ok_5m:
        return False

    # --- 1min: MA7 também alinhada na mesma direção ---
    df1 = get_candles(client, symbol, Client.KLINE_INTERVAL_1MINUTE, limit=30)
    df1["ma7"]  = df1["close"].rolling(7).mean()
    df1["ma25"] = df1["close"].rolling(25).mean()
    m1 = df1.iloc[-1]
    m2 = df1.iloc[-2]

    if direcao == "LONG":
        alinhado_1m = m1["ma7"] > m1["ma25"]
        acelerando_1m = (m1["ma7"] - m1["ma25"]) > (m2["ma7"] - m2["ma25"])
    else:
        alinhado_1m = m1["ma7"] < m1["ma25"]
        acelerando_1m = (m1["ma25"] - m1["ma7"]) > (m2["ma25"] - m2["ma7"])

    if not alinhado_1m:
        return False  # 1min não confirma — espera

    # --- Fibonacci: confirma tendência ---
    high_20 = df5["high"].iloc[-20:].max()
    low_20  = df5["low"].iloc[-20:].min()
    fib_382 = low_20 + (high_20 - low_20) * 0.382
    fib_618 = low_20 + (high_20 - low_20) * 0.618
    preco   = df5["close"].iloc[-1]

    if direcao == "LONG":
        fib_ok = preco >= fib_382
    else:
        fib_ok = preco <= fib_618

    if not fib_ok:
        return False

    log.info(f"  {symbol}: 3x confirmado | 5min MA7{'>' if direcao=='LONG' else '<'}MA25 | 1min MA7{'>' if direcao=='LONG' else '<'}MA25{' acelerando' if acelerando_1m else ''} | Fib OK")
    return True


def dca_ativo_tem_sinal(client: Client, abertas: list) -> bool:
    """
    Verifica se o dca_ativo atual ainda tem sinal de recuperação ativo.
    Se MA7 não está se aproximando de MA25 na direção favorável, o bloqueio
    da fila é liberado para quem tiver sinal.
    """
    if not dca_ativo:
        return False
    posicao = next((p for p in abertas if p["symbol"] == dca_ativo), None)
    if not posicao:
        return False
    direcao = "LONG" if float(posicao["positionAmt"]) > 0 else "SHORT"
    try:
        df = get_candles(client, dca_ativo, Client.KLINE_INTERVAL_5MINUTE, limit=30)
        df["ma7"]  = df["close"].rolling(7).mean()
        df["ma25"] = df["close"].rolling(25).mean()
        c1 = df.iloc[-1]
        # Sinal ativo = MA7 já está do lado correto (em recuperação)
        if direcao == "LONG":
            return c1["ma7"] > c1["ma25"]
        else:
            return c1["ma7"] < c1["ma25"]
    except Exception:
        return False


def rsi_extremo(client: Client, symbol: str, direcao: str, periodo: int = 14) -> bool:
    """
    Retorna True se RSI estiver em extremo favorável ao DCA:
    - LONG: RSI < 30 (sobrevendido — reversão de alta provável)
    - SHORT: RSI > 70 (sobrecomprado — reversão de queda provável)
    """
    df = get_candles(client, symbol, Client.KLINE_INTERVAL_5MINUTE, limit=periodo + 10)
    delta = df["close"].diff()
    gain  = delta.clip(lower=0).rolling(periodo).mean()
    loss  = (-delta.clip(upper=0)).rolling(periodo).mean()
    rs    = gain / loss.replace(0, float("inf"))
    rsi   = 100 - (100 / (1 + rs))
    rsi_atual = rsi.iloc[-1]
    if direcao == "LONG":
        return rsi_atual < 30
    else:
        return rsi_atual > 70


_cache_precisao: dict[str, int] = {}  # cache de precisão por symbol

def get_precisao_quantidade(client: Client, symbol: str) -> int:
    """
    Retorna a precisão de quantidade (casas decimais) exigida pela Binance para o par.
    Usa cache para não chamar a API a cada ordem.
    """
    if symbol in _cache_precisao:
        return _cache_precisao[symbol]
    try:
        info = client.futures_exchange_info()
        for s in info["symbols"]:
            if s["symbol"] == symbol:
                precisao = int(s.get("quantityPrecision", 3))
                _cache_precisao[symbol] = precisao
                return precisao
    except Exception:
        pass
    return 3  # fallback seguro


def aplicar_dca(client: Client, posicao: dict, banca: float) -> None:
    """
    Estratégia 3x (padrão Bruno): escala o 3x pela profundidade da perda.
    Quanto mais negativo, mais agressivo — para recuperar rápido.
    """
    symbol       = posicao["symbol"]
    amt          = float(posicao["positionAmt"])
    direcao      = "LONG" if amt > 0 else "SHORT"
    entry        = float(posicao["entryPrice"])
    amt_abs      = abs(float(posicao["positionAmt"]))
    leverage     = float(posicao.get("leverage", 20))
    margem_atual = round((entry * amt_abs) / leverage, 2)
    roi          = calcular_roi(posicao)

    # 3x escalado pela profundidade (padrão Bruno)
    # Quanto mais negativo, mais agressivo para sair rápido
    if roi <= -1000:
        # Audacioso: usa 80% da margem disponível
        adicional = round(banca * 0.80, 2)
        modo_3x = "AUDACIOSO (80% banca)"
    elif roi <= -500:
        # Agressivo: usa 40% da margem disponível
        adicional = round(banca * 0.40, 2)
        modo_3x = "AGRESSIVO (40% banca)"
    else:
        # Normal: triplica a margem atual
        adicional = round(margem_atual * 2.0, 2)
        modo_3x = "NORMAL (3x margem)"
        # Limita a 40% do saldo
        if adicional > banca * 0.40:
            adicional = round(banca * 0.40, 2)
            modo_3x = "NORMAL (max 40% banca)"

    log.info(f"  {symbol}: 3x {modo_3x} | ROI {roi:+.1f}% | Adicional: ${adicional:.2f}")

    preco      = float(client.futures_symbol_ticker(symbol=symbol)["price"])
    precisao   = get_precisao_quantidade(client, symbol)
    quantidade = round((adicional * ALAVANCAGEM) / preco, precisao)
    side       = "BUY" if direcao == "LONG" else "SELL"

    if quantidade <= 0:
        log.warning(f"  DCA {symbol} bloqueado: quantidade={quantidade} (saldo insuficiente para ordem minima)")
        return

    if MODO == "simulacao":
        log.info(f"[DCA SIMULACAO] {symbol} | +${adicional:.2f} margem (35%) | Qtd: {quantidade}")
        dca_aplicado.add(symbol)
        dca_log[symbol] = time.time()
        salvar_estado()
        return

    try:
        client.futures_create_order(symbol=symbol, side=side, type="MARKET", quantity=quantidade)
        msg = (
            f"<b>DCA aplicado!</b>\n"
            f"{direcao} {symbol}\n"
            f"Margem adicional: ${adicional:.2f} USDT (3x)\n"
            f"Quantidade: {quantidade}"
        )
        log.info(msg.replace("<b>", "").replace("</b>", ""))
        telegram(msg)
        dca_aplicado.add(symbol)
        dca_log[symbol] = time.time()
        roi_no_dca[symbol] = calcular_roi(posicao)  # registra ROI para stop pós-DCA -30%
        salvar_estado()
    except BinanceAPIException as e:
        log.error(f"Erro DCA {symbol}: {e}")
        # Não marca como dca_ativo — a ordem não foi executada
        dca_aplicado.discard(symbol)


def fechar_parcial(client: Client, posicao: dict, pct: float, motivo: str) -> None:
    """Fecha X% da posição a mercado."""
    symbol     = posicao["symbol"]
    amt        = float(posicao["positionAmt"])
    direcao    = "LONG" if amt > 0 else "SHORT"
    roi        = calcular_roi(posicao)
    step       = get_step_size(client, symbol)
    qty_fechar = arredondar_quantidade(abs(amt) * pct, step)
    side_close = "SELL" if direcao == "LONG" else "BUY"

    if MODO == "simulacao":
        log.info(f"[SAIDA {int(pct*100)}% SIMULACAO] {symbol} | ROI: {roi:.1f}% | {motivo}")
        return

    try:
        client.futures_create_order(
            symbol=symbol, side=side_close, type="MARKET",
            quantity=qty_fechar, reduceOnly=True,
        )
        pnl = float(posicao.get("unrealizedProfit", posicao.get("unRealizedProfit", 0)))
        msg = (
            f"<b>Saida {int(pct*100)}% — {motivo}</b>\n"
            f"{symbol} | {direcao} | ROI: {roi:+.1f}%\n"
            f"Lucro estimado: {pnl * pct:+.2f} USDT\n"
            f"Quantidade fechada: {qty_fechar}"
        )
        log.info(msg.replace("<b>", "").replace("</b>", ""))
        telegram(msg)
        # Se fechou 100% e era herdada, remove da lista
        if pct >= 1.0:
            posicoes_herdadas.discard(symbol)
            salvar_estado()
    except BinanceAPIException as e:
        log.error(f"Erro saida parcial {symbol}: {e}")


def verificar_alertas_risco(client: Client, posicao: dict, roi: float) -> None:
    """Verifica e envia alertas de risco para posições abertas."""
    symbol    = posicao["symbol"]
    amt       = float(posicao["positionAmt"])
    direcao   = "LONG" if amt > 0 else "SHORT"
    liq_price = float(posicao.get("liquidationPrice", 0))
    mark      = float(posicao.get("markPrice", 0))

    # 1. Queda acelerada de ROI (mais de 30% em um ciclo de 15s)
    roi_prev = roi_anterior.get(symbol)
    if roi_prev is not None:
        queda = roi_prev - roi
        if queda >= 30:
            if roi > 0:
                # Queda em posição positiva — trailing provavelmente agindo
                msg = (
                    f"Trailing ativo: {symbol}\n"
                    f"ROI {roi_prev:+.1f}% -> {roi:+.1f}%\n"
                    f"Queda de {queda:.1f}% em 15s — fechamento proximo"
                )
                log.info(msg)
                telegram(f"<b>{msg}</b>")
            else:
                # Queda em posição negativa — situação de risco
                msg = (
                    f"ALERTA: queda acelerada!\n"
                    f"{symbol} | ROI {roi_prev:+.1f}% -> {roi:+.1f}%\n"
                    f"Queda de {queda:.1f}% em 15 segundos"
                )
                log.warning(msg)
                telegram(f"<b>{msg}</b>")
    roi_anterior[symbol] = roi

    # 2. Preço próximo da liquidação (menos de 15%)
    if liq_price > 0 and mark > 0:
        dist_liq = abs(mark - liq_price) / mark * 100
        ultimo_alerta = alerta_liq_log.get(symbol, 0)
        if dist_liq <= 15 and time.time() - ultimo_alerta >= 300:
            msg = (
                f"URGENTE: liquidacao proxima!\n"
                f"{symbol} | {direcao}\n"
                f"Preco atual: {mark} | Liquidacao: {liq_price}\n"
                f"Distancia: {dist_liq:.1f}%"
            )
            log.warning(msg)
            telegram(f"<b>{msg}</b>")
            alerta_liq_log[symbol] = time.time()

    # 3. MA7 acelerando contra a posição no 5min
    try:
        df = get_candles(client, symbol, Client.KLINE_INTERVAL_5MINUTE, limit=30)
        df["ma7"]  = df["close"].rolling(7).mean()
        df["ma25"] = df["close"].rolling(25).mean()
        df["diff"] = df["ma7"] - df["ma25"]

        # Verifica se a distância entre MAs está crescendo na direção errada
        diff_atual = df["diff"].iloc[-1]
        diff_prev  = df["diff"].iloc[-3]

        acelerando_contra = (
            (direcao == "LONG" and diff_atual < diff_prev < 0) or
            (direcao == "SHORT" and diff_atual > diff_prev > 0)
        )
        if acelerando_contra and roi <= -50:
            log.info(f"  {symbol}: MA acelerando contra {direcao} | ROI {roi:+.1f}%")
    except Exception:
        pass


def ma_reverteu_contra(client: Client, symbol: str, direcao: str) -> bool:
    """Detecta se MA7 cruzou MA25 contra a direção da posição no 5min."""
    df = get_candles(client, symbol, Client.KLINE_INTERVAL_5MINUTE, limit=30)
    df["ma7"]  = df["close"].rolling(7).mean()
    df["ma25"] = df["close"].rolling(25).mean()
    prev  = df.iloc[-2]
    atual = df.iloc[-1]
    if direcao == "LONG":
        # MA7 cruzou MA25 para baixo = reversão contra LONG
        return prev["ma7"] >= prev["ma25"] and atual["ma7"] < atual["ma25"]
    else:
        # MA7 cruzou MA25 para cima = reversão contra SHORT
        return prev["ma7"] <= prev["ma25"] and atual["ma7"] > atual["ma25"]


def ma_alinhada_5min(client: Client, symbol: str, direcao: str) -> bool:
    """Verifica se MA7 está alinhada com a direção no 5min."""
    df = get_candles(client, symbol, Client.KLINE_INTERVAL_5MINUTE, limit=30)
    df["ma7"]  = df["close"].rolling(7).mean()
    df["ma25"] = df["close"].rolling(25).mean()
    atual = df.iloc[-1]
    if direcao in ("alta", "LONG"):
        return atual["ma7"] > atual["ma25"]
    return atual["ma7"] < atual["ma25"]


def ma_alinhada_15min(client: Client, symbol: str, direcao: str) -> bool:
    """Verifica se MA7 está alinhada com a direção no 15min (filtro intermediário)."""
    df = get_candles(client, symbol, Client.KLINE_INTERVAL_15MINUTE, limit=30)
    df["ma7"]  = df["close"].rolling(7).mean()
    df["ma25"] = df["close"].rolling(25).mean()
    atual = df.iloc[-1]
    if direcao in ("alta", "LONG"):
        return atual["ma7"] > atual["ma25"]
    return atual["ma7"] < atual["ma25"]


def calcular_adx(df: pd.DataFrame, periodo: int = 14) -> float:
    """
    Calcula o ADX (Average Directional Index) — mede a força da tendência.
    ADX > 20: mercado em tendência (entradas válidas).
    ADX < 20: mercado lateral (MAs dão sinais falsos — evitar entrar).
    """
    high  = df["high"]
    low   = df["low"]
    close = df["close"]

    plus_dm  = high.diff()
    minus_dm = low.diff().abs()
    plus_dm  = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0.0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)

    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low  - close.shift()).abs()
    ], axis=1).max(axis=1)

    atr       = tr.ewm(span=periodo, adjust=False).mean()
    plus_di   = 100 * plus_dm.ewm(span=periodo, adjust=False).mean()  / atr.replace(0, float("nan"))
    minus_di  = 100 * minus_dm.ewm(span=periodo, adjust=False).mean() / atr.replace(0, float("nan"))
    dx        = (100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, float("nan")))
    adx       = dx.ewm(span=periodo, adjust=False).mean()
    return float(adx.iloc[-1]) if not adx.empty else 0.0


def sinal_aguia_spread(client: Client, symbol: str) -> str | None:
    """
    Replica do sistema Águia Spread (Bruno Aguiar):
    Detecta Bollinger Squeeze (compressão de volatilidade) + release.

    Fluxo:
    1. Bandas de Bollinger se contraem (bandwidth no mínimo de 20 períodos)
    2. Volume abaixo da média (acumulação silenciosa)
    3. Squeeze resolve: bandas começam a expandir + preço rompe
    4. Preço rompe acima da banda superior = LONG
    5. Preço rompe abaixo da banda inferior = SHORT

    Timeframe: 2H (intervalos de 2h, alinhado com os alertas do Bruno)
    """
    # Bollinger Bands no 2H (timeframe do Bruno)
    df = get_candles(client, symbol, Client.KLINE_INTERVAL_2HOUR, limit=30)
    df["ma20"]      = df["close"].rolling(20).mean()
    df["std"]       = df["close"].rolling(20).std()
    df["bb_upper"]  = df["ma20"] + 2 * df["std"]
    df["bb_lower"]  = df["ma20"] - 2 * df["std"]
    df["bandwidth"] = (df["bb_upper"] - df["bb_lower"]) / df["ma20"]
    df["vol_media"] = df["volume"].rolling(20).mean()

    # Candles fechados
    c_ref  = df.iloc[-2]   # último candle fechado
    c_prev = df.iloc[-3]   # candle anterior

    preco  = c_ref["close"]
    ma20   = c_ref["ma20"]
    bb_up  = c_ref["bb_upper"]
    bb_low = c_ref["bb_lower"]

    if ma20 <= 0:
        return None

    # --- 1. DETECTA SQUEEZE: bandwidth nos últimos candles era mínima ---
    # Bandwidth mínimo das últimas 20 velas = squeeze aconteceu recentemente
    bw_atual  = c_ref["bandwidth"]
    bw_prev   = c_prev["bandwidth"]
    bw_min_20 = df["bandwidth"].iloc[-22:-2].min()  # mínimo recente (excluindo atuais)

    # Squeeze: bandwidth anterior estava perto do mínimo (dentro de 20% do mínimo)
    estava_em_squeeze = bw_prev <= bw_min_20 * 1.2

    # Release: bandwidth atual é maior que o anterior (bandas expandindo)
    squeeze_resolvendo = bw_atual > bw_prev

    if not estava_em_squeeze or not squeeze_resolvendo:
        return None

    # --- 2. VOLUME: acumulação seguida de expansão ---
    # Volume dos candles anteriores ao squeeze deve ser baixo (acumulação)
    vol_media_recente = df["volume"].iloc[-6:-2].mean()  # média dos últimos 4 candles fechados
    vol_media_geral   = c_ref["vol_media"]
    acumulacao = vol_media_recente <= vol_media_geral * 1.0  # volume normal ou abaixo

    # Volume do candle de release deve subir
    vol_release = c_ref["volume"]
    volume_subindo = vol_release >= vol_media_geral * 0.8  # aceita volume normal no release

    if not (acumulacao or volume_subindo):
        return None

    # --- 3. DIREÇÃO DO ROMPIMENTO ---
    # Posição do preço relativa às bandas
    posicao_banda = (preco - bb_low) / (bb_up - bb_low) if (bb_up - bb_low) > 0 else 0.5

    # LONG: preço rompeu acima da metade superior (>0.7) ou acima da banda
    if preco >= bb_up or posicao_banda >= 0.75:
        return "LONG"

    # SHORT: preço rompeu abaixo da metade inferior (<0.3) ou abaixo da banda
    if preco <= bb_low or posicao_banda <= 0.25:
        return "SHORT"

    return None

# ---------------------------------------------------------------------------
# Gestão de posição
# ---------------------------------------------------------------------------
def get_banca(client: Client) -> float:
    """Retorna o saldo disponível em USDT na carteira de Futuros."""
    balances = client.futures_account_balance()
    for b in balances:
        if b["asset"] == "USDT":
            return float(b["availableBalance"])
    return 0.0


def calcular_margem(banca: float) -> float:
    return round(banca * RISCO_POR_TRADE, 2)


def alavancagem_dinamica(saldo_total: float) -> int:
    """
    Ajusta alavancagem conforme o saldo cresce.
    Conservador até $300, aumenta progressivamente.
    """
    if saldo_total >= 1000:
        return 50
    elif saldo_total >= 500:
        return 30
    else:
        return 20  # padrão atual


def horario_volatil() -> bool:
    """Retorna True nos horários de maior volatilidade do mercado."""
    hora = datetime.now().hour
    # Abertura americana: 14h-17h BRT | Asiático: 21h-23h BRT
    return (14 <= hora < 17) or (21 <= hora < 24)


def enviar_resumo_diario(client: Client, saldo_abertura_dia: float) -> None:
    """Envia resumo completo do dia às 22h."""
    try:
        abertas     = posicoes_abertas(client)
        saldo_total = get_saldo_total(client)
        racio       = get_racio_margem(client)
        usd_brl     = get_usd_brl(client)
        pnl_aberto  = sum(float(p.get("unrealizedProfit", p.get("unRealizedProfit", 0))) for p in abertas)
        variacao    = ((saldo_total - saldo_abertura_dia) / saldo_abertura_dia * 100) if saldo_abertura_dia > 0 else 0
        lucro_dia   = saldo_total - saldo_abertura_dia

        sinal_var = "+" if variacao >= 0 else ""
        sinal_luc = "+" if lucro_dia >= 0 else ""
        brl_str   = f" (R${saldo_total * usd_brl:.2f})" if usd_brl > 0 else ""

        msg = (
            f"<b>Resumo do dia — {datetime.now().strftime('%d/%m/%Y')}</b>\n\n"
            f"Saldo inicio do dia: ${saldo_abertura_dia:.2f}\n"
            f"Saldo atual: ${saldo_total:.2f}{brl_str}\n"
            f"Resultado do dia: {sinal_luc}${lucro_dia:.2f} ({sinal_var}{variacao:.1f}%)\n"
            f"PnL aberto: {pnl_aberto:+.2f} USDT\n"
            f"Racio de Margem: {racio:.2f}%\n"
            f"Posicoes abertas: {len(abertas)}\n"
        )
        if abertas:
            msg += "\n<b>Posicoes:</b>\n"
            for p in abertas:
                amt  = float(p["positionAmt"])
                lado = "LONG" if amt > 0 else "SHORT"
                roi  = calcular_roi(p)
                emoji = "🟢" if roi >= 0 else "🔴"
                msg += f"{emoji} {p['symbol']} | {lado} | ROI: {roi:+.1f}%\n"

        telegram(msg)
        log.info("Resumo diario enviado.")
    except Exception as e:
        log.warning(f"Erro no resumo diario: {e}")


def enviar_resumo_hora(client: Client, saldo_abertura: float) -> None:
    """Envia resumo horário no Telegram com saldo, PnL e posições."""
    try:
        abertas     = posicoes_abertas(client)
        banca       = get_banca(client)
        saldo_total = get_saldo_total(client)
        racio       = get_racio_margem(client)
        usd_brl     = get_usd_brl(client)
        pnl_total   = sum(float(p.get("unrealizedProfit", p.get("unRealizedProfit", 0))) for p in abertas)
        variacao    = ((saldo_total - saldo_abertura) / saldo_abertura * 100) if saldo_abertura > 0 else 0

        sinal_var = "+" if variacao >= 0 else ""
        sinal_pnl = "+" if pnl_total >= 0 else ""
        brl_str   = f" (R${saldo_total * usd_brl:.2f})" if usd_brl > 0 else ""

        linhas = [
            f"<b>Resumo — {datetime.now().strftime('%H:%M')}</b>",
            f"Saldo total: ${saldo_total:.2f}{brl_str}",
            f"Variacao do dia: {sinal_var}{variacao:.1f}%",
            f"PnL aberto: {sinal_pnl}{pnl_total:.2f} USDT",
            f"Racio de Margem: {racio:.2f}%",
            f"Posicoes abertas: {len(abertas)}",
        ]
        if abertas:
            linhas.append("")
            for p in abertas:
                amt  = float(p["positionAmt"])
                lado = "LONG" if amt > 0 else "SHORT"
                roi  = calcular_roi(p)
                emoji = "🟢" if roi >= 0 else "🔴"
                linhas.append(f"{emoji} {p['symbol']} | {lado} | ROI: {roi:+.1f}%")

        telegram("\n".join(linhas))
    except Exception as e:
        log.warning(f"Erro no resumo horario: {e}")


def get_step_size(client: Client, symbol: str) -> float:
    """Retorna o stepSize (precisão mínima de quantidade) do par."""
    info = client.futures_exchange_info()
    for s in info["symbols"]:
        if s["symbol"] == symbol:
            for f in s["filters"]:
                if f["filterType"] == "LOT_SIZE":
                    return float(f["stepSize"])
    return 0.001  # fallback


def arredondar_quantidade(quantidade: float, step: float) -> float:
    """Arredonda a quantidade para o stepSize do par usando Decimal (evita erro de ponto flutuante)."""
    step_d = Decimal(str(step))
    qty_d  = Decimal(str(quantidade))
    return float((qty_d // step_d) * step_d)


def calcular_stops(preco: float, direcao: str, alavancagem: int):
    """
    Stop loss: -200% do ROI da margem → movimento de preço = 200% / alavancagem
    Take profit: +500% do ROI da margem → movimento de preço = 500% / alavancagem
    """
    sl_pct = STOP_LOSS_ROI / alavancagem
    tp_pct = TAKE_PROFIT_ROI / alavancagem

    if direcao == "LONG":
        sl = round(preco * (1 - sl_pct), 4)
        tp = round(preco * (1 + tp_pct), 4)
    else:
        sl = round(preco * (1 + sl_pct), 4)
        tp = round(preco * (1 - tp_pct), 4)

    return sl, tp


def posicoes_abertas(client: Client) -> list:
    account = client.futures_account()
    return [p for p in account["positions"] if abs(float(p["positionAmt"])) > 0]



def get_usd_brl(client: Client) -> float:
    """Retorna a cotação atual USDT/BRL via Binance Spot."""
    try:
        ticker = client.get_symbol_ticker(symbol="USDTBRL")
        return float(ticker["price"])
    except Exception:
        return 0.0


def get_saldo_total(client: Client) -> float:
    """Retorna o saldo de margem total (saldo + PnL aberto), igual ao exibido na Binance."""
    account = client.futures_account()
    return float(account.get("totalMarginBalance", 0))


def get_racio_margem(client: Client) -> float:
    """
    Rácio de Margem igual ao exibido no app da Binance.
    Fórmula: totalMaintMargin / totalMarginBalance × 100
    totalMarginBalance = saldo da carteira + PnL não realizado
    """
    account = client.futures_account()
    maint  = float(account.get("totalMaintMargin", 0))
    balance = float(account.get("totalMarginBalance", 0))
    if balance == 0:
        return 0.0
    return (maint / balance) * 100


# Níveis de proteção do Rácio de Margem
RACIO_BLOQUEIA_ENTRADAS = 15.0  # acima disso: sem novas entradas
RACIO_BLOQUEIA_DCA      = 18.0  # acima disso: DCA automático bloqueado
RACIO_FECHA_PIOR        = 20.0  # acima disso: fecha a posição com maior prejuízo em $
RACIO_EMERGENCIA        = 25.0  # acima disso: fecha posições até voltar a 15%
RACIO_ALERTA_TS: float  = 0.0   # timestamp do último alerta de rácio alto

def proteger_racio(client: Client, abertas: list) -> bool:
    """
    Proteção escalonada do Rácio de Margem.
    Retorna True se DCA deve ser bloqueado (rácio > RACIO_BLOQUEIA_DCA).
    """
    global RACIO_ALERTA_TS
    racio = get_racio_margem(client)

    if racio < RACIO_BLOQUEIA_DCA:
        return False  # tudo normal

    bloquear_dca = racio >= RACIO_BLOQUEIA_DCA

    # Candidatas a fechar: sem DCA ativo, ordenadas por maior prejuízo em dólar
    sem_dca = [p for p in abertas if p["symbol"] not in dca_aplicado]
    sem_dca.sort(key=lambda p: float(p.get("unrealizedProfit", p.get("unRealizedProfit", 0))))

    def fechar_posicao_racio(posicao):
        symbol = posicao["symbol"]
        amt    = float(posicao["positionAmt"])
        lado   = "LONG" if amt > 0 else "SHORT"
        side   = "SELL" if lado == "LONG" else "BUY"
        pnl    = float(posicao.get("unrealizedProfit", posicao.get("unRealizedProfit", 0)))
        roi    = calcular_roi(posicao)
        try:
            if MODO == "real":
                client.futures_create_order(
                    symbol=symbol, side=side, type="MARKET",
                    quantity=abs(amt), reduceOnly=True
                )
            peak_roi.pop(symbol, None)
            ma_reverteu.pop(symbol, None)
            posicao_abertura.pop(symbol, None)
            log.warning(f"  [RACIO {racio:.1f}%] Fechando {symbol} {lado} | PnL ${pnl:+.2f} | ROI {roi:+.1f}%")
            return True
        except BinanceAPIException as e:
            log.error(f"  Erro ao fechar {symbol} por rácio: {e}")
            return False

    if racio >= RACIO_EMERGENCIA:
        # Fecha posições até voltar a 15%
        if time.time() - RACIO_ALERTA_TS >= 300:
            telegram(
                f"<b>Protecao ativa: Racio {racio:.1f}%</b>\n"
                f"Reorganizando posicoes para liberar margem.\n"
                f"Abrindo espaco para novas oportunidades."
            )
            RACIO_ALERTA_TS = time.time()
        for posicao in sem_dca:
            racio_atual = get_racio_margem(client)
            if racio_atual < RACIO_BLOQUEIA_ENTRADAS:
                log.info(f"  [RACIO] Recuperado para {racio_atual:.1f}%. Parando fechamentos.")
                break
            fechar_posicao_racio(posicao)
            time.sleep(0.5)
        # Se ainda acima do limite e só restam posições com DCA, fecha as piores delas também
        racio_pos = get_racio_margem(client)
        if racio_pos >= RACIO_EMERGENCIA:
            com_dca = [p for p in abertas if p["symbol"] in dca_aplicado]
            com_dca.sort(key=lambda p: float(p.get("unrealizedProfit", p.get("unRealizedProfit", 0))))
            for posicao in com_dca:
                if get_racio_margem(client) < RACIO_BLOQUEIA_ENTRADAS:
                    break
                fechar_posicao_racio(posicao)
                time.sleep(0.5)

    elif racio >= RACIO_FECHA_PIOR:
        # Fecha apenas a posição com maior prejuízo em dólar
        if sem_dca:
            pior = sem_dca[0]
            pnl  = float(pior.get("unrealizedProfit", pior.get("unRealizedProfit", 0)))
            log.warning(f"  [RACIO {racio:.1f}%] Fechando pior posicao: {pior['symbol']} PnL ${pnl:+.2f}")
            fechar_posicao_racio(pior)
        elif time.time() - RACIO_ALERTA_TS >= 600:
            telegram(
                f"<b>Atencao: Racio {racio:.1f}%</b>\n"
                f"Todas as posicoes negativas estao em DCA.\n"
                f"Monitorando recuperacao."
            )
            RACIO_ALERTA_TS = time.time()

    return bloquear_dca

# ---------------------------------------------------------------------------
# Execução de ordens
# ---------------------------------------------------------------------------
def abrir_posicao(client: Client, symbol: str, direcao: str, preco: float, banca: float, qualidade: str = "NORMAL", risco_base: float = None) -> None:
    saldo_total    = get_saldo_total(client)
    alav_ideal     = alavancagem_dinamica(saldo_total)
    _risco_base    = risco_base if risco_base is not None else RISCO_POR_TRADE
    risco          = 0.02 if qualidade == "PREMIUM" else _risco_base
    margem         = round(banca * risco, 2)
    side           = "BUY" if direcao == "LONG" else "SELL"
    side_close     = "SELL" if direcao == "LONG" else "BUY"

    if alav_ideal != ALAVANCAGEM:
        log.info(f"  Alavancagem dinamica: {alav_ideal}x (saldo ${saldo_total:.0f})")

    if MODO == "simulacao":
        step       = get_step_size(client, symbol)
        quantidade = arredondar_quantidade((margem * alav_ideal) / preco, step)
        msg = (
            f"[SIMULACAO] {direcao} {symbol}\n"
            f"Preco: {preco} | Qtd: {quantidade}\n"
            f"Margem: ${margem} | Alavancagem: {ALAVANCAGEM}x\n"
            f"Saida: DCA + Trailing (sem stop fixo)"
        )
        log.info(msg)
        telegram(msg)
        return

    try:
        # Define alavancagem — tenta reduzir se o par não suportar o valor configurado
        alavancagem_real = ALAVANCAGEM
        for lev in [alav_ideal, 50, 30, 20, 15, 10, 5, 3, 2, 1]:
            try:
                client.futures_change_leverage(symbol=symbol, leverage=lev)
                alavancagem_real = lev
                if lev < ALAVANCAGEM:
                    log.info(f"  {symbol}: alavancagem reduzida para {lev}x (maximo permitido)")
                break
            except BinanceAPIException:
                continue

        step       = get_step_size(client, symbol)
        quantidade = arredondar_quantidade((margem * alavancagem_real) / preco, step)
        sl, tp     = calcular_stops(preco, direcao, alavancagem_real)

        # Ordem de entrada a mercado
        client.futures_create_order(
            symbol=symbol,
            side=side,
            type="MARKET",
            quantity=quantidade,
        )

        # Confirma preenchimento da ordem
        time.sleep(0.5)
        try:
            posicoes = client.futures_position_information(symbol=symbol)
            pos_conf = next((p for p in posicoes if abs(float(p["positionAmt"])) > 0), None)
            preco_real = float(pos_conf["entryPrice"]) if pos_conf else preco
            status = "CONFIRMADA" if pos_conf else "PENDENTE — verifique na Binance"
        except Exception:
            preco_real = preco
            status = "nao confirmada"

        msg = (
            f"<b>ORDEM {status}: {direcao} {symbol} [{qualidade}]</b>\n"
            f"Preco entrada: {preco_real} | Qtd: {quantidade}\n"
            f"Margem: ${margem} ({risco*100:.0f}% risco) | Alavancagem: {alavancagem_real}x\n"
            f"Saida: DCA + Trailing (sem stop fixo)"
        )
        log.info(msg.replace("<b>", "").replace("</b>", ""))
        telegram(msg)
        # Marca posição Bruno para trailing mais paciente
        if qualidade == "BRUNO":
            posicoes_bruno.add(symbol)

    except BinanceAPIException as e:
        log.error(f"Erro ao abrir posicao {symbol}: {e}")
        telegram(f"ERRO ao abrir {symbol}: {e.message}")

# ---------------------------------------------------------------------------
# Loop principal
# ---------------------------------------------------------------------------
def verificar_atualizacao(reiniciar: bool = False) -> None:
    """
    Verifica se há nova versão no GitHub.
    Se reiniciar=True e houver atualização: faz git pull e reinicia o processo.
    """
    try:
        import subprocess, sys
        subprocess.run(["git", "fetch"], capture_output=True, text=True, timeout=15)
        status = subprocess.run(
            ["git", "status", "-uno"],
            capture_output=True, text=True, timeout=10
        )
        if "Your branch is behind" in status.stdout:
            if reiniciar:
                log.warning("=" * 50)
                log.warning("Nova versao detectada! Baixando e reiniciando...")
                log.warning("=" * 50)
                telegram("Atualizacao detectada no GitHub!\nBaixando nova versao e reiniciando o bot...")
                pull = subprocess.run(["git", "pull"], capture_output=True, text=True, timeout=30)
                log.info(f"git pull: {pull.stdout.strip()}")
                log.info("Reiniciando processo...")
                os.execv(sys.executable, [sys.executable] + sys.argv)
            else:
                log.warning("=" * 50)
                log.warning("AVISO: Nova versao disponivel no GitHub!")
                log.warning("O bot vai baixar automaticamente na proxima verificacao.")
                log.warning("=" * 50)
    except Exception:
        pass  # sem internet ou git não configurado — ignora silenciosamente


def main() -> None:
    verificar_atualizacao()
    log.info("=" * 50)
    log.info(f"Nunes iniciado | Modo: {MODO.upper()} | Estrategia: {ESTRATEGIA.upper()}")
    log.info(f"Sistema Aguia Spread: Bollinger Squeeze 2H | DCA 3x | Trailing paciente")
    if ESTRATEGIA == "scalping":
        log.info(f"SCALPING: TP +{SCALP_TP:.0f}% | SL {SCALP_SL:.0f}% | Max 4 posicoes")
    log.info(f"Risco: {RISCO_POR_TRADE*100}% | Alavancagem: {ALAVANCAGEM}x")
    log.info("=" * 50)

    client = Client(API_KEY, API_SECRET)
    ciclo_salvo, saldo_ciclo_salvo, ciclos_positivos_salvos = carregar_estado()

    banca_inicial = get_banca(client)
    log.info(f"Saldo disponivel: ${banca_inicial:.2f} USDT")
    saldo_total_ini = get_saldo_total(client)
    usd_brl_ini     = get_usd_brl(client)
    abertas_ini     = posicoes_abertas(client)
    pos_ini         = len(abertas_ini)
    pos_pos         = sum(1 for p in abertas_ini if float(p.get("unrealizedProfit", p.get("unRealizedProfit", 0))) >= 0)
    pos_neg         = pos_ini - pos_pos
    pnl_ini         = sum(float(p.get("unrealizedProfit", p.get("unRealizedProfit", 0))) for p in abertas_ini)
    pnl_ini_brl     = pnl_ini * usd_brl_ini if usd_brl_ini > 0 else pnl_ini
    msg_inicio = (
        f"<b>Nunes iniciado — {MODO.upper()}</b>\n"
        f"Saldo: ${saldo_total_ini:.2f} (R${saldo_total_ini * usd_brl_ini:.2f})\n"
        f"Alavancagem: {ALAVANCAGEM}x | Risco: {RISCO_POR_TRADE*100:.0f}%\n\n"
        f"Posicoes abertas: {pos_ini} | 🟢 {pos_pos} | 🔴 {pos_neg}\n"
        f"PnL atual: {pnl_ini:+.2f} USDT (R${pnl_ini_brl:+.2f})"
    )
    telegram(msg_inicio)

    ultimo_scan_entradas  = 0
    ultimo_resumo_hora    = 0
    ultimo_check_update   = time.time()
    saldo_abertura        = banca_inicial
    saldo_abertura_dia    = get_saldo_total(client)
    resumo_diario_enviado = False
    dia_atual             = datetime.now().day
    ultimo_entrada        = time.time()  # controla timeout sem entrada

    # Controle de ciclos
    ciclo_num                    = ciclo_salvo
    ciclos_positivos_consecutivos = ciclos_positivos_salvos
    saldo_total_atual            = get_saldo_total(client)
    saldo_ciclo_inicio           = saldo_ciclo_salvo if saldo_ciclo_salvo else saldo_total_atual
    ultimo_check_ciclo           = 0
    meta_confirmacoes            = 0
    _usd_brl_ini                 = get_usd_brl(client)
    _meta_pct_ini                = meta_formiguinha(ciclos_positivos_consecutivos)
    _meta_ini_usdt               = saldo_ciclo_inicio * (_meta_pct_ini / 100)
    _meta_ini_brl                = _meta_ini_usdt * _usd_brl_ini if _usd_brl_ini > 0 else _meta_ini_usdt
    log.info(f"Ciclo {ciclo_num} retomado | Saldo: ${saldo_ciclo_inicio:.2f} | Meta: {_meta_pct_ini:.1f}% (${_meta_ini_usdt:.2f} / R${_meta_ini_brl:.2f}) | Ciclos positivos: {ciclos_positivos_consecutivos}")
    telegram(f"<b>Ciclo {ciclo_num} retomado</b>\nSaldo: ${saldo_ciclo_inicio:.2f} USDT\nMeta: {_meta_pct_ini:.1f}% = ${_meta_ini_usdt:.2f} / R${_meta_ini_brl:.2f}\nCiclos positivos consecutivos: {ciclos_positivos_consecutivos}")

    while bot_ativo:
        try:
            global dca_ativo, alerta_ciclo_risco_ts, posicoes_herdadas
            processar_comandos(client)
            banca = get_banca(client)

            # --- VERIFICACAO DE META DE CICLO (a cada 15s) ---
            if time.time() - ultimo_check_ciclo >= 15:
                ultimo_check_ciclo = time.time()
                try:
                    abertas_ciclo   = posicoes_abertas(client)
                    usd_brl_c       = get_usd_brl(client)
                    meta_pct_atual  = meta_formiguinha(ciclos_positivos_consecutivos)
                    meta_ciclo_usdt = saldo_ciclo_inicio * (meta_pct_atual / 100)
                    fase_txt        = f"{meta_pct_atual:.1f}% | meta ${meta_ciclo_usdt:.2f}"
                    meta_brl        = meta_ciclo_usdt * usd_brl_c if usd_brl_c > 0 else meta_ciclo_usdt
                    if abertas_ciclo:
                        # PnL do ciclo = apenas posições do ciclo atual (exclui herdadas)
                        pos_ciclo_atual = [p for p in abertas_ciclo if p["symbol"] not in posicoes_herdadas]
                        pnl_ciclo = sum(float(p.get("unrealizedProfit", p.get("unRealizedProfit", 0))) for p in pos_ciclo_atual)
                        pnl_brl   = pnl_ciclo * usd_brl_c if usd_brl_c > 0 else pnl_ciclo
                        # PnL das herdadas (informativo)
                        pnl_herd  = sum(float(p.get("unrealizedProfit", p.get("unRealizedProfit", 0))) for p in abertas_ciclo if p["symbol"] in posicoes_herdadas)
                        herd_str  = f" | Herdadas: {pnl_herd:+.2f} USDT" if posicoes_herdadas else ""
                        meta_rapida_usdt = saldo_ciclo_inicio * 0.08  # 8% fecha imediatamente
                        if pnl_ciclo >= meta_rapida_usdt:
                            meta_confirmacoes = 2  # força fechamento imediato
                            log.info(f"Ciclo {ciclo_num} [{fase_txt}] | PnL ciclo: {pnl_brl:+.2f} BRL{herd_str} | Meta rapida 8% atingida")
                        elif pnl_ciclo >= meta_ciclo_usdt:
                            meta_confirmacoes += 1
                        else:
                            meta_confirmacoes = 0
                        log.info(f"Ciclo {ciclo_num} [{fase_txt}] | PnL ciclo: {pnl_brl:+.2f} BRL{herd_str} | Meta: R${meta_brl:.2f} | Confirmacoes: {meta_confirmacoes}/2")

                        # --- ALERTA DE CICLO EM RISCO ---
                        drenos = [(p["symbol"], calcular_roi(p), float(p.get("unrealizedProfit", p.get("unRealizedProfit", 0))))
                                  for p in abertas_ciclo if calcular_roi(p) <= -150]
                        if pnl_ciclo < 0 and drenos and time.time() - alerta_ciclo_risco_ts >= 1800:
                            alerta_ciclo_risco_ts = time.time()
                            linhas_drenos = "\n".join(
                                f"🔴 {sym} ROI {roi:+.1f}% ({pnl:+.2f} USDT)"
                                for sym, roi, pnl in sorted(drenos, key=lambda x: x[1])
                            )
                            vencedores = sorted(
                                [(p["symbol"], calcular_roi(p)) for p in abertas_ciclo if calcular_roi(p) > 0],
                                key=lambda x: -x[1]
                            )
                            linha_venc = ", ".join(f"{s} {r:+.1f}%" for s, r in vencedores[:3]) if vencedores else "nenhum"
                            msg = (
                                f"<b>Ciclo {ciclo_num} em risco</b>\n\n"
                                f"PnL atual: {pnl_brl:+.2f} BRL\n"
                                f"Meta: R${meta_brl:.2f}\n\n"
                                f"<b>Drenos principais:</b>\n{linhas_drenos}\n\n"
                                f"<b>Segurando o ciclo:</b> {linha_venc}\n\n"
                                f"<b>O que voce pode fazer:</b>\n"
                                + "\n".join(f"• /fechar {sym} — aceitar perda e liberar margem" for sym, _, _ in drenos)
                                + "\n• /fechartudo — encerrar tudo e reiniciar ciclo"
                                + "\n• Aguardar — bot continua monitorando e tentando DCA"
                            )
                            telegram(msg)
                            log.warning(f"Ciclo {ciclo_num} em risco — alerta enviado. Drenos: {[s for s,_,_ in drenos]}")

                        if meta_confirmacoes >= 2:
                            meta_confirmacoes = 0
                            MAX_HERDADAS = 3

                            # Separa posições do ciclo atual em positivas e negativas
                            pos_fechar   = [p for p in pos_ciclo_atual if calcular_roi(p) >= 0]
                            pos_herdar   = [p for p in pos_ciclo_atual if calcular_roi(p) < 0]

                            # Limita herança a MAX_HERDADAS (fecha as piores se passar do limite)
                            vagas_herdadas = MAX_HERDADAS - len(posicoes_herdadas)
                            if len(pos_herdar) > vagas_herdadas:
                                # Ordena por ROI (piores primeiro) — os excedentes são fechados
                                pos_herdar_sorted = sorted(pos_herdar, key=lambda p: calcular_roi(p))
                                pos_fechar += pos_herdar_sorted[vagas_herdadas:]  # excedentes vão para fechar
                                pos_herdar  = pos_herdar_sorted[:vagas_herdadas]

                            telegram(f"<b>Meta do Ciclo {ciclo_num} confirmada!</b>\nPnL ciclo: R${pnl_brl:.2f}\nFechando {len(pos_fechar)} posicoes | Herdando {len(pos_herdar)} negativas...")
                            log.info(f"Meta atingida! Fechando {len(pos_fechar)} | Herdando {len(pos_herdar)}")

                            resultados_fechados = []
                            resultados_herdados = []

                            for p in pos_fechar:
                                symbol = p["symbol"]
                                amt    = float(p["positionAmt"])
                                lado   = "LONG" if amt > 0 else "SHORT"
                                side   = "SELL" if lado == "LONG" else "BUY"
                                roi    = calcular_roi(p)
                                try:
                                    if MODO == "real":
                                        client.futures_create_order(
                                            symbol=symbol, side=side, type="MARKET",
                                            quantity=abs(amt), reduceOnly=True
                                        )
                                    emoji = "🟢" if roi >= 0 else "🔴"
                                    resultados_fechados.append(f"{emoji} {symbol} {lado} ROI:{roi:+.1f}%")
                                    time.sleep(0.5)
                                except BinanceAPIException as e:
                                    log.error(f"Erro ao fechar {symbol}: {e}")
                                    resultados_fechados.append(f"{symbol}: ERRO")

                            for p in pos_herdar:
                                symbol = p["symbol"]
                                roi    = calcular_roi(p)
                                posicoes_herdadas.add(symbol)
                                resultados_herdados.append(f"🔴 {symbol} ROI:{roi:+.1f}% (herdada)")
                                log.info(f"  {symbol} herdada para o proximo ciclo | ROI {roi:+.1f}%")

                            saldo_novo     = get_saldo_total(client)
                            lucro_ciclo    = saldo_novo - saldo_ciclo_inicio
                            lucro_brl      = lucro_ciclo * usd_brl_c if usd_brl_c > 0 else lucro_ciclo
                            pct_real       = (lucro_ciclo / saldo_ciclo_inicio * 100) if saldo_ciclo_inicio > 0 else 0
                            # Atualiza contador formiguinha
                            if lucro_ciclo > 0:
                                ciclos_positivos_consecutivos += 1
                            else:
                                ciclos_positivos_consecutivos = 0
                            nova_meta_pct  = meta_formiguinha(ciclos_positivos_consecutivos)
                            nova_meta_usdt = saldo_novo * (nova_meta_pct / 100)
                            nova_meta_brl  = nova_meta_usdt * usd_brl_c if usd_brl_c > 0 else nova_meta_usdt
                            sinal_lucro    = "+" if lucro_ciclo >= 0 else ""

                            msg = (
                                f"<b>Ciclo {ciclo_num} encerrado!</b>\n\n"
                                f"Lucro realizado: {sinal_lucro}{lucro_ciclo:.2f} USDT (R${lucro_brl:.2f})\n"
                                f"Percentual real: {sinal_lucro}{pct_real:.2f}%\n"
                                f"Saldo novo: ${saldo_novo:.2f} (R${saldo_novo * usd_brl_c:.2f})\n\n"
                                f"Fechadas:\n" + "\n".join(resultados_fechados)
                            )
                            if resultados_herdados:
                                msg += f"\n\nHerdadas para o ciclo {ciclo_num + 1}:\n" + "\n".join(resultados_herdados)
                            msg += (
                                f"\n\n<b>Ciclo {ciclo_num + 1} iniciado</b>\n"
                                f"Meta: {nova_meta_pct:.1f}% = ${nova_meta_usdt:.2f} USDT / R${nova_meta_brl:.2f}\n"
                                f"Ciclos positivos consecutivos: {ciclos_positivos_consecutivos}"
                            )
                            telegram(msg)
                            log.info(f"Ciclo {ciclo_num} encerrado! Lucro: {sinal_lucro}{lucro_ciclo:.2f} USDT | Ciclo {ciclo_num+1} | Meta: R${nova_meta_brl:.2f}")
                            ciclo_num          += 1
                            saldo_ciclo_inicio  = saldo_novo
                            meta_confirmacoes   = 0
                            # Limpa apenas posições do ciclo (não as herdadas)
                            for sym in list(dca_aplicado):
                                if sym not in posicoes_herdadas:
                                    dca_aplicado.discard(sym)
                            for sym in list(peak_roi):
                                if sym not in posicoes_herdadas:
                                    peak_roi.pop(sym, None)
                            for sym in list(ma_reverteu):
                                if sym not in posicoes_herdadas:
                                    ma_reverteu.pop(sym, None)
                            for sym in list(posicao_abertura):
                                if sym not in posicoes_herdadas:
                                    posicao_abertura.pop(sym, None)
                            dca_ativo = None
                            salvar_estado(ciclo_num=ciclo_num, saldo_ciclo_inicio=saldo_ciclo_inicio, ciclos_positivos=ciclos_positivos_consecutivos)
                except Exception as e:
                    log.warning(f"Erro verificacao ciclo: {e}")

            abertas = posicoes_abertas(client)

            # --- PROTEÇÃO DO RÁCIO DE MARGEM ---
            dca_bloqueado_por_racio = proteger_racio(client, abertas)

            # Listar posições abertas
            if abertas:
                saldo_total = get_saldo_total(client)
                margem_total = sum(float(p.get("positionInitialMargin", 0)) for p in abertas)
                pnl_total = sum(float(p.get("unrealizedProfit", p.get("unRealizedProfit", 0))) for p in abertas)
                racio = get_racio_margem(client)
                usd_brl = get_usd_brl(client)
                brl_str = f" (R${saldo_total * usd_brl:.2f})" if usd_brl > 0 else ""
                pnl_brl = f" (R${pnl_total * usd_brl:+.2f})" if usd_brl > 0 else ""
                log.info(f"--- Posicoes abertas ({len(abertas)}) | Racio de Margem: {racio:.2f}% | PnL Total: {pnl_total:+.2f} USDT{pnl_brl} ---")
                for p in abertas:
                    amt   = float(p["positionAmt"])
                    lado  = "LONG" if amt > 0 else "SHORT"
                    pnl   = float(p.get("unrealizedProfit", p.get("unRealizedProfit", 0)))
                    preco_entrada = float(p["entryPrice"])
                    roi   = calcular_roi(p)
                    sinal_pnl = "+" if pnl >= 0 else ""
                    pnl_p_brl = f" (R${pnl * usd_brl:+.2f})" if usd_brl > 0 else ""
                    herd_tag = " [HERDADA]" if p["symbol"] in posicoes_herdadas else ""
                    log.info(f"  {p['symbol']}{herd_tag} | {lado} | Entrada: {preco_entrada} | PnL: {sinal_pnl}{pnl:.2f} USDT{pnl_p_brl} | ROI: {roi:+.1f}%")
                log.info(f"  Saldo disponivel: ${banca:.2f} USDT | Saldo total: ${saldo_total:.2f} USDT{brl_str}")
                log.info("-------------------------------")

            # Gestão de posições abertas
            for p in abertas:
                symbol  = p["symbol"]
                amt     = float(p["positionAmt"])
                direcao = "LONG" if amt > 0 else "SHORT"
                roi     = calcular_roi(p)
                margem_atual = float(p.get("positionInitialMargin", 0))

                # Detecta DCA manual: margem aumentou > 100% desde o último registro
                # Ignora topups recentes (< 5 min) — topup é pequeno, DCA real triplica
                if symbol in margem_registrada:
                    margem_anterior = margem_registrada[symbol]
                    foi_topup = symbol in topup_recente and time.time() - topup_recente[symbol] < 300
                    if margem_anterior > 0 and margem_atual > margem_anterior * 1.5 and not foi_topup:
                        if symbol not in dca_aplicado:
                            dca_aplicado.add(symbol)
                            dca_contagem[symbol] = dca_contagem.get(symbol, 0) + 1
                            if dca_ativo is None:
                                dca_ativo = symbol
                            salvar_estado()  # salva IMEDIATAMENTE para não perder no reinício
                            log.info(f"  {symbol}: 3x manual detectado (margem ${margem_anterior:.2f} -> ${margem_atual:.2f})")
                            telegram(f"<b>3x manual detectado: {symbol}</b>\nMargem ${margem_anterior:.2f} -> ${margem_atual:.2f}\nBot vai aguardar saida inteligente (+3%/+10%).")
                margem_registrada[symbol] = margem_atual

                # Registra horário de abertura da posição
                if symbol not in posicao_abertura:
                    try:
                        # Pega apenas trades recentes (última hora) para evitar confundir
                        # com trades antigos de posições anteriores no mesmo symbol
                        desde = int((time.time() - 3600) * 1000)  # última 1 hora
                        trades = client.futures_account_trades(symbol=symbol, limit=50, startTime=desde)
                        if trades:
                            posicao_abertura[symbol] = trades[0]["time"] / 1000
                        else:
                            # Posição existia antes do bot iniciar — usa tempo atual como base
                            posicao_abertura[symbol] = time.time()
                    except Exception:
                        posicao_abertura[symbol] = time.time()

                # Atualiza pico de ROI (trailing stop)
                if roi > peak_roi.get(symbol, roi):
                    peak_roi[symbol] = roi
                    salvar_estado()

                pico = peak_roi.get(symbol, roi)

                # --- LIMITES ABSOLUTOS ---
                # +500%: fecha 50% e deixa resto correr com trailing apertado 10%
                if roi >= 500 and symbol not in parcial_500:
                    log.info(f"  {symbol}: ROI {roi:+.1f}% >= +500% -> fechando 50%, resto com trailing 10%")
                    telegram(f"<b>Lucro extraordinario: {symbol}</b>\n{direcao} | ROI: {roi:+.1f}%\nGarantindo 50% do lucro! Resto continua correndo.")
                    fechar_parcial(client, p, 0.50, f"TP parcial +500% ({roi:+.1f}%)")
                    parcial_500.add(symbol)
                    continue
                # Após parcial de 500%, trailing apertado de 10% do pico
                if symbol in parcial_500:
                    queda_pct = (pico - roi) / pico if pico > 0 else 0
                    if queda_pct >= 0.10:
                        log.info(f"  {symbol}: trailing pos-500%! Pico {pico:.0f}% -> atual {roi:.0f}% (queda 10%) -> fechando resto")
                        telegram(f"<b>Lucro protegido: {symbol}</b>\n{direcao} | Pico: {pico:.0f}% | Atual: {roi:+.1f}%\nFechando restante com lucro garantido.")
                        fechar_parcial(client, p, 1.0, f"Trailing pos-500% (pico {pico:.0f}%)")
                        parcial_500.discard(symbol)
                        peak_roi.pop(symbol, None)
                        ma_reverteu.pop(symbol, None)
                        continue
                    else:
                        log.info(f"  {symbol}: ROI {roi:+.1f}% | pico {pico:.0f}% | pos-500% trailing 10%")
                        continue
                # SL -200% REMOVIDO — Rácio de Margem protege a conta
                # Posições negativas aguardam oportunidade de 3x

                # --- MODO SCALPING PURO: TP/SL rápido, sem DCA, sem trailing ---
                # (modo híbrido usa trailing do swing, não entra aqui)
                if ESTRATEGIA == "scalping" and ESTRATEGIA != "hibrido":
                    if roi >= SCALP_TP:
                        log.info(f"  {symbol}: SCALP TP! ROI {roi:+.1f}% >= {SCALP_TP:.0f}% -> fechando 100%")
                        telegram(
                            f"<b>Scalp TP: {symbol}</b>\n"
                            f"{direcao} | ROI: {roi:+.1f}%\n"
                            f"Lucro rapido garantido."
                        )
                        fechar_parcial(client, p, 1.0, f"Scalp TP ({roi:+.1f}%)")
                        peak_roi.pop(symbol, None)
                        posicao_abertura.pop(symbol, None)
                    elif roi <= SCALP_SL:
                        log.info(f"  {symbol}: SCALP SL! ROI {roi:+.1f}% <= {SCALP_SL:.0f}% -> fechando 100%")
                        telegram(
                            f"<b>Scalp SL: {symbol}</b>\n"
                            f"{direcao} | ROI: {roi:+.1f}%\n"
                            f"Stop rapido — proxima entrada."
                        )
                        fechar_parcial(client, p, 1.0, f"Scalp SL ({roi:+.1f}%)")
                        peak_roi.pop(symbol, None)
                        posicao_abertura.pop(symbol, None)
                    else:
                        log.info(f"  {symbol}: ROI {roi:+.1f}% | scalping (TP {SCALP_TP:.0f}% / SL {SCALP_SL:.0f}%)")
                    continue  # pula toda lógica de swing

                # Alertas de risco para todas as posições
                verificar_alertas_risco(client, p, roi)

                # --- DETECÇÃO DE MA REVERSAL CONTRA A POSIÇÃO ---
                if roi >= 50 and symbol not in dca_aplicado:
                    try:
                        df5 = get_candles(client, symbol, Client.KLINE_INTERVAL_5MINUTE, limit=30)
                        df5["ma7"]  = df5["close"].rolling(7).mean()
                        df5["ma25"] = df5["close"].rolling(25).mean()
                        c2, c1 = df5.iloc[-2], df5.iloc[-1]
                        if direcao == "LONG":
                            reverteu = c2["ma7"] >= c2["ma25"] and c1["ma7"] < c1["ma25"]
                        else:
                            reverteu = c2["ma7"] <= c2["ma25"] and c1["ma7"] > c1["ma25"]

                        if reverteu and symbol not in ma_reverteu:
                            ma_reverteu[symbol] = roi
                            msg = (
                                f"⚠️ <b>MA reverteu: {symbol}</b>\n"
                                f"{direcao} | ROI atual: {roi:+.1f}% | Pico: {pico:+.1f}%\n"
                                f"MA7 cruzou contra a posição no 5min.\n"
                                f"Trailing apertado ativado (piso: ROI {roi - 15:.0f}%)."
                            )
                            telegram(msg)
                            log.info(f"  {symbol}: MA reverteu contra {direcao} | ROI {roi:+.1f}% | trailing apertado ativado")

                        # Se MA voltou a favor, remove o estado de reversão
                        if not reverteu and symbol in ma_reverteu:
                            ma_reverteu.pop(symbol)
                            log.info(f"  {symbol}: MA voltou a favor — trailing normal restaurado")
                    except Exception:
                        pass

                # --- TRAILING APERTADO PÓS-REVERSÃO DE MA ---
                if symbol in ma_reverteu and symbol not in dca_aplicado:
                    roi_na_reversao = ma_reverteu[symbol]
                    piso_apertado = roi_na_reversao - 25.0
                    if roi < piso_apertado:
                        if roi_na_reversao >= 100:
                            pct_fechar = 0.80
                        elif roi_na_reversao >= 30:
                            pct_fechar = 0.90
                        else:
                            pct_fechar = 1.0
                        log.info(f"  {symbol}: trailing pós-MA! ROI {roi:+.1f}% < piso {piso_apertado:.0f}% -> fechando {int(pct_fechar*100)}%")
                        fechar_parcial(client, p, pct_fechar, f"Trailing pos-MA reversal (piso {piso_apertado:.0f}%)")
                        ma_reverteu.pop(symbol, None)
                        peak_roi.pop(symbol, None)
                        continue

                # --- SAÍDA PÓS-3x: +10% se MA forte, +3% se lateralizando ---
                if symbol in dca_aplicado:
                    roi_entrada_dca = roi_no_dca.get(symbol, roi)
                    n_3x = dca_contagem.get(symbol, 1)

                    if roi > 0:
                        # Verifica se MA7 está com força ou lateralizando no 5min
                        try:
                            df5_dca = get_candles(client, symbol, Client.KLINE_INTERVAL_5MINUTE, limit=30)
                            df5_dca["ma7"]  = df5_dca["close"].rolling(7).mean()
                            df5_dca["ma25"] = df5_dca["close"].rolling(25).mean()
                            diff_agora = abs(df5_dca["ma7"].iloc[-1] - df5_dca["ma25"].iloc[-1])
                            diff_antes = abs(df5_dca["ma7"].iloc[-3] - df5_dca["ma25"].iloc[-3])
                            ma_perdendo_forca = diff_agora <= diff_antes  # lateralizando ou convergindo
                        except Exception:
                            ma_perdendo_forca = False

                        # Meta dinâmica: +10% se MA forte, +3% se perdendo força
                        meta_saida = 3.0 if ma_perdendo_forca else 10.0
                        motivo_meta = "MA lateralizando" if ma_perdendo_forca else "MA com forca"

                        if roi >= meta_saida:
                            log.info(f"  {symbol}: [POS-3x #{n_3x}] ROI {roi:+.1f}% >= +{meta_saida:.0f}% ({motivo_meta}) -> fechando 90%")
                            telegram(
                                f"<b>Lucro realizado: {symbol}</b>\n"
                                f"{direcao} | ROI: {roi:+.1f}% | 3x aplicados: {n_3x}\n"
                                f"Saida em +{meta_saida:.0f}% ({motivo_meta})\n"
                                f"Fechando 90% com lucro! 10% continua na posicao."
                            )
                            fechar_parcial(client, p, 0.90, f"Saida 90% pos-3x #{n_3x} +{meta_saida:.0f}% (ROI {roi:.1f}%)")
                            dca_aplicado.discard(symbol)
                            dca_contagem.pop(symbol, None)
                            roi_no_dca.pop(symbol, None)
                            if dca_ativo == symbol:
                                dca_ativo = None
                        else:
                            log.info(f"  {symbol}: [POS-3x #{n_3x}] ROI {roi:+.1f}% | meta {meta_saida:.0f}% ({motivo_meta}) | aguardando")
                    else:
                        log.info(f"  {symbol}: [EM 3x #{n_3x}] ROI {roi:+.1f}% | aguardando virar positivo")

                # --- POSIÇÕES NORMAIS — TRAILING STOP ESCALONADO ---
                elif roi > 0 and pico >= 5:
                    # Detecta cripto com alta variação 24h (pump) — trailing mais apertado
                    cripto_pump = False
                    try:
                        ticker_info = client.futures_symbol_ticker(symbol=symbol)
                        ticker_24h = client.futures_ticker(symbol=symbol)
                        var_24h = abs(float(ticker_24h["priceChangePercent"]))
                        cripto_pump = var_24h >= 30.0  # variou mais de 30% em 24h
                    except Exception:
                        pass

                    # Tolerância escalonada: quanto maior o pico, mais apertado
                    is_bruno = symbol in posicoes_bruno
                    if cripto_pump and not is_bruno:
                        tolerancia = 0.05  # 5% do pico — pump pode despencar
                    elif is_bruno:
                        # Bruno: segura por dias, trailing largo
                        if pico >= 100:
                            tolerancia = 0.20  # 20% — só protege lucro grande
                        else:
                            tolerancia = 0.50  # 50% — muita paciência, deixa crescer
                    elif pico >= 50:
                        tolerancia = 0.15  # 15% do pico
                    elif pico >= 20:
                        tolerancia = 0.20  # 20% do pico
                    else:
                        tolerancia = 0.40  # 40% do pico

                    queda_do_pico = pico - roi
                    queda_pct = queda_do_pico / pico if pico > 0 else 0
                    pump_tag = " [PUMP]" if cripto_pump else ""
                    if queda_pct >= tolerancia:
                        log.info(f"  {symbol}{pump_tag}: trailing stop! Pico {pico:.0f}% -> atual {roi:.0f}% (tolerancia {tolerancia:.0%}) -> fechando 90%")
                        fechar_parcial(client, p, 0.90, f"Trailing stop{pump_tag} (pico {pico:.0f}% tol {tolerancia:.0%})")
                        peak_roi.pop(symbol, None)
                        ma_reverteu.pop(symbol, None)
                    else:
                        log.info(f"  {symbol}{pump_tag}: ROI {roi:+.1f}% | pico {pico:.0f}% | trailing ok (tol {tolerancia:.0%})")

                # --- MONITORAMENTO NEGATIVO (sem stop por ROI/tempo — Rácio de Margem protege) ---
                elif roi < 0:
                    # Alerta quando se aproxima do gatilho de 3x (-180%)
                    if roi <= -180 and roi > -200:
                        alerta_key = f"perto3x_{symbol}"
                        if time.time() - alerta_dca_log.get(alerta_key, 0) >= 600:
                            grafico = analise_grafico_3x(client, symbol, direcao)
                            telegram(
                                f"<b>Preparando 3x: {symbol}</b>\n"
                                f"{direcao} | ROI: {roi:+.1f}%\n"
                                f"Proximo do gatilho (-200%). Acompanhe:{grafico}"
                            )
                            alerta_dca_log[alerta_key] = time.time()
                    log.info(f"  {symbol}: ROI {roi:+.1f}% | aguardando oportunidade de 3x")

                # --- ESTRATÉGIA 3x (DCA padrão Bruno) ---
                # Gatilho: ROI <= -200% + MA7 cruzando a favor no 5min
                # Repetível: pode fazer 3x múltiplas vezes no mesmo ativo
                elif roi <= -200.0:
                    n_3x = dca_contagem.get(symbol, 0)
                    try:
                        ma_ok = ma_cruza_favor(client, symbol, direcao)
                        if ma_ok:
                            if dca_bloqueado_por_racio:
                                log.warning(f"  {symbol}: 3x bloqueado — Racio de Margem acima de {RACIO_BLOQUEIA_DCA:.0f}%")
                            else:
                                log.info(f"  {symbol}: ROI {roi:.1f}% + MA cruzou a favor -> 3x #{n_3x + 1}")
                                aplicar_dca(client, p, banca)
                                dca_aplicado.add(symbol)
                                dca_contagem[symbol] = n_3x + 1
                                dca_ativo = symbol
                                grafico = analise_grafico_3x(client, symbol, direcao)
                                telegram(
                                    f"<b>OBA! 3x ativado: {symbol} (#{n_3x + 1})</b>\n"
                                    f"{direcao} | ROI: {roi:+.1f}%\n"
                                    f"MA7 cruzou MA25 + Fibonacci confirmou!\n"
                                    f"Oportunidade de lucro com spread maior.{grafico}"
                                )
                        else:
                            log.info(f"  {symbol}: ROI {roi:.1f}% | 3x #{n_3x + 1} preparado | aguardando sinal de reversao para lucrar")
                    except Exception as e:
                        log.warning(f"  Erro 3x {symbol}: {e}")
                # --- MONITORANDO (positivo mas abaixo do limiar de reversão) ---
                else:
                    # Saída apenas pelo trailing stop (sem reversão de MA)
                    if roi > 0:
                        # --- TOPUP AUTOMÁTICO (margem $1-$2, ROI positivo, MA a favor, rácio ok) ---
                        margem_atual = float(p.get("positionInitialMargin", 0))
                        ultimo_topup = alerta_dca_log.get(f"topup_{symbol}", 0)
                        topup_cooldown = time.time() - ultimo_topup >= 3600  # 1x por hora no máximo
                        if (1.0 <= margem_atual <= 2.0
                                and topup_cooldown
                                and get_racio_margem(client) < RACIO_MARGEM_MAX):
                            try:
                                df5 = get_candles(client, symbol, Client.KLINE_INTERVAL_5MINUTE, limit=30)
                                df5["ma7"]  = df5["close"].rolling(7).mean()
                                df5["ma25"] = df5["close"].rolling(25).mean()
                                c1 = df5.iloc[-1]
                                ma_favor = (direcao == "LONG" and c1["ma7"] > c1["ma25"]) or \
                                           (direcao == "SHORT" and c1["ma7"] < c1["ma25"])
                                if ma_favor:
                                    saldo_tp   = get_saldo_total(client)
                                    preco_tp   = float(client.futures_ticker(symbol=symbol)["lastPrice"])
                                    alav_tp    = alavancagem_dinamica(saldo_tp)
                                    margem_add = saldo_tp * 0.01
                                    step_tp    = get_step_size(client, symbol)
                                    qty_add    = arredondar_quantidade((margem_add * alav_tp) / preco_tp, step_tp)
                                    if qty_add > 0 and MODO == "real":
                                        side_tp = "BUY" if direcao == "LONG" else "SELL"
                                        client.futures_create_order(
                                            symbol=symbol, side=side_tp,
                                            type="MARKET", quantity=qty_add
                                        )
                                        alerta_dca_log[f"topup_{symbol}"] = time.time()
                                        topup_recente[symbol] = time.time()  # marca para não confundir com DCA
                                        log.info(f"  {symbol}: topup automatico +${margem_add:.2f} | margem era ${margem_atual:.2f}")
                                        telegram(f"<b>Topup automático: {symbol}</b>\n{direcao} | ROI {roi:+.1f}% | +${margem_add:.2f} adicionado")
                            except Exception as e:
                                log.warning(f"  Erro topup automatico {symbol}: {e}")

                        log.info(f"  {symbol}: ROI {roi:+.1f}% | monitorando")
                    else:
                        # DCA antecipado em -80%
                        if roi <= -80 and symbol not in dca_aplicado:
                            if dca_ativo and dca_ativo != symbol and dca_ativo_tem_sinal(client, abertas):
                                log.info(f"  {symbol}: ROI {roi:.1f}% | DCA -80% bloqueado ({dca_ativo} em recuperacao com sinal ativo)")
                            else:
                                try:
                                    df5 = get_candles(client, symbol, Client.KLINE_INTERVAL_5MINUTE, limit=30)
                                    df5["ma7"]  = df5["close"].rolling(7).mean()
                                    df5["ma25"] = df5["close"].rolling(25).mean()
                                    df5["diff"] = df5["ma7"] - df5["ma25"]
                                    diff_atual  = df5["diff"].iloc[-1]
                                    diff_prev   = df5["diff"].iloc[-3]

                                    # Stop -80% REMOVIDO — aguarda oportunidade de 3x
                                    # MA acelerando contra = não faz 3x agora, espera reverter

                                    ma_ok = ma_cruza_favor(client, symbol, direcao)
                                    if ma_ok:
                                        sinal_ma_detectado[symbol] = time.time()
                                        if dca_bloqueado_por_racio:
                                            log.warning(f"  {symbol}: DCA -80% bloqueado — Racio de Margem acima de {RACIO_BLOQUEIA_DCA:.0f}%")
                                        else:
                                            log.info(f"  {symbol}: ROI {roi:.1f}% + MA cruzou a favor -> DCA antecipado em -80%")
                                            aplicar_dca(client, p, banca)
                                            dca_ativo = symbol
                                            alerta_80_log[symbol] = time.time()
                                    else:
                                        ultimo_alerta_80 = alerta_80_log.get(symbol, 0)
                                        if time.time() - ultimo_alerta_80 >= 1800:
                                            msg = (
                                                f"<b>Atencao: {symbol} em {roi:.1f}%</b>\n"
                                                f"{direcao} | MA ainda nao reverteu\n"
                                                f"DCA automatico quando MA cruzar | Manual: /dca {symbol}"
                                            )
                                            telegram(msg)
                                            log.warning(f"  {symbol}: ROI {roi:.1f}% — aguardando MA para DCA -80%")
                                            alerta_80_log[symbol] = time.time()
                                except Exception as e:
                                    log.warning(f"  Erro DCA -80% {symbol}: {e}")

                        # Alerta de cruzamento de MA a favor em posição negativa
                        if roi < 0:
                            try:
                                ultimo_conv = alerta_dca_log.get(f"conv_{symbol}", 0)
                                if time.time() - ultimo_conv >= 300:
                                    if ma_cruza_favor(client, symbol, direcao):
                                        msg = (
                                            f"<b>{symbol}: MA7 cruzou a favor!</b>\n"
                                            f"{direcao} | ROI: {roi:+.1f}%\n"
                                            f"Cruzamento com momentum detectado.\n"
                                            f"Para fazer DCA agora: /dca {symbol}"
                                        )
                                        telegram(msg)
                                        log.info(f"  {symbol}: ROI {roi:+.1f}% | MA7 cruzou a favor — alerta enviado")
                                        alerta_dca_log[f"conv_{symbol}"] = time.time()
                                    else:
                                        log.info(f"  {symbol}: ROI {roi:+.1f}% | monitorando")
                            except Exception:
                                log.info(f"  {symbol}: ROI {roi:+.1f}% | monitorando")
                        else:
                            log.info(f"  {symbol}: ROI {roi:+.1f}% | monitorando")

            # --- RESUMO DIÁRIO E RESET ---
            agora_dt = datetime.now()
            if agora_dt.day != dia_atual:
                saldo_abertura_dia    = get_saldo_total(client)
                resumo_diario_enviado = False
                dia_atual             = agora_dt.day
                log.info(f"Novo dia. Saldo de abertura: ${saldo_abertura_dia:.2f}")

            if agora_dt.hour == RESUMO_HORA and not resumo_diario_enviado:
                enviar_resumo_diario(client, saldo_abertura_dia)
                resumo_diario_enviado = True

            # --- BUSCA DE NOVAS ENTRADAS (a cada 30 segundos) ---
            agora = time.time()
            if agora - ultimo_scan_entradas >= INTERVALO_ENTRADAS:
                ultimo_scan_entradas = agora

                saldo_atual_dia = get_saldo_total(client)
                max_pos_dinamico, risco_dinamico = limites_por_saldo(saldo_atual_dia)

                # --- LIMPEZA AUTOMÁTICA: fecha as menos promissoras se exceder o limite ---
                if len(abertas) > max_pos_dinamico:
                    excesso = len(abertas) - max_pos_dinamico
                    # Candidatas a fechar: negativas sem DCA e sem ROI alto
                    candidatas = [
                        p for p in abertas
                        if p["symbol"] not in dca_aplicado
                        and p["symbol"] not in posicoes_herdadas
                        and calcular_roi(p) < 30.0
                    ]
                    # Ordena por ROI crescente (piores primeiro)
                    candidatas.sort(key=lambda p: calcular_roi(p))
                    para_fechar = candidatas[:excesso]
                    for p in para_fechar:
                        symbol = p["symbol"]
                        roi    = calcular_roi(p)
                        amt    = float(p["positionAmt"])
                        lado   = "LONG" if amt > 0 else "SHORT"
                        side   = "SELL" if lado == "LONG" else "BUY"
                        try:
                            client.futures_create_order(
                                symbol=symbol, side=side, type="MARKET",
                                quantity=abs(amt), reduceOnly=True
                            )
                            peak_roi.pop(symbol, None)
                            ma_reverteu.pop(symbol, None)
                            posicao_abertura.pop(symbol, None)
                            log.warning(f"Limpeza automatica: {symbol} {lado} ROI {roi:+.1f}% fechado (excesso de posicoes)")
                            telegram(f"<b>Limpeza automatica:</b> {symbol} {lado} ROI {roi:+.1f}%\nFechado por excesso ({len(abertas)}/{max_pos_dinamico} posicoes)")
                            time.sleep(0.5)
                        except BinanceAPIException as e:
                            log.error(f"Erro limpeza automatica {symbol}: {e}")

                if len(abertas) >= max_pos_dinamico:
                    log.info(f"Maximo dinamico atingido ({len(abertas)}/{max_pos_dinamico} posicoes | saldo ${saldo_atual_dia:.0f}).")
                elif (racio_atual := get_racio_margem(client)) >= RACIO_MARGEM_MAX:
                    log.info(f"Racio de Margem {racio_atual:.2f}% >= limite {RACIO_MARGEM_MAX:.0f}%. Sem novas entradas.")
                else:
                    sessao = sessao_atual()
                    # Ajustes dinâmicos por sessão
                    if sessao == "ASIATICA":
                        top_pares_sessao   = min(TOP_PARES + 100, 652)  # +100 pares extras
                        timeout_camada2    = 300   # 5 min para liberar camada 2
                        racio_limite_sessao = RACIO_MARGEM_MAX  # normal
                    elif sessao == "AMERICANA":
                        top_pares_sessao   = TOP_PARES
                        timeout_camada2    = TIMEOUT_SEM_ENTRADA
                        racio_limite_sessao = max(RACIO_MARGEM_MAX - 2.0, 4.0)  # -2% no horário americano
                    else:  # EUROPEIA
                        top_pares_sessao   = TOP_PARES
                        timeout_camada2    = TIMEOUT_SEM_ENTRADA
                        racio_limite_sessao = RACIO_MARGEM_MAX

                    # Revalida rácio com limite da sessão
                    if (racio_atual := get_racio_margem(client)) >= racio_limite_sessao:
                        log.info(f"[{sessao}] Racio {racio_atual:.2f}% >= limite sessao {racio_limite_sessao:.0f}%. Sem novas entradas.")
                        continue

                    log.info(f"Sessao {sessao} | Escaneando {top_pares_sessao} pares | Racio limite {racio_limite_sessao:.0f}%")
                    btc_tendencia = tendencia_btc(client)
                    sem_entrada_ha = time.time() - ultimo_entrada
                    camada2_ativa  = sem_entrada_ha >= timeout_camada2
                    camada_txt     = "CAMADA 2 (sem MA99)" if camada2_ativa else "CAMADA 1 (com MA99)"
                    log.info(f"BTC: {btc_tendencia.upper()} | {camada_txt} | timeout camada2={timeout_camada2//60}min")

                    pares = get_top_pares(client, top_pares_sessao)
                    simbolos_abertos = [p["symbol"] for p in abertas]
                    pares_filtrados = [s for s in pares if s not in simbolos_abertos]

                    sinais_encontrados = []
                    lock_sinais = threading.Lock()

                    def analisar_par(symbol):
                        try:
                            # Sistema Águia Spread: Bollinger Squeeze no 2H
                            sinal = sinal_aguia_spread(client, symbol)
                            if not sinal:
                                return

                            # Copy trading: se master configurado, só entra se master tem posição positiva
                            master_pos = get_master_positions()
                            if master_pos:
                                if symbol not in master_pos:
                                    return
                                if master_pos[symbol]["direcao"] != sinal:
                                    return

                            preco = float(client.futures_symbol_ticker(symbol=symbol)["price"])

                            # Classifica: ativos do Bruno = AGUIA, outros = SPREAD
                            qualidade = "AGUIA" if symbol in PARES_BRUNO else "SPREAD"
                            if master_pos:
                                qualidade = "COPY"

                            with lock_sinais:
                                sinais_encontrados.append((symbol, sinal, "squeeze", preco, qualidade))
                        except Exception as e:
                            log.warning(f"Erro ao analisar {symbol}: {e}")

                    with ThreadPoolExecutor(max_workers=THREADS_VARREDURA) as executor:
                        executor.map(analisar_par, pares_filtrados)

                    # --- Modo Bruno: detecta volume anormal nos ativos do Bruno ---
                    sinais_bruno = detectar_sinais_bruno(client, simbolos_abertos)
                    sinais_encontrados.extend(sinais_bruno)

                    # Ordena: AGUIA (ativos Bruno) primeiro, depois COPY, depois SPREAD
                    ordem_qualidade = {"AGUIA": 0, "BRUNO": 1, "COPY": 2, "SPREAD": 3, "PREMIUM": 4, "NORMAL": 5}
                    sinais_encontrados.sort(key=lambda x: ordem_qualidade.get(x[4], 9))

                    for symbol, sinal, direcao_tf, preco, qualidade in sinais_encontrados:
                        if len(posicoes_abertas(client)) >= max_pos_dinamico:
                            break
                        if get_racio_margem(client) >= RACIO_MARGEM_MAX:
                            log.info(f"Racio limite atingido — parando entradas.")
                            break
                        log.info(f"Sinal {sinal} [{qualidade}] em {symbol} | Preco: {preco}")
                        abrir_posicao(client, symbol, sinal, preco, banca, qualidade, risco_dinamico)
                        ultimo_entrada = time.time()

                    n_bruno = len(sinais_bruno)
                    n_normal = len(sinais_encontrados) - n_bruno
                    log.info(f"Varredura concluida: {len(pares_filtrados)} pares | {n_normal} sinais MA | {n_bruno} sinais BRUNO")

            # Resumo horário
            if time.time() - ultimo_resumo_hora >= 3600:
                enviar_resumo_hora(client, saldo_abertura)
                ultimo_resumo_hora = time.time()

            # Auto-update: verifica GitHub a cada 5 minutos e reinicia se houver nova versão
            if time.time() - ultimo_check_update >= 300:
                verificar_atualizacao(reiniciar=True)
                ultimo_check_update = time.time()

            # Check mais rápido quando tem 3x ativo ou posição perto do gatilho
            if dca_aplicado or any(calcular_roi(p) <= -180 for p in abertas if float(p["positionAmt"]) != 0):
                time.sleep(5)   # 5s — monitora de perto o 3x e oportunidade
            else:
                time.sleep(INTERVALO_POSICOES)

        except KeyboardInterrupt:
            log.info("Nunes encerrado pelo usuario.")
            telegram("Nunes encerrado.")
            break
        except Exception as e:
            log.error(f"Erro geral: {e}")
            time.sleep(30)


if __name__ == "__main__":
    main()
