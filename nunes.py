#!/usr/bin/env python3
"""
Robô de trade - Binance Futuros
Guardião CNS v2.1 — Sistema Cristiano Nunes Silva
Evolução do Águia Spread (Bruno Aguiar) sem muletas psicológicas:

ENTRADA:
- Tendência H4 (EMA21) como filtro macro (multi-timeframe Bruno)
- Bollinger Squeeze H2 (contração + release)
- ATR H2 >= 2% (elimina tokens mortos)
- Volume release >= 1.2x média
- Candle fechado fora da banda + 2 candles confirmando
- Filtro BTC + bloqueio contra-maré

GESTÃO:
- Sistema de Tiers (A: 1.5x, B: 1.0x, C: 0.7x) — peso por qualidade
- DCA 3x em duas camadas: -120% (primeiro reforço) e -240% (agressivo)
- 3x escalonado pela profundidade (audacioso < -1000%)
- MA7 cruzando MA25 + Fibonacci + 1min alinhado

SAÍDA:
- Alvo mediano Bruno: fecha 50% em ROI +200% (10% movimento)
- Trailing escalonado (5%/20%/50% por faixa de pico)
- Saída 90/10 pós-3x
- Timeout 30 dias (descarta dead trades)

PROTEÇÃO:
- Rácio de Margem 6% máximo (única proteção técnica real)
- Notional mínimo $5.50 garantido

v2.1 REMOVEU (muletas psicológicas do curso Bruno):
- Circuit breaker diário (bot não tem pânico)
- Timeout 30 dias (não existe dead trade — DCA resolve)
- Confirmação dupla da meta (latência desnecessária)
- Alertas "ciclo em risco" com sugestões de ação
- Separação contábil de posições herdadas
- LIMITE_PERDA_DIARIA (era código morto)

O que supera o Bruno:
1. Squeeze + ATR (ele não filtra contração/tokens mortos)
2. Trailing dinâmico (ele usa alvo fixo)
3. Lista moderna Tier A + B + C (ele só opera 4 ativos)
4. Gestão de margem real (ele roda 50x cego)
5. Log honesto com wins e losses (ele só mostra wins)
6. Aprendizado automático via aprendizados.json
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
ALAVANCAGEM        = int(os.getenv("ALAVANCAGEM", "20"))
RISCO_POR_TRADE    = float(os.getenv("RISCO_POR_TRADE", "0.01"))  # 1% padrão CNS
RACIO_MARGEM_MAX   = float(os.getenv("RACIO_MARGEM_MAX", "6.0"))  # % máximo do Rácio de Margem da Binance

MAX_POSICOES          = 10   # padrão CNS: 10 posições fixas


def limites_por_saldo(saldo: float) -> tuple[int, float]:
    """
    Sistema Guardião (CNS): 10 posições fixas, 1% por trade.
    Uniforme para todos os saldos. Notional mínimo $5 da Binance é
    garantido em abrir_posicao() quando o saldo é muito baixo.
    """
    return 10, 0.01

TOP_PARES             = 326  # quantos pares por volume monitorar (50% do mercado)
THREADS_VARREDURA     = 10   # pares analisados em paralelo
INTERVALO_POSICOES    = 15   # segundos entre verificação de posições (rápido para pegar 3x)
INTERVALO_ENTRADAS    = 60   # segundos entre busca de novas entradas (2H timeframe)
RESUMO_HORA           = 22   # hora do resumo diário (horário local)
META_CICLO_PCT        = float(os.getenv("META_CICLO_PCT", "5.0"))   # meta de lucro por ciclo (%)
META_CICLO_FASE2_USD  = float(os.getenv("META_CICLO_FASE2_USD", "50.0"))  # meta fixa em USDT após $1.000
META_CICLO_FASE2_MIN  = float(os.getenv("META_CICLO_FASE2_MIN", "1000.0")) # saldo para ativar fase 2

# ---------------------------------------------------------------------------
# Modo CNS: ativos prioritários + detecção de volume anormal
# ---------------------------------------------------------------------------
# Guardião CNS v2 — Sistema de Tiers baseado em análise do Bruno
# Tier A: "Núcleo" peso 1.5x — ativos que o Bruno opera 93% do tempo
# Tier B: "Narrativa moderna" peso 1.0x — tokens 2025/26
# Tier C: "Oportunistas" peso 0.7x — liquidez menor ou teses incertas
TIERS_CNS = {
    # Tier A — Núcleo (peso 1.5x): validados pelo histórico do Bruno
    "BTCUSDT": 1.5, "ETHUSDT": 1.5, "SOLUSDT": 1.5, "DOGEUSDT": 1.5,
    # Tier B — Narrativa moderna (peso 1.0x)
    "HYPEUSDT": 1.0, "TAOUSDT": 1.0, "BERAUSDT": 1.0, "SAHARAUSDT": 1.0,
    "ENAUSDT": 1.0, "WLDUSDT": 1.0, "KITEUSDT": 1.0, "NIGHTUSDT": 1.0,
    "XPLUSDT": 1.0, "HEMIUSDT": 1.0, "SKYAIUSDT": 1.0, "JTOUSDT": 1.0,
    "RENDERUSDT": 1.0, "FARTCOINUSDT": 1.0, "FETUSDT": 1.0, "ZKUSDT": 1.0,
    # Tier C — Oportunistas (peso 0.7x)
    "ALGOUSDT": 0.7, "POLYXUSDT": 0.7, "RIVERUSDT": 0.7, "BEATUSDT": 0.7,
    "PLAYUSDT": 0.7, "KERNELUSDT": 0.7, "COSUSDT": 0.7, "ZETAUSDT": 0.7,
    "MONUSDT": 0.7, "SIGNUSDT": 0.7, "HUSDT": 0.7, "VVVUSDT": 0.7,
    "MUSDT": 0.7, "CUSDT": 0.7, "GIGGLEUSDT": 0.7, "LITUSDT": 0.7,
    "GASUSDT": 0.7, "CRVUSDT": 0.7, "DASHUSDT": 0.7, "APTUSDT": 0.7,
}
PARES_CNS = list(TIERS_CNS.keys())

def peso_tier(symbol: str) -> float:
    """Retorna o peso do ativo baseado no tier (1.5/1.0/0.7)."""
    return TIERS_CNS.get(symbol, 0.5)  # ativos fora da lista: peso mínimo

CNS_VOLUME_MULT = 3.0   # volume atual >= 3x a média = spike
CNS_HORARIO_INICIO = 1  # 01:00 BRT
CNS_HORARIO_FIM    = 9  # 09:00 BRT

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
                        pico_pos_3x.pop(symbol, None)
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

                # Projeção Guardião: banca inicial * 1.55 ^ (ciclos/10)
                # Cada 10 ciclos = +55% sobre a banca
                saldo_total  = get_saldo_total(client)
                banca_inicio = saldo_total - lucro_total
                ciclos       = trades_count
                projecao_guardiao = [
                    (0,   banca_inicio),
                    (10,  banca_inicio * (1.55 ** 1)),
                    (20,  banca_inicio * (1.55 ** 2)),
                    (30,  banca_inicio * (1.55 ** 3)),
                    (50,  banca_inicio * (1.55 ** 5)),
                    (100, banca_inicio * (1.55 ** 10)),
                    (128, banca_inicio * (1.55 ** 12.8)),
                ]
                # Acha em qual etapa da projeção está
                etapa_atual = max((c for c, _ in projecao_guardiao if c <= ciclos), default=0)
                prox_etapa  = min((c for c, _ in projecao_guardiao if c > ciclos), default=128)
                meta_prox   = dict(projecao_guardiao)[prox_etapa]

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
                    f"\n<b>Projecao Guardiao</b>\n"
                    f"Voce esta no ciclo {ciclos}\n"
                    f"Meta proxima ({prox_etapa} ciclos): ${meta_prox:.2f}\n"
                    f"Para chegar a $1M: 128 ciclos\n\n"
                    f"Projecao completa:\n"
                )
                for ciclo, valor in projecao_guardiao:
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
    df["open"]   = df["open"].astype(float)
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
_cns_alertados: dict[str, float] = {}  # symbol -> timestamp do último alerta CNS


def detectar_sinais_cns(client: Client, simbolos_abertos: list[str]) -> list[tuple]:
    """
    Modo CNS: detecta volume anormal nos ativos prioritários.
    Só opera LONG. Foca no horário 01:00-09:00 BRT.
    Retorna lista de (symbol, "LONG", "alta", preco, "CNS").
    """
    global _cns_alertados
    hora_local = datetime.now().hour

    sinais = []
    for symbol in PARES_CNS:
        if symbol in simbolos_abertos:
            continue
        # Não repetir alerta do mesmo ativo em menos de 4 horas
        if symbol in _cns_alertados and time.time() - _cns_alertados[symbol] < 14400:
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
            if vol_ratio < CNS_VOLUME_MULT:
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

            # Prioriza horário CNS (01-09h BRT) mas aceita fora também se volume for muito alto
            if CNS_HORARIO_INICIO <= hora_local <= CNS_HORARIO_FIM:
                min_vol = CNS_VOLUME_MULT
            else:
                min_vol = CNS_VOLUME_MULT * 2  # fora do horário, exige 6x

            if vol_ratio >= min_vol:
                sinais.append((symbol, "LONG", "alta", preco_ticker, "CNS"))
                _cns_alertados[symbol] = time.time()
                log.info(f"  [CNS] {symbol}: volume {vol_ratio:.1f}x a media | MA7 > MA25 1H | LONG")
                telegram(
                    f"<b>[CNS] Sinal detectado: {symbol}</b>\n"
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
sinal_ma_detectado:    dict[str, float] = {}   # timestamp da última vez que MA cruzou a favor por symbol
roi_no_dca:            dict[str, float] = {}   # ROI no momento em que o DCA foi aplicado
posicoes_herdadas:     set[str]        = set() # symbols herdados de ciclos anteriores (negativos não fechados)
margem_registrada:     dict[str, float] = {}  # margem inicial registrada por symbol para detectar DCA manual
topup_recente:         dict[str, float] = {}  # symbols que tiveram topup recente (ignora na detecção DCA)
score_historico:       dict[str, list] = {}  # symbol -> [(timestamp, score, preco)] para validar continuidade
posicoes_cns:        set[str]        = set() # symbols que entraram pelo modo CNS
parcial_500:           set[str]        = set() # symbols que já tiveram saída parcial em +500%
parcial_10pct:         set[str]        = set() # symbols que já fecharam 50% em +10% ROI
pico_pos_3x:           dict[str, float] = {}  # pico de ROI apos o 3x (para trailing escalonado)
topup_equilibrar:      dict[str, float] = {}  # symbol -> margem faltante para equilibrar a 3% da banca

ESTADO_FILE = "C:/robo-trade/estado_bot.json"

def meta_dinamica(ciclos_positivos: int) -> float:
    """
    Meta dinâmica: base META_CICLO_PCT, sobe 0.5% a cada 5 ciclos positivos.
    Teto = META_CICLO_PCT + 2% (ex: se .env = 5%, teto = 7%).
    """
    incrementos = ciclos_positivos // 5
    meta = META_CICLO_PCT + incrementos * 0.5
    return min(meta, META_CICLO_PCT + 2.0)


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


APRENDIZADOS_FILE = "C:/robo-trade/aprendizados.json"

def registrar_aprendizado(client: Client, symbol: str, direcao: str, tipo: str, roi_final: float, detalhes: str = ""):
    """
    Registra caso de estudo (sucesso ou fracasso) para aprendizado do bot.
    Inclui snapshot completo de 1min, 5min, 1H e Fibonacci no momento.
    """
    try:
        df5 = get_candles(client, symbol, Client.KLINE_INTERVAL_5MINUTE, limit=50)
        df5["ma7"] = df5["close"].rolling(7).mean()
        df5["ma25"] = df5["close"].rolling(25).mean()
        df5["ma99"] = df5["close"].rolling(99).mean()

        df1 = get_candles(client, symbol, Client.KLINE_INTERVAL_1MINUTE, limit=30)
        df1["ma7"] = df1["close"].rolling(7).mean()
        df1["ma25"] = df1["close"].rolling(25).mean()

        df1h = get_candles(client, symbol, Client.KLINE_INTERVAL_1HOUR, limit=30)
        df1h["ma7"] = df1h["close"].rolling(7).mean()
        df1h["ma25"] = df1h["close"].rolling(25).mean()

        preco = df5["close"].iloc[-1]
        high_20 = df5["high"].iloc[-20:].max()
        low_20 = df5["low"].iloc[-20:].min()

        caso = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol,
            "direcao": direcao,
            "tipo": tipo,  # "3x_sucesso", "3x_fracasso", "entrada_sucesso", "entrada_fracasso"
            "roi_final": roi_final,
            "preco": float(preco),
            "detalhes": detalhes,
            "snapshot": {
                "1min": {
                    "ma7": float(df1["ma7"].iloc[-1]),
                    "ma25": float(df1["ma25"].iloc[-1]),
                    "ma7_acima_ma25": bool(df1["ma7"].iloc[-1] > df1["ma25"].iloc[-1]),
                },
                "5min": {
                    "ma7": float(df5["ma7"].iloc[-1]),
                    "ma25": float(df5["ma25"].iloc[-1]),
                    "ma99": float(df5["ma99"].iloc[-1]) if pd.notna(df5["ma99"].iloc[-1]) else None,
                    "ma7_acima_ma25": bool(df5["ma7"].iloc[-1] > df5["ma25"].iloc[-1]),
                    "ma7_acima_ma99": bool(df5["ma7"].iloc[-1] > df5["ma99"].iloc[-1]) if pd.notna(df5["ma99"].iloc[-1]) else None,
                },
                "1h": {
                    "ma7": float(df1h["ma7"].iloc[-1]),
                    "ma25": float(df1h["ma25"].iloc[-1]),
                    "ma7_acima_ma25": bool(df1h["ma7"].iloc[-1] > df1h["ma25"].iloc[-1]),
                },
                "fibonacci": {
                    "suporte": float(low_20),
                    "topo": float(high_20),
                    "fib_382": float(low_20 + (high_20 - low_20) * 0.382),
                    "fib_500": float(low_20 + (high_20 - low_20) * 0.500),
                    "fib_618": float(low_20 + (high_20 - low_20) * 0.618),
                    "preco_zona": "acima_618" if preco >= low_20 + (high_20 - low_20) * 0.618 else
                                  "382_618" if preco >= low_20 + (high_20 - low_20) * 0.382 else
                                  "abaixo_382"
                }
            }
        }

        # Carrega aprendizados existentes e adiciona
        try:
            with open(APRENDIZADOS_FILE, "r") as f:
                aprendizados = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            aprendizados = []

        aprendizados.append(caso)
        with open(APRENDIZADOS_FILE, "w") as f:
            json.dump(aprendizados, f, indent=2)

        resultado = "SUCESSO" if "sucesso" in tipo else "FRACASSO"
        log.info(f"  [APRENDIZADO] {resultado}: {symbol} {direcao} | ROI: {roi_final:+.1f}% | {detalhes}")
        telegram(
            f"<b>Aprendizado registrado: {resultado}</b>\n"
            f"{symbol} {direcao} | ROI final: {roi_final:+.1f}%\n"
            f"1min: MA7 {'>' if caso['snapshot']['1min']['ma7_acima_ma25'] else '<'} MA25\n"
            f"5min: MA7 {'>' if caso['snapshot']['5min']['ma7_acima_ma25'] else '<'} MA25\n"
            f"1H: MA7 {'>' if caso['snapshot']['1h']['ma7_acima_ma25'] else '<'} MA25\n"
            f"Fib: {caso['snapshot']['fibonacci']['preco_zona']}\n"
            f"{detalhes}"
        )
    except Exception as e:
        log.warning(f"  Erro ao registrar aprendizado: {e}")

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


def calcular_volume_profile(df: pd.DataFrame, n_buckets: int = 24) -> dict:
    """
    Calcula Volume Profile (barras laterais de volume) a partir de um DataFrame de candles.
    Retorna POC, VAL, VAH e zonas de alto/baixo volume.

    - POC (Point of Control): preço com maior volume
    - VAL (Value Area Low): limite inferior dos 70% de volume
    - VAH (Value Area High): limite superior dos 70% de volume
    """
    if len(df) < 10:
        return {"poc": 0, "val": 0, "vah": 0, "valid": False}

    preco_min = df["low"].min()
    preco_max = df["high"].max()
    if preco_max <= preco_min:
        return {"poc": 0, "val": 0, "vah": 0, "valid": False}

    # Cria buckets de preço
    bucket_size = (preco_max - preco_min) / n_buckets
    buckets = [0.0] * n_buckets

    # Distribui o volume de cada candle nos buckets que ele atravessa
    for _, row in df.iterrows():
        low = row["low"]
        high = row["high"]
        volume = row["volume"]
        if high <= low:
            continue
        # Distribui o volume proporcionalmente entre os buckets que o candle cobre
        idx_low = max(0, int((low - preco_min) / bucket_size))
        idx_high = min(n_buckets - 1, int((high - preco_min) / bucket_size))
        n_atingidos = idx_high - idx_low + 1
        if n_atingidos > 0:
            vol_por_bucket = volume / n_atingidos
            for i in range(idx_low, idx_high + 1):
                buckets[i] += vol_por_bucket

    # POC: bucket com maior volume
    poc_idx = buckets.index(max(buckets))
    poc_preco = preco_min + (poc_idx + 0.5) * bucket_size

    # Value Area: 70% do volume total centrado no POC
    volume_total = sum(buckets)
    volume_alvo = volume_total * 0.70
    volume_acum = buckets[poc_idx]
    lo_idx = hi_idx = poc_idx

    while volume_acum < volume_alvo and (lo_idx > 0 or hi_idx < n_buckets - 1):
        vol_lo = buckets[lo_idx - 1] if lo_idx > 0 else 0
        vol_hi = buckets[hi_idx + 1] if hi_idx < n_buckets - 1 else 0
        if vol_lo >= vol_hi and lo_idx > 0:
            lo_idx -= 1
            volume_acum += vol_lo
        elif hi_idx < n_buckets - 1:
            hi_idx += 1
            volume_acum += vol_hi
        else:
            break

    val = preco_min + lo_idx * bucket_size
    vah = preco_min + (hi_idx + 1) * bucket_size

    return {
        "poc": poc_preco,
        "val": val,
        "vah": vah,
        "preco_min": preco_min,
        "preco_max": preco_max,
        "valid": True
    }


def validar_continuidade_score(symbol: str, score_atual: int, preco_atual: float, direcao: str) -> tuple[bool, str]:
    """
    Valida se o score tem CONTINUIDADE antes de disparar o 3x.

    Regra: nao dispara em picos isolados de score. Exige que:
    1. Score >= 50 no momento atual
    2. Score tambem estava >= 40 ha pelo menos 2 minutos atras
    3. Preco se moveu a favor desde o primeiro sinal (nao falso breakout)

    Retorna (pode_disparar, motivo).
    """
    global score_historico
    agora = time.time()

    # Registra o ponto atual no historico
    if symbol not in score_historico:
        score_historico[symbol] = []
    score_historico[symbol].append((agora, score_atual, preco_atual))

    # Limita a 10 pontos (remove os mais antigos)
    score_historico[symbol] = score_historico[symbol][-10:]

    # Limpa pontos muito antigos (> 10 min)
    score_historico[symbol] = [
        (t, s, p) for (t, s, p) in score_historico[symbol]
        if agora - t < 600
    ]

    historico = score_historico[symbol]

    # Precisa de pelo menos 2 pontos com >= 2 minutos de diferenca
    if len(historico) < 2:
        return False, "primeira deteccao — aguardando confirmacao"

    # Pega o ponto mais antigo dentro de 2-5 min
    ponto_antigo = None
    for t, s, p in historico:
        if 120 <= (agora - t) <= 300:  # entre 2 e 5 minutos atras
            ponto_antigo = (t, s, p)
            break

    if not ponto_antigo:
        return False, "sem historico de 2-5 min atras — aguardando"

    t_antigo, score_antigo, preco_antigo = ponto_antigo

    # Regra 1: score antigo >= 40 (tinha qualidade ha 2+ minutos)
    if score_antigo < 40:
        return False, f"score nao era forte ha 2min (era {score_antigo})"

    # Regra 2: preco se moveu a favor
    if direcao == "LONG":
        mov_pct = (preco_atual - preco_antigo) / preco_antigo * 100
        if mov_pct < 0.2:
            return False, f"preco nao subiu o suficiente ({mov_pct:+.2f}%)"
    else:  # SHORT
        mov_pct = (preco_antigo - preco_atual) / preco_antigo * 100
        if mov_pct < 0.2:
            return False, f"preco nao caiu o suficiente ({mov_pct:+.2f}%)"

    return True, f"confirmado (score ha 2min: {score_antigo}, mov: {mov_pct:+.2f}%)"


def detectar_padrao_reversao(client: Client, symbol: str, direcao: str) -> tuple[bool, str]:
    """
    Detecta padroes de exaustao que sinalizam reversao iminente.
    Bloqueia o 3x quando o trade seria contra o contexto maior.

    Regra do Bruno: "nada sobe para sempre, nada desce para sempre".
    Uma hora vai convergir a favor.

    Padroes que bloqueiam SHORT:
    - MA99 abaixo do preco (suporte forte) + MA7 desacelerando a queda
    - MA99 subindo + MA25 lateral = formacao de fundo em tendencia de alta

    Padroes que bloqueiam LONG:
    - MA99 acima do preco (resistencia forte) + MA7 desacelerando a alta
    - MA99 caindo + MA25 lateral = formacao de topo em tendencia de baixa

    Retorna (bloqueado, motivo).
    """
    try:
        df = get_candles(client, symbol, Client.KLINE_INTERVAL_5MINUTE, limit=120)
        df["ma7"] = df["close"].rolling(7).mean()
        df["ma25"] = df["close"].rolling(25).mean()
        df["ma99"] = df["close"].rolling(99).mean()

        preco = df["close"].iloc[-1]
        ma7_0 = df["ma7"].iloc[-1]
        ma7_5 = df["ma7"].iloc[-5]
        ma7_10 = df["ma7"].iloc[-10]
        ma25_0 = df["ma25"].iloc[-1]
        ma25_10 = df["ma25"].iloc[-10]
        ma99_0 = df["ma99"].iloc[-1]
        ma99_10 = df["ma99"].iloc[-10]

        if pd.isna(ma99_0) or pd.isna(ma99_10):
            return False, "sem MA99 suficiente"

        # Slopes (variacao percentual)
        slope_ma7 = (ma7_0 - ma7_10) / ma7_10 * 100 if ma7_10 > 0 else 0
        slope_ma25 = (ma25_0 - ma25_10) / ma25_10 * 100 if ma25_10 > 0 else 0
        slope_ma99 = (ma99_0 - ma99_10) / ma99_10 * 100 if ma99_10 > 0 else 0

        # Aceleracao do MA7 (slope atual vs slope 5 candles atras)
        slope_ma7_recente = (ma7_0 - ma7_5) / ma7_5 * 100 if ma7_5 > 0 else 0

        if direcao == "SHORT":
            # Padrao: fundo em formacao, nao shortar
            # MA99 subindo + preco acima da MA99 = suporte forte
            preco_acima_ma99 = preco > ma99_0
            ma99_subindo = slope_ma99 >= 0.2  # subindo claramente
            ma25_fraca = slope_ma25 >= -0.5  # leve queda ou lateral (tolera correcao normal)
            # MA7 desacelerando a queda
            ma7_desacelerando = slope_ma7_recente > slope_ma7 and slope_ma7 < 0

            if preco_acima_ma99 and ma99_subindo and ma25_fraca and ma7_desacelerando:
                return True, f"fundo em formacao (MA99 +{slope_ma99:.2f}% suporte, MA25 {slope_ma25:+.2f}% fraca, MA7 desacelerando)"

        else:  # LONG
            # Padrao: topo em formacao, nao comprar
            preco_abaixo_ma99 = preco < ma99_0
            ma99_descendo = slope_ma99 <= -0.2
            ma25_fraca = slope_ma25 <= 0.5
            ma7_desacelerando = slope_ma7_recente < slope_ma7 and slope_ma7 > 0

            if preco_abaixo_ma99 and ma99_descendo and ma25_fraca and ma7_desacelerando:
                return True, f"topo em formacao (MA99 {slope_ma99:+.2f}% resistencia, MA25 {slope_ma25:+.2f}% fraca, MA7 desacelerando)"

    except Exception:
        pass

    return False, ""


def calcular_score_3x(client: Client, symbol: str, direcao: str) -> tuple[int, dict]:
    """
    Sistema de score CNS para 3x automatico (0-110 pontos).
    Retorna (score, detalhes_dict).

    Criterios:
    1. Candle 1 cruzou MA25 (20 pts)
    2. Candle 2 separando (20 pts)
    3. Corpo do candle 2 forte >= 60% do range (15 pts)
    4. Candle 2 na direcao correta (10 pts)
    5. 1min alinhado (15 pts)
    6. Fibonacci favoravel (10 pts)
    7. Volume do candle 2 >= 1.2x media (10 pts)
    8. Volume Profile: preco em zona de reversao (10 pts) — NOVO

    Gatilho do 3x automatico: score >= 50
    """
    detalhes = {}
    score = 0
    try:
        # --- 5min ---
        df5 = get_candles(client, symbol, Client.KLINE_INTERVAL_5MINUTE, limit=50)
        df5["ma7"]  = df5["close"].rolling(7).mean()
        df5["ma25"] = df5["close"].rolling(25).mean()
        df5["vol_media"] = df5["volume"].rolling(20).mean()

        c3 = df5.iloc[-3]  # antes do cruzamento
        c2 = df5.iloc[-2]  # candle 1: cruzamento (fechado)
        c1 = df5.iloc[-1]  # candle 2: confirmacao (ainda aberto ou recem-fechado)

        # 1. CANDLE 1 CRUZOU (20 pts)
        if direcao == "LONG":
            cruzou = c3["ma7"] <= c3["ma25"] and c2["ma7"] > c2["ma25"]
        else:
            cruzou = c3["ma7"] >= c3["ma25"] and c2["ma7"] < c2["ma25"]
        if cruzou:
            score += 20
            detalhes["candle_1_cruzou"] = True
        else:
            detalhes["candle_1_cruzou"] = False
            # Aceita tambem cruzamento um pouco mais antigo (ate 3 candles)
            if direcao == "LONG" and c1["ma7"] > c1["ma25"]:
                score += 15  # penalidade por nao ser cruzamento recente
                detalhes["candle_1_cruzou"] = "recente"
            elif direcao == "SHORT" and c1["ma7"] < c1["ma25"]:
                score += 15
                detalhes["candle_1_cruzou"] = "recente"

        # 2. CANDLE 2 SEPARANDO (20 pts)
        if direcao == "LONG":
            sep_c2 = c2["ma7"] - c2["ma25"]
            sep_c1 = c1["ma7"] - c1["ma25"]
            separando = sep_c1 > sep_c2
        else:
            sep_c2 = c2["ma25"] - c2["ma7"]
            sep_c1 = c1["ma25"] - c1["ma7"]
            separando = sep_c1 > sep_c2
        if separando:
            score += 20
            detalhes["candle_2_separando"] = True
        else:
            detalhes["candle_2_separando"] = False

        # 3. CORPO DO CANDLE 2 FORTE >= 60% (15 pts)
        range_c1 = c1["high"] - c1["low"]
        corpo_c1 = abs(c1["close"] - c1["open"])
        corpo_pct = (corpo_c1 / range_c1) if range_c1 > 0 else 0
        detalhes["corpo_pct"] = round(corpo_pct * 100, 1)
        if corpo_pct >= 0.6:
            score += 15
            detalhes["corpo_forte"] = True
        elif corpo_pct >= 0.4:
            score += 8  # corpo medio
            detalhes["corpo_forte"] = "medio"
        else:
            detalhes["corpo_forte"] = False

        # 4. CANDLE 2 NA DIRECAO (10 pts)
        c1_verde = c1["close"] > c1["open"]
        c1_vermelho = c1["close"] < c1["open"]
        if direcao == "LONG" and c1_verde:
            score += 10
            detalhes["candle_cor_ok"] = True
        elif direcao == "SHORT" and c1_vermelho:
            score += 10
            detalhes["candle_cor_ok"] = True
        else:
            detalhes["candle_cor_ok"] = False

        # 5. 1MIN ALINHADO (15 pts)
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
        if alinhado_1m:
            score += 10
            if acelerando_1m:
                score += 5
                detalhes["1min"] = "alinhado_acelerando"
            else:
                detalhes["1min"] = "alinhado"
        else:
            detalhes["1min"] = "nao_alinhado"

        # 6. FIBONACCI FAVORAVEL (10 pts)
        high_20 = df5["high"].iloc[-20:].max()
        low_20  = df5["low"].iloc[-20:].min()
        fib_382 = low_20 + (high_20 - low_20) * 0.382
        fib_618 = low_20 + (high_20 - low_20) * 0.618
        preco   = df5["close"].iloc[-1]
        if direcao == "LONG":
            fib_ok = preco >= fib_382
        else:
            fib_ok = preco <= fib_618
        if fib_ok:
            score += 10
            detalhes["fibonacci"] = "favoravel"
        else:
            detalhes["fibonacci"] = "contra"

        # 7. VOLUME DO CANDLE 2 >= 1.2x MEDIA (10 pts)
        vol_atual = c1["volume"]
        vol_media = c1["vol_media"] if pd.notna(c1["vol_media"]) else 0
        if vol_media > 0:
            vol_ratio = vol_atual / vol_media
            detalhes["vol_ratio"] = round(vol_ratio, 2)
            if vol_ratio >= 1.2:
                score += 10
                detalhes["volume_forte"] = True
            elif vol_ratio >= 0.8:
                score += 5
                detalhes["volume_forte"] = "medio"
            else:
                detalhes["volume_forte"] = False
        else:
            detalhes["volume_forte"] = "sem_dados"

        # 8. VOLUME PROFILE: preço em zona de reversão forte (10 pts)
        # LONG: próximo do VAL (suporte forte) ou abaixo do POC (recuperação)
        # SHORT: próximo do VAH (resistência forte) ou acima do POC (distribuição)
        vp = calcular_volume_profile(df5.tail(50), n_buckets=24)
        if vp["valid"]:
            preco_atual = c1["close"]
            val = vp["val"]
            vah = vp["vah"]
            poc = vp["poc"]
            range_va = vah - val if vah > val else 1

            if direcao == "LONG":
                # Distância do preço ao VAL (normalizada)
                dist_val = (preco_atual - val) / range_va
                if dist_val <= 0.25:
                    # Preço colado no VAL — zona de suporte forte
                    score += 10
                    detalhes["vol_profile"] = "VAL_suporte_forte"
                elif dist_val <= 0.5 and preco_atual < poc:
                    # Abaixo do POC — zona de recuperação
                    score += 7
                    detalhes["vol_profile"] = "abaixo_POC_recuperacao"
                elif preco_atual < vah:
                    score += 3
                    detalhes["vol_profile"] = "dentro_VA_neutro"
                else:
                    detalhes["vol_profile"] = "acima_VAH_contra_long"
            else:  # SHORT
                dist_vah = (vah - preco_atual) / range_va
                if dist_vah <= 0.25:
                    score += 10
                    detalhes["vol_profile"] = "VAH_resistencia_forte"
                elif dist_vah <= 0.5 and preco_atual > poc:
                    score += 7
                    detalhes["vol_profile"] = "acima_POC_distribuicao"
                elif preco_atual > val:
                    score += 3
                    detalhes["vol_profile"] = "dentro_VA_neutro"
                else:
                    detalhes["vol_profile"] = "abaixo_VAL_contra_short"
        else:
            detalhes["vol_profile"] = "sem_dados"

    except Exception as e:
        log.warning(f"  Erro score 3x {symbol}: {e}")
        detalhes["erro"] = str(e)

    detalhes["score_total"] = score
    return score, detalhes


def ma_cruza_favor(client: Client, symbol: str, direcao: str) -> bool:
    """
    Compatibilidade: retorna True se score >= 50 (3x automático liberado).
    """
    score, _ = calcular_score_3x(client, symbol, direcao)
    return score >= 50


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
    Estratégia 3x (padrão CNS): escala o 3x pela profundidade da perda.
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

    # DCA DINAMICO: calcula margem necessaria para breakeven em ~0.5% de movimento
    # Quanto mais margem, menos o preco precisa se mover pra recuperar.
    # Formula: adicional = (perda / (alavancagem * alvo_recuperacao)) - (0.94 * margem_atual)
    # Teto: 50% da banca. Piso: triplicar margem atual (3x classico).
    perda_atual = abs(roi / 100) * margem_atual  # perda em USDT
    alvo_recuperacao = 0.005  # 0.5% de movimento do preco para breakeven
    fator_desconto = 1.0 - (abs(roi) / 100 / leverage)  # ~0.94 para -120% ROI 20x

    adicional_ideal = (perda_atual / (leverage * alvo_recuperacao)) - (fator_desconto * margem_atual)
    adicional_ideal = max(adicional_ideal, margem_atual * 2.0)  # piso: 3x classico

    # Teto escalonado: quanto mais profundo o ROI, mais % da banca libera
    # -120%: 40% | -240%: 54% | -500%: 80% | acima: 80% (teto)
    cap_dca_pct = min(0.80, 0.30 + abs(roi) * 0.001)
    adicional = round(min(adicional_ideal, banca * cap_dca_pct), 2)

    # Calcula recuperacao real com o adicional escolhido
    margem_total = margem_atual + adicional
    recuperacao_pct = perda_atual / (leverage * (fator_desconto * margem_atual + adicional)) * 100
    modo_3x = f"DCA DINAMICO ({adicional/banca*100:.0f}% banca | breakeven ~{recuperacao_pct:.2f}%)"

    log.info(f"  {symbol}: {modo_3x} | ROI {roi:+.1f}% | Adicional: ${adicional:.2f} | Recuperacao: {recuperacao_pct:.2f}%")

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
            f"<b>3x EM ACAO! {symbol}</b>\n"
            f"{direcao} | Modo: {modo_3x}\n"
            f"Reforco: +${adicional:.2f} USDT\n\n"
            f"A virada comeca agora. Bora ver de quanto vai ser o lucro!"
        )
        log.info(f"3x EM ACAO! {symbol} {direcao} | {modo_3x} | +${adicional:.2f}")
        telegram(msg)
        dca_aplicado.add(symbol)
        dca_log[symbol] = time.time()
        roi_no_dca[symbol] = calcular_roi(posicao)  # registra ROI para stop pós-DCA -30%
        salvar_estado()
    except BinanceAPIException as e:
        log.error(f"Erro DCA {symbol}: {e}")
        # Não marca como dca_ativo — a ordem não foi executada
        dca_aplicado.discard(symbol)
        pico_pos_3x.pop(symbol, None)


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
    # Queda de ROI registrada só como log (sem alertas alarmantes)
    roi_prev = roi_anterior.get(symbol)
    if roi_prev is not None:
        queda = roi_prev - roi
        if queda >= 30 and roi > 0:
            # Só registra trailing ativo em posições positivas (informativo)
            log.info(f"Trailing ativo: {symbol} | ROI {roi_prev:+.1f}% -> {roi:+.1f}%")
    roi_anterior[symbol] = roi

    # Proximidade de liquidação registrada no log (sem alarmes no Telegram)
    # Rácio de Margem já protege a conta tecnicamente
    if liq_price > 0 and mark > 0:
        dist_liq = abs(mark - liq_price) / mark * 100
        if dist_liq <= 15:
            ultimo_alerta = alerta_liq_log.get(symbol, 0)
            if time.time() - ultimo_alerta >= 1800:  # log a cada 30 min
                log.warning(f"{symbol} | distancia liquidacao: {dist_liq:.1f}%")
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



def tendencia_h4(client: Client, symbol: str) -> str:
    """
    Retorna tendência no H4 baseada em EMA21 (padrão Bruno inferido).
    'alta'  = preço acima da EMA21 e EMA21 subindo
    'baixa' = preço abaixo da EMA21 e EMA21 caindo
    'lateral' = outros casos
    """
    try:
        df = get_candles(client, symbol, Client.KLINE_INTERVAL_4HOUR, limit=30)
        df["ema21"] = df["close"].ewm(span=21, adjust=False).mean()
        preco = df["close"].iloc[-1]
        ema_atual = df["ema21"].iloc[-1]
        ema_antes = df["ema21"].iloc[-4]
        ema_subindo = ema_atual > ema_antes
        ema_caindo = ema_atual < ema_antes
        if preco > ema_atual and ema_subindo:
            return "alta"
        if preco < ema_atual and ema_caindo:
            return "baixa"
        return "lateral"
    except Exception:
        return "lateral"


def sinal_guardiao(client: Client, symbol: str, btc_tendencia: str = "lateral") -> str | None:
    """
    Guardião CNS v2: Squeeze H2 + Filtro H4 (multi-timeframe do Bruno).

    Critérios:
    1. Tendência H4 confirmada (novo v2) — filtro macro do Bruno
    2. Bandas em squeeze H2 (bandwidth no mínimo de 20 períodos)
    3. Squeeze resolvendo (bandas expandindo >5%)
    4. Volume release >= 1.2x média
    5. Candle H2 FECHOU fora da banda
    6. Dois candles H2 consecutivos confirmando direção
    7. ATR H2 >= 2% do preço (filtro de tokens mortos — novo v2)
    8. Alinhamento com BTC
    """
    # --- 1. Filtro H4 de tendência (novo v2) ---
    h4_trend = tendencia_h4(client, symbol)
    if h4_trend == "lateral":
        return None  # H4 indeciso, não opera

    # --- Bollinger Bands no 2H ---
    df = get_candles(client, symbol, Client.KLINE_INTERVAL_2HOUR, limit=30)
    df["ma20"]      = df["close"].rolling(20).mean()
    df["std"]       = df["close"].rolling(20).std()
    df["bb_upper"]  = df["ma20"] + 2 * df["std"]
    df["bb_lower"]  = df["ma20"] - 2 * df["std"]
    df["bandwidth"] = (df["bb_upper"] - df["bb_lower"]) / df["ma20"]
    df["vol_media"] = df["volume"].rolling(20).mean()
    # ATR H2 (filtro de tokens mortos — novo v2)
    df["tr"] = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift()).abs(),
        (df["low"] - df["close"].shift()).abs()
    ], axis=1).max(axis=1)
    df["atr14"] = df["tr"].rolling(14).mean()

    c_ref  = df.iloc[-2]
    c_prev = df.iloc[-3]

    preco  = c_ref["close"]
    ma20   = c_ref["ma20"]
    if ma20 <= 0:
        return None

    # --- 2. ATR mínimo 2% do preço (filtra tokens sem movimento) ---
    atr_pct = c_ref["atr14"] / preco if preco > 0 else 0
    if atr_pct < 0.02:
        return None

    # --- 3. SQUEEZE: bandwidth em mínimo recente ---
    bw_atual  = c_ref["bandwidth"]
    bw_prev   = c_prev["bandwidth"]
    bw_min_20 = df["bandwidth"].iloc[-22:-2].min()
    estava_em_squeeze = bw_prev <= bw_min_20 * 1.15
    squeeze_resolvendo = bw_atual > bw_prev * 1.05
    if not (estava_em_squeeze and squeeze_resolvendo):
        return None

    # --- 4. VOLUME: release com força ---
    volume_ok = c_ref["volume"] >= c_ref["vol_media"] * 1.2
    if not volume_ok:
        return None

    # --- 5. ROMPIMENTO CONFIRMADO ---
    fechou_acima = c_ref["close"] > c_ref["bb_upper"]
    fechou_abaixo = c_ref["close"] < c_ref["bb_lower"]

    prev_alta = c_prev["close"] > c_prev["open"]
    prev_baixa = c_prev["close"] < c_prev["open"]
    cur_alta = c_ref["close"] > c_ref["open"]
    cur_baixa = c_ref["close"] < c_ref["open"]

    # --- 6. DECISÃO: H4 + BTC + Rompimento todos alinhados ---
    if fechou_acima and cur_alta and prev_alta:
        if h4_trend != "alta":
            return None  # H4 deve confirmar
        if btc_tendencia == "baixa":
            return None
        return "LONG"

    if fechou_abaixo and cur_baixa and prev_baixa:
        if h4_trend != "baixa":
            return None
        if btc_tendencia == "alta":
            return None
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
            pico_pos_3x.pop(symbol, None)
            log.warning(f"  [RACIO {racio:.1f}%] Fechando {symbol} {lado} | PnL ${pnl:+.2f} | ROI {roi:+.1f}%")
            registrar_aprendizado(client, symbol, lado, "racio_fechamento", roi,
                f"Racio {racio:.1f}% forcou fechamento | PnL ${pnl:+.2f}")
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
            log.info(f"Racio {racio:.1f}% | todas negativas em DCA | monitorando")
            RACIO_ALERTA_TS = time.time()

    return bloquear_dca

# ---------------------------------------------------------------------------
# Execução de ordens
# ---------------------------------------------------------------------------
def abrir_posicao(client: Client, symbol: str, direcao: str, preco: float, banca: float, qualidade: str = "NORMAL", risco_base: float = None) -> None:
    saldo_total    = get_saldo_total(client)
    alav_ideal     = alavancagem_dinamica(saldo_total)
    _risco_base    = risco_base if risco_base is not None else RISCO_POR_TRADE
    # Aplica peso por tier (Guardião v2): Tier A 1.5x, Tier B 1.0x, Tier C 0.7x
    peso = peso_tier(symbol)
    risco = _risco_base * peso
    margem = round(banca * risco, 2)
    # Garante notional mínimo de $5.50 (Binance exige $5)
    if margem * alav_ideal < 5.50:
        margem = round(5.50 / alav_ideal, 2)
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
        # Marca posição CNS para trailing mais paciente
        if qualidade == "CNS":
            posicoes_cns.add(symbol)

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
    log.info(f"Nunes iniciado | Modo: {MODO.upper()}")
    log.info(f"Guardiao CNS v2.1: Bollinger Squeeze H4+H2 | DCA 3x (-120%/-240%) | Trailing escalonado")
    log.info(f"Risco: {RISCO_POR_TRADE*100}% por trade | Max {MAX_POSICOES} posicoes | Alavancagem: {ALAVANCAGEM}x")
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
    ultimo_checkpoint_basedusdt = 0  # monitoramento de caso de estudo
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
    _usd_brl_ini                 = get_usd_brl(client)
    _meta_pct_ini                = meta_dinamica(ciclos_positivos_consecutivos)
    _meta_ini_usdt               = saldo_ciclo_inicio * (_meta_pct_ini / 100)
    _meta_ini_brl                = _meta_ini_usdt * _usd_brl_ini if _usd_brl_ini > 0 else _meta_ini_usdt
    log.info(f"Ciclo {ciclo_num} retomado | Saldo: ${saldo_ciclo_inicio:.2f} | Meta: {_meta_pct_ini:.1f}% (${_meta_ini_usdt:.2f} / R${_meta_ini_brl:.2f}) | Ciclos positivos: {ciclos_positivos_consecutivos}")
    telegram(f"<b>Ciclo {ciclo_num} retomado</b>\nSaldo: ${saldo_ciclo_inicio:.2f} USDT\nMeta: {_meta_pct_ini:.1f}% = ${_meta_ini_usdt:.2f} / R${_meta_ini_brl:.2f}\nCiclos positivos consecutivos: {ciclos_positivos_consecutivos}")

    while bot_ativo:
        try:
            global dca_ativo, posicoes_herdadas
            processar_comandos(client)
            banca = get_banca(client)

            # --- VERIFICACAO DE META DE CICLO (a cada 15s) ---
            if time.time() - ultimo_check_ciclo >= 15:
                ultimo_check_ciclo = time.time()
                try:
                    abertas_ciclo   = posicoes_abertas(client)
                    usd_brl_c       = get_usd_brl(client)
                    meta_pct_atual  = meta_dinamica(ciclos_positivos_consecutivos)
                    meta_ciclo_usdt = saldo_ciclo_inicio * (meta_pct_atual / 100)
                    fase_txt        = f"{meta_pct_atual:.1f}% | meta ${meta_ciclo_usdt:.2f}"
                    meta_brl        = meta_ciclo_usdt * usd_brl_c if usd_brl_c > 0 else meta_ciclo_usdt
                    if abertas_ciclo:
                        # PnL do ciclo = todas as posições abertas (incluindo herdadas, unificado)
                        pos_ciclo_atual = list(abertas_ciclo)
                        pnl_ciclo = sum(float(p.get("unrealizedProfit", p.get("unRealizedProfit", 0))) for p in pos_ciclo_atual)
                        pnl_brl   = pnl_ciclo * usd_brl_c if usd_brl_c > 0 else pnl_ciclo

                        # META CNS: só dispara quando as posições vencedoras sozinhas (ROI >= +10%)
                        # já pagam a meta — fecha SÓ positivas, nada é cortado no prejuízo
                        # Posicoes em 3x NAO contam (tem logica propria de saida)
                        ROI_META = 10.0
                        pnl_vencedoras = sum(
                            float(p.get("unrealizedProfit", p.get("unRealizedProfit", 0)))
                            for p in pos_ciclo_atual
                            if calcular_roi(p) >= ROI_META
                            and p["symbol"] not in dca_aplicado
                        )
                        pnl_venc_brl = pnl_vencedoras * usd_brl_c if usd_brl_c > 0 else pnl_vencedoras

                        meta_atingida = pnl_vencedoras >= meta_ciclo_usdt

                        log.info(f"Ciclo {ciclo_num} [{fase_txt}] | PnL: {pnl_brl:+.2f} BRL | Vencedoras >= +10%: R${pnl_venc_brl:+.2f} | Meta: R${meta_brl:.2f}")

                        if meta_atingida:
                            # Meta CNS: so fecha as vencedoras (ROI >= +10%)
                            # Nada e cortado no prejuizo — negativas continuam rodando
                            # IMPORTANTE: posicoes em 3x tem logica propria de saida (90/10)
                            # — nao sao fechadas pela meta do ciclo
                            # Protecao anti-BULLAUSDT: se a margem mudou muito recente
                            # (DCA manual ainda nao detectado), nao fecha
                            def _possivel_3x_manual(pos):
                                sym = pos["symbol"]
                                if sym in dca_aplicado:
                                    return True
                                m_atual = float(pos.get("positionInitialMargin", 0))
                                m_reg = margem_registrada.get(sym, 0)
                                if m_reg > 0 and m_atual > m_reg * 1.5:
                                    log.info(f"  {sym}: margem mudou ${m_reg:.2f}->${m_atual:.2f} — possivel 3x manual, meta nao fecha")
                                    return True
                                return False

                            pos_fechar = [p for p in pos_ciclo_atual
                                          if calcular_roi(p) >= ROI_META
                                          and not _possivel_3x_manual(p)]
                            pos_herdar = [p for p in pos_ciclo_atual
                                          if calcular_roi(p) < ROI_META
                                          or _possivel_3x_manual(p)]

                            telegram(
                                f"<b>Meta do Ciclo {ciclo_num} confirmada!</b>\n"
                                f"Lucro realizado: R${pnl_venc_brl:.2f}\n"
                                f"Fechando {len(pos_fechar)} vencedoras (>= +{ROI_META:.0f}% ROI)\n"
                                f"Mantendo {len(pos_herdar)} em andamento"
                            )
                            log.info(f"Meta atingida! Fechando {len(pos_fechar)} vencedoras | Mantendo {len(pos_herdar)}")

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
                            # Atualiza contador de ciclos positivos consecutivos
                            if lucro_ciclo > 0:
                                ciclos_positivos_consecutivos += 1
                            else:
                                ciclos_positivos_consecutivos = 0
                            nova_meta_pct  = meta_dinamica(ciclos_positivos_consecutivos)
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
                            # Limpa apenas posições do ciclo (não as herdadas)
                            for sym in list(dca_aplicado):
                                if sym not in posicoes_herdadas:
                                    dca_aplicado.discard(sym)
                                    pico_pos_3x.pop(sym, None)
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
                pico_anterior = peak_roi.get(symbol, roi)
                if roi > pico_anterior:
                    peak_roi[symbol] = roi
                    salvar_estado()
                    # Mensagens motivacionais em marcos (desde 3% para feedback mais frequente)
                    marcos = [3, 5, 10, 15, 20, 30, 50, 75, 100, 150, 200, 300, 500]
                    for marco in marcos:
                        if pico_anterior < marco <= roi:
                            telegram(
                                f"<b>Marco atingido: {symbol} +{marco}%!</b>\n"
                                f"{direcao} | ROI: {roi:+.1f}%\n"
                                f"Lucro crescendo! Trailing segue protegendo."
                            )
                            break

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

                # --- SAIDA POS-3x: 2 camadas de protecao ---
                # Camada 2 (prioridade): reversao tecnica -> fecha mesmo em ROI negativo
                # Camada 1: trailing escalonado por faixa de pico + piso no zero
                # Para AGRESSIVO/AUDACIOSO (3x em ROI muito profundo): saida rapida em +2%/+1%
                if symbol in dca_aplicado:
                    roi_entrada_dca = roi_no_dca.get(symbol, roi)
                    n_3x = dca_contagem.get(symbol, 1)

                    # Atualiza pico pos-3x
                    pico_3x = pico_pos_3x.get(symbol, roi)
                    if roi > pico_3x:
                        pico_3x = roi
                        pico_pos_3x[symbol] = pico_3x

                    fechou_pos_3x = False

                    # --- CAMADA 2: REVERSAO TECNICA (prioridade sobre Camada 1) ---
                    # Se o padrao virou contra, fecha imediato mesmo em ROI negativo
                    try:
                        rev_bloqueado, rev_motivo = detectar_padrao_reversao(client, symbol, direcao)
                    except Exception:
                        rev_bloqueado, rev_motivo = False, ""

                    if rev_bloqueado:
                        log.warning(f"  {symbol}: [POS-3x #{n_3x}] REVERSAO TECNICA -> fechando 90% | ROI {roi:+.1f}% | pico {pico_3x:+.1f}% | {rev_motivo}")
                        telegram(
                            f"<b>Saida tecnica pos-3x: {symbol}</b>\n"
                            f"{direcao} | ROI: {roi:+.1f}% | Pico: {pico_3x:+.1f}%\n"
                            f"Padrao virou contra: {rev_motivo}\n"
                            f"Fechando 90% para proteger a banca."
                        )
                        registrar_aprendizado(client, symbol, direcao, "3x_saida_reversao", roi,
                            f"3x #{n_3x} | Pico {pico_3x:+.1f}% | Reversao: {rev_motivo}")
                        fechar_parcial(client, p, 0.90, f"Reversao tecnica pos-3x #{n_3x} (ROI {roi:.1f}%)")
                        fechou_pos_3x = True

                    # --- STOP LOSS DINAMICO POS-3x: limita perda a max 2% da banca ---
                    # Quanto maior o DCA, mais apertado o stop (proporcional).
                    # Ex: margem $80, banca $150 -> stop em -3.75% ROI (perda max $3)
                    elif roi < 0:
                        margem_pos = float(p.get("positionInitialMargin", 0))
                        max_perda = banca * 0.02  # maximo 2% da banca
                        stop_dinamico = -(max_perda / margem_pos * 100) if margem_pos > 0 else -3.0
                        stop_dinamico = max(stop_dinamico, -10.0)  # nunca mais permissivo que -10%
                        stop_dinamico = min(stop_dinamico, -2.0)   # nunca mais apertado que -2%

                        if roi <= stop_dinamico:
                            perda_est = abs(roi / 100) * margem_pos
                            log.warning(f"  {symbol}: [POS-3x #{n_3x}] STOP {stop_dinamico:.1f}% atingido | ROI {roi:+.1f}% | ~${perda_est:.2f} -> fechando 90%")
                            telegram(
                                f"<b>Stop pos-3x: {symbol}</b>\n"
                                f"{direcao} | ROI: {roi:+.1f}% | 3x #{n_3x}\n"
                                f"Stop dinamico em {stop_dinamico:.1f}% (max 2% da banca).\n"
                                f"Perda: ~${perda_est:.2f}. Cortando rapido."
                            )
                            registrar_aprendizado(client, symbol, direcao, "3x_stop_loss", roi,
                                f"3x #{n_3x} | Stop {stop_dinamico:.1f}% | Perda ~${perda_est:.2f} | Entrada DCA: {roi_entrada_dca:+.0f}%")
                            fechar_parcial(client, p, 0.90, f"Stop pos-3x #{n_3x} {stop_dinamico:.1f}% (ROI {roi:.1f}%)")
                            fechou_pos_3x = True

                    # --- MODO AGRESSIVO/AUDACIOSO: saida rapida em qualquer lucro pequeno ---
                    # 3x em ROI muito profundo -> qualquer virada positiva ja compensa
                    elif roi_entrada_dca <= -1000 and roi >= 1.0:
                        log.info(f"  {symbol}: [POS-3x #{n_3x}] AUDACIOSO ROI {roi:+.1f}% >= +1% -> fechando 90%")
                        telegram(
                            f"<b>Lucro realizado (AUDACIOSO): {symbol}</b>\n"
                            f"{direcao} | ROI: {roi:+.1f}% | 3x #{n_3x} | Entrada: {roi_entrada_dca:+.0f}%\n"
                            f"Saida rapida apos recuperacao profunda."
                        )
                        registrar_aprendizado(client, symbol, direcao, "3x_sucesso_audacioso", roi,
                            f"3x #{n_3x} | Entrada DCA {roi_entrada_dca:+.0f}% | Saida +1%")
                        fechar_parcial(client, p, 0.90, f"Saida 90% pos-3x #{n_3x} AUDACIOSO (ROI {roi:.1f}%)")
                        fechou_pos_3x = True

                    elif roi_entrada_dca <= -500 and roi >= 2.0:
                        log.info(f"  {symbol}: [POS-3x #{n_3x}] AGRESSIVO ROI {roi:+.1f}% >= +2% -> fechando 90%")
                        telegram(
                            f"<b>Lucro realizado (AGRESSIVO): {symbol}</b>\n"
                            f"{direcao} | ROI: {roi:+.1f}% | 3x #{n_3x} | Entrada: {roi_entrada_dca:+.0f}%\n"
                            f"Saida rapida apos recuperacao forte."
                        )
                        registrar_aprendizado(client, symbol, direcao, "3x_sucesso_agressivo", roi,
                            f"3x #{n_3x} | Entrada DCA {roi_entrada_dca:+.0f}% | Saida +2%")
                        fechar_parcial(client, p, 0.90, f"Saida 90% pos-3x #{n_3x} AGRESSIVO (ROI {roi:.1f}%)")
                        fechou_pos_3x = True

                    # --- CAMADA 1: TRAILING ESCALONADO POR FAIXA DE PICO (modo NORMAL) ---
                    # So arma quando pico >= +1% (evita tremor lateral disparar)
                    # MODO VOLUME: se DCA foi grande (>=50% banca) e volume 1min esta forte,
                    # trailing afrouxa pra surfar o pump. Quando volume esfria, aperta.
                    elif pico_3x >= 1.0:
                        # Detecta se DCA foi grande e volume esta forte
                        volume_mode = False
                        vol_forte = False
                        margem_pos = float(p.get("positionInitialMargin", 0))
                        if margem_pos >= banca * 0.40 and roi > 0:
                            volume_mode = True
                            try:
                                df1_vol = get_candles(client, symbol, Client.KLINE_INTERVAL_1MINUTE, limit=10)
                                vol_atual = df1_vol["volume"].iloc[-1]
                                vol_media = df1_vol["volume"].iloc[-5:].mean()
                                vol_forte = vol_atual >= vol_media * 0.8  # volume sustentado
                            except Exception:
                                vol_forte = False

                        if volume_mode and vol_forte and pico_3x >= 3.0:
                            # MODO VOLUME: trailing mais frouxo — deixa o lucro correr
                            # Só aperta quando volume cair
                            if pico_3x < 10.0:
                                stop = pico_3x * 0.3  # 30% do pico (vs 50% normal)
                                faixa = f"VOLUME ALTO: 30% do pico ({stop:.1f}%)"
                            elif pico_3x < 20.0:
                                stop = pico_3x - 8.0   # -8pp (vs -6pp normal)
                                faixa = f"VOLUME ALTO: pico -8pp ({stop:.1f}%)"
                            elif pico_3x < 30.0:
                                stop = pico_3x - 12.0  # -12pp (vs -8pp normal)
                                faixa = f"VOLUME ALTO: pico -12pp ({stop:.1f}%)"
                            else:
                                stop = pico_3x - 15.0  # -15pp (vs -10pp normal)
                                faixa = f"VOLUME ALTO: pico -15pp ({stop:.1f}%)"
                        else:
                            # Trailing normal (ou volume caiu — aperta)
                            if pico_3x < 3.0:
                                stop = 0.0
                                faixa = "piso zero"
                            elif pico_3x < 10.0:
                                stop = pico_3x * 0.5
                                faixa = f"metade do pico ({stop:.1f}%)"
                            elif pico_3x < 20.0:
                                stop = pico_3x - 6.0
                                faixa = f"pico -6pp ({stop:.1f}%)"
                            elif pico_3x < 30.0:
                                stop = pico_3x - 8.0
                                faixa = f"pico -8pp ({stop:.1f}%)"
                            else:
                                stop = pico_3x - 10.0
                                faixa = f"pico -10pp ({stop:.1f}%)"

                            if volume_mode and not vol_forte and pico_3x >= 5.0:
                                faixa += " (volume esfriou — trailing apertado)"

                        if roi <= stop:
                            log.info(f"  {symbol}: [POS-3x #{n_3x}] TRAILING acionado! ROI {roi:+.1f}% <= stop {stop:+.1f}% | pico {pico_3x:+.1f}% | {faixa} -> fechando 90%")
                            telegram(
                                f"<b>Lucro travado: {symbol}</b>\n"
                                f"{direcao} | ROI: {roi:+.1f}% | Pico: {pico_3x:+.1f}%\n"
                                f"Trailing: {faixa}\n"
                                f"Fechando 90%, 10% segue na posicao."
                            )
                            registrar_aprendizado(client, symbol, direcao, "3x_trailing", roi,
                                f"3x #{n_3x} | Pico {pico_3x:+.1f}% | Stop {stop:+.1f}% ({faixa})")
                            fechar_parcial(client, p, 0.90, f"Trailing pos-3x #{n_3x} pico {pico_3x:.1f}% (ROI {roi:.1f}%)")
                            fechou_pos_3x = True

                    if fechou_pos_3x:
                        dca_aplicado.discard(symbol)
                        dca_contagem.pop(symbol, None)
                        roi_no_dca.pop(symbol, None)
                        pico_pos_3x.pop(symbol, None)
                        if dca_ativo == symbol:
                            dca_ativo = None
                    else:
                        # Acompanhamento a cada 5 min
                        alerta_key = f"3x_acomp_{symbol}"
                        if time.time() - alerta_dca_log.get(alerta_key, 0) >= 300:
                            grafico = analise_grafico_3x(client, symbol, direcao)
                            if roi > 0 and pico_3x >= 1.0:
                                status_txt = f"ROI: {roi:+.1f}% | Pico: {pico_3x:+.1f}% | Trailing armado"
                            elif roi > 0:
                                status_txt = f"ROI: {roi:+.1f}% | Pico: {pico_3x:+.1f}% | aguardando pico >=+1% p/ armar trailing"
                            else:
                                status_txt = f"ROI: {roi:+.1f}% | aguardando virar positivo"
                            telegram(
                                f"<b>Acompanhamento 3x: {symbol} (#{n_3x})</b>\n"
                                f"{direcao} | {status_txt}"
                                f"{grafico}"
                            )
                            alerta_dca_log[alerta_key] = time.time()

                # --- ALVO MEDIANO BRUNO: fecha 50% quando ROI >= +200% (10% movimento × 20x) ---
                # Bruno mostra que a mediana dos movimentos é 10% no ativo
                # Com 20x isso = 200% ROI. Fecha metade e deixa o resto correr com trailing
                elif roi >= 200 and symbol not in parcial_10pct and symbol not in parcial_500:
                    log.info(f"  {symbol}: ALVO 10% atingido (ROI {roi:+.1f}%) -> fechando 50%")
                    telegram(
                        f"<b>Alvo Bruno atingido: {symbol}</b>\n"
                        f"{direcao} | ROI: {roi:+.1f}% (+10% no preco)\n"
                        f"Fechando 50% — mediana historica. Resto corre no trailing."
                    )
                    fechar_parcial(client, p, 0.50, f"Alvo mediano Bruno +200% (ROI {roi:.1f}%)")
                    parcial_10pct.add(symbol)
                    continue

                # --- POSIÇÕES NORMAIS — TRAILING STOP ESCALONADO ---
                if roi > 0 and pico >= 5:
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
                    is_cns = symbol in posicoes_cns
                    if cripto_pump and not is_cns:
                        tolerancia = 0.05  # 5% do pico — pump pode despencar
                    elif is_cns:
                        # CNS: segura por dias, trailing largo
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
                        telegram(
                            f"<b>Lucro capturado: {symbol}{pump_tag}</b>\n"
                            f"{direcao} | Pico: {pico:.0f}% | Saida: {roi:+.1f}%\n"
                            f"Trailing protegeu o ganho! Fechando 90%."
                        )
                        registrar_aprendizado(client, symbol, direcao, "trailing_sucesso", roi,
                            f"Pico {pico:.0f}% | Saida {roi:+.1f}% | Tolerancia {tolerancia:.0%}{pump_tag}")
                        fechar_parcial(client, p, 0.90, f"Trailing stop{pump_tag} (pico {pico:.0f}% tol {tolerancia:.0%})")
                        peak_roi.pop(symbol, None)
                        ma_reverteu.pop(symbol, None)
                    else:
                        log.info(f"  {symbol}{pump_tag}: ROI {roi:+.1f}% | pico {pico:.0f}% | trailing ok (tol {tolerancia:.0%})")

                # --- MONITORAMENTO NEGATIVO (Rácio de Margem protege — 3x é a oportunidade) ---
                elif roi < 0:
                    # Analisa distância real da MA7 em relação à MA25 no 5min
                    # Detecta CRUZAMENTO ACONTECENDO e envia alerta especial
                    status_ma = "distante"
                    extra_msg = ""
                    cruzamento_agora = False
                    try:
                        df5_ma = get_candles(client, symbol, Client.KLINE_INTERVAL_5MINUTE, limit=30)
                        df5_ma["ma7"]  = df5_ma["close"].rolling(7).mean()
                        df5_ma["ma25"] = df5_ma["close"].rolling(25).mean()
                        ma7_cur  = df5_ma["ma7"].iloc[-1]
                        ma25_cur = df5_ma["ma25"].iloc[-1]
                        ma7_prev1 = df5_ma["ma7"].iloc[-2]
                        ma25_prev1 = df5_ma["ma25"].iloc[-2]
                        ma7_prev = df5_ma["ma7"].iloc[-3]
                        ma25_prev = df5_ma["ma25"].iloc[-3]

                        # Distância percentual entre MA7 e MA25
                        dist_pct = abs(ma7_cur - ma25_cur) / ma25_cur * 100 if ma25_cur > 0 else 0
                        dist_prev = abs(ma7_prev - ma25_prev) / ma25_prev * 100 if ma25_prev > 0 else 0
                        convergindo = dist_pct < dist_prev

                        # Detecta cruzamento recente (aconteceu entre candle -2 e -1)
                        if direcao == "LONG":
                            cruzou_agora = ma7_prev1 <= ma25_prev1 and ma7_cur > ma25_cur
                            ma_favor = ma7_cur > ma25_cur
                        else:
                            cruzou_agora = ma7_prev1 >= ma25_prev1 and ma7_cur < ma25_cur
                            ma_favor = ma7_cur < ma25_cur

                        # --- CANDLE 1: CRUZAMENTO ACONTECENDO AGORA ---
                        # Ainda NAO dispara 3x — precisa candle 2 confirmar separacao
                        if cruzou_agora:
                            cruzamento_agora = True
                            alerta_cruz_key = f"cruz_agora_{symbol}"
                            if time.time() - alerta_dca_log.get(alerta_cruz_key, 0) >= 900:
                                grafico = analise_grafico_3x(client, symbol, direcao)
                                telegram(
                                    f"<b>Candle 1: Cruzamento {symbol}</b>\n"
                                    f"{direcao} | ROI: {roi:+.1f}%\n"
                                    f"MA7 acabou de cruzar MA25 a favor.\n"
                                    f"Aguardando candle 2 confirmar separacao para disparar 3x.{grafico}"
                                )
                                alerta_dca_log[alerta_cruz_key] = time.time()

                        if ma_favor:
                            # Mede há quantos candles está favorável
                            candles_favor = 0
                            for i in range(1, min(10, len(df5_ma))):
                                ma7_i = df5_ma["ma7"].iloc[-i-1]
                                ma25_i = df5_ma["ma25"].iloc[-i-1]
                                if direcao == "LONG" and ma7_i > ma25_i:
                                    candles_favor += 1
                                elif direcao == "SHORT" and ma7_i < ma25_i:
                                    candles_favor += 1
                                else:
                                    break

                            # Verifica se está separando (candle 2 confirmando candle 1)
                            if direcao == "LONG":
                                separando = (ma7_cur - ma25_cur) > (ma7_prev1 - ma25_prev1)
                            else:
                                separando = (ma25_cur - ma7_cur) > (ma25_prev1 - ma7_prev1)

                            if candles_favor <= 1:
                                status_ma = "candle 1 cruzamento"
                                extra_msg = f"Candle 1: MA7 cruzou MA25. Aguardando candle 2 confirmar."
                            elif candles_favor == 2:
                                if separando:
                                    status_ma = "candle 2 confirmando"
                                    extra_msg = f"Candle 2: confirmando separacao ({dist_pct:.2f}%). 3x pode disparar no proximo ciclo!"
                                else:
                                    status_ma = "candle 2 lateralizando"
                                    extra_msg = f"Candle 2: MA7 lateralizou ({dist_pct:.2f}%). 3x aguarda separacao real."
                            elif candles_favor <= 4:
                                status_ma = "cruzamento confirmado"
                                extra_msg = f"MA7 confirmada ha {candles_favor} candles | Sep {dist_pct:.2f}%. Gatilho validado."
                            else:
                                status_ma = "tendencia estavel"
                                extra_msg = f"MA7 a favor ha {candles_favor}+ candles | Sep {dist_pct:.2f}%."
                        elif dist_pct < 0.3:
                            status_ma = "muito proximo"
                            extra_msg = f"MA7 a {dist_pct:.2f}% da MA25 — cruzamento iminente!"
                        elif dist_pct < 0.8 and convergindo:
                            status_ma = "aproximando"
                            extra_msg = f"MA7 a {dist_pct:.2f}% da MA25 e convergindo. Vai cruzar em breve."
                        elif convergindo:
                            status_ma = "caminhando"
                            extra_msg = f"MA7 a {dist_pct:.2f}% da MA25, mas ja comecando a convergir."
                        else:
                            status_ma = "distante"
                            extra_msg = f"MA7 a {dist_pct:.2f}% da MA25. Ainda vai levar tempo — paciencia."
                    except Exception:
                        extra_msg = "Aguardando sinal de reversao."

                    # Marcos motivacionais em drawdown — adaptados ao status da MA
                    marcos_negativos = {
                        -30:  "Acumulando potencial",
                        -60:  "Carregando a mola",
                        -90:  "Spread crescendo",
                        -120: "Zona de 3x",
                        -180: "Zona de 3x profunda",
                        -240: "Segundo gatilho 3x",
                    }
                    for nivel in sorted(marcos_negativos.keys()):
                        if roi <= nivel:
                            titulo = marcos_negativos[nivel]
                            alerta_key = f"marco_neg_{symbol}_{nivel}"
                            if time.time() - alerta_dca_log.get(alerta_key, 0) >= 1800:
                                telegram(
                                    f"<b>{titulo}: {symbol}</b>\n"
                                    f"{direcao} | ROI: {roi:+.1f}%\n"
                                    f"{extra_msg}"
                                )
                                alerta_dca_log[alerta_key] = time.time()
                            break  # só o mais profundo
                    log.info(f"  {symbol}: ROI {roi:+.1f}% | MA: {status_ma}")

                # --- ESTRATÉGIA 3x v2.2 (Guardião CNS) — Sistema de Score ---
                # Duas camadas de DCA: -120% (primeiro reforço) e -240% (segundo)
                # Score >= 85 dispara 3x automático
                elif roi <= -120.0:
                    n_3x = dca_contagem.get(symbol, 0)
                    # Cooldown de 10 min por symbol (reduzido: trailing pos-3x protege)
                    cooldown_key = f"3x_cooldown_{symbol}"
                    tempo_desde_ultimo = time.time() - alerta_dca_log.get(cooldown_key, 0)
                    if tempo_desde_ultimo < 600:
                        log.info(f"  {symbol}: ROI {roi:.1f}% | 3x em cooldown ({(600-tempo_desde_ultimo)/60:.0f} min restantes)")
                    else:
                        try:
                            # Filtro anti-convergencia: avisa quando MA99/MA25/MA7
                            # indicam reversao, mas NAO bloqueia — trailing pos-3x protege
                            bloqueado, motivo_bloqueio = detectar_padrao_reversao(client, symbol, direcao)
                            if bloqueado:
                                log.info(f"  {symbol}: ROI {roi:.1f}% | Padrao adverso (aviso): {motivo_bloqueio}")

                            score, detalhes = calcular_score_3x(client, symbol, direcao)
                            preco_atual_3x = float(p.get("markPrice", 0))
                            log.info(f"  {symbol}: ROI {roi:.1f}% | Score 3x: {score}/110")

                            if score >= 40 and not dca_bloqueado_por_racio:
                                log.info(f"  {symbol}: SCORE {score}/110 -> 3x #{n_3x + 1} DISPARADO")
                                aplicar_dca(client, p, banca)
                                dca_aplicado.add(symbol)
                                dca_contagem[symbol] = n_3x + 1
                                dca_ativo = symbol
                                alerta_dca_log[cooldown_key] = time.time()
                                grafico = analise_grafico_3x(client, symbol, direcao)
                                criterios_txt = "\n".join([f"  • {k}: {v}" for k, v in detalhes.items() if k != "score_total"])
                                aviso_adverso = f"\nAviso: padrao adverso ({motivo_bloqueio})" if bloqueado else ""
                                telegram(
                                    f"<b>Score: {score}/110</b>\n"
                                    f"<b>Criterios:</b>\n{criterios_txt}{aviso_adverso}\n"
                                    f"{grafico}"
                                )
                                registrar_aprendizado(client, symbol, direcao, "3x_auto", roi,
                                    f"Score {score}/110 | #{n_3x + 1}")
                            elif dca_bloqueado_por_racio:
                                log.warning(f"  {symbol}: 3x score {score} mas Racio acima de {RACIO_BLOQUEIA_DCA:.0f}%")
                            elif score >= 30:
                                log.info(f"  {symbol}: Score {score}/110 — fraco, aguardando")
                            else:
                                log.info(f"  {symbol}: Score {score}/110 — insuficiente")
                        except Exception as e:
                            log.warning(f"  Erro 3x score {symbol}: {e}")
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
                                    margem_add = max(saldo_tp * 0.01, 0.30)  # mínimo $0.30 margem = $6 notional
                                    # Garante notional mínimo de $5.50 (Binance exige $5)
                                    notional_add = margem_add * alav_tp
                                    if notional_add < 5.50:
                                        margem_add = 5.50 / alav_tp
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
                    # Posições negativas: aguardam 3x automático em -200% ou recuperação
                    # (o 3x é tratado no bloco `elif roi <= -200.0` acima)

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

            # --- EQUILIBRAR MARGEM: topup inteligente em posicoes abaixo de 3% ---
            # Fase 2: bot detecta posicoes com margem abaixo do alvo e faz topup
            # quando MA está a favor (momento certo). Roda a cada 5 min.
            agora = time.time()
            if agora - alerta_dca_log.get("equilibrar_scan", 0) >= 300:
                alerta_dca_log["equilibrar_scan"] = agora
                try:
                    saldo_eq = get_saldo_total(client)
                    alvo_margem = saldo_eq * RISCO_POR_TRADE
                    racio_eq = get_racio_margem(client)

                    if racio_eq < RACIO_MARGEM_MAX:  # topup usa limite mais conservador (6%)
                        for p in abertas:
                            sym_eq = p["symbol"]
                            amt_eq = float(p["positionAmt"])
                            if amt_eq == 0:
                                continue
                            margem_eq = float(p.get("positionInitialMargin", 0))
                            falta_eq = alvo_margem - margem_eq

                            if falta_eq < 0.50:  # ja esta equilibrada ou quase
                                continue
                            if sym_eq in dca_aplicado:  # 3x ativo, nao interferir
                                continue

                            dire_eq = "LONG" if amt_eq > 0 else "SHORT"

                            # Verifica MA — so faz topup se momento for favoravel
                            try:
                                df_eq = get_candles(client, sym_eq, Client.KLINE_INTERVAL_5MINUTE, limit=15)
                                df_eq["ma7"] = df_eq["close"].rolling(7).mean()
                                df_eq["ma25"] = df_eq["close"].rolling(25).mean()
                                c_eq = df_eq.iloc[-1]
                                if dire_eq == "LONG":
                                    ma_favor = c_eq["ma7"] > c_eq["ma25"]
                                else:
                                    ma_favor = c_eq["ma7"] < c_eq["ma25"]

                                if not ma_favor:
                                    continue  # MA contra — espera virar

                                # Momento bom — executa topup
                                preco_eq = float(client.futures_symbol_ticker(symbol=sym_eq)["price"])
                                precisao_eq = get_precisao_quantidade(client, sym_eq)
                                qty_eq = round((falta_eq * ALAVANCAGEM) / preco_eq, precisao_eq)
                                side_eq = "BUY" if dire_eq == "LONG" else "SELL"

                                if qty_eq > 0 and MODO == "real":
                                    # Checa racio antes de cada topup
                                    if get_racio_margem(client) >= RACIO_MARGEM_MAX:
                                        log.info(f"  Equilibrar: racio {RACIO_MARGEM_MAX:.0f}% atingido, parando topups")
                                        break
                                    client.futures_create_order(
                                        symbol=sym_eq, side=side_eq,
                                        type="MARKET", quantity=qty_eq, reduceOnly=False
                                    )
                                    topup_recente[sym_eq] = time.time()
                                    log.info(f"  {sym_eq}: EQUILIBRADO +${falta_eq:.2f} margem (alvo {RISCO_POR_TRADE*100:.0f}% banca) | MA a favor")
                                    telegram(
                                        f"<b>Margem equilibrada: {sym_eq}</b>\n"
                                        f"{dire_eq} | +${falta_eq:.2f} adicionado\n"
                                        f"Margem: ${margem_eq:.2f} -> ${margem_eq + falta_eq:.2f} (alvo {RISCO_POR_TRADE*100:.0f}%)\n"
                                        f"MA7 a favor — momento certo para reforcar."
                                    )
                            except Exception as e:
                                log.debug(f"  Erro equilibrar {sym_eq}: {e}")
                except Exception as e:
                    log.debug(f"Erro equilibrar scan: {e}")

            # --- BUSCA DE NOVAS ENTRADAS (a cada 30 segundos) ---
            if agora - ultimo_scan_entradas >= INTERVALO_ENTRADAS:
                ultimo_scan_entradas = agora

                saldo_atual_dia = get_saldo_total(client)
                max_pos_dinamico, risco_dinamico = limites_por_saldo(saldo_atual_dia)

                # Limpeza automática REMOVIDA — contradiz estratégia Guardião
                # Posições negativas ficam abertas aguardando 3x ou recuperação
                # Se exceder o limite: simplesmente não abre novas (não fecha as existentes)

                if len(abertas) >= max_pos_dinamico:
                    log.info(f"Maximo dinamico atingido ({len(abertas)}/{max_pos_dinamico} posicoes | saldo ${saldo_atual_dia:.0f}).")
                elif (racio_atual := get_racio_margem(client)) >= RACIO_MARGEM_MAX:
                    log.info(f"Racio de Margem {racio_atual:.2f}% >= limite {RACIO_MARGEM_MAX:.0f}%. Sem novas entradas.")
                else:
                    sessao = sessao_atual()
                    # Asiática escaneia mais pares (mercado mais movimentado)
                    if sessao == "ASIATICA":
                        top_pares_sessao = min(TOP_PARES + 100, 652)
                    else:
                        top_pares_sessao = TOP_PARES

                    log.info(f"Sessao {sessao} | Escaneando {top_pares_sessao} pares | Racio limite {RACIO_MARGEM_MAX:.0f}%")
                    btc_tendencia = tendencia_btc(client)
                    log.info(f"BTC: {btc_tendencia.upper()}")

                    pares = get_top_pares(client, top_pares_sessao)
                    simbolos_abertos = [p["symbol"] for p in abertas]
                    pares_filtrados = [s for s in pares if s not in simbolos_abertos]

                    sinais_encontrados = []
                    lock_sinais = threading.Lock()

                    def analisar_par(symbol):
                        try:
                            # Sistema Águia Spread: Bollinger Squeeze no 2H
                            sinal = sinal_guardiao(client, symbol, btc_tendencia)
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

                            # Classifica: ativos CNS prioritários = CNS, outros = GUARDIAO
                            qualidade = "CNS" if symbol in PARES_CNS else "GUARDIAO"
                            if master_pos:
                                qualidade = "COPY"

                            with lock_sinais:
                                sinais_encontrados.append((symbol, sinal, "squeeze", preco, qualidade))
                        except Exception as e:
                            log.warning(f"Erro ao analisar {symbol}: {e}")

                    with ThreadPoolExecutor(max_workers=THREADS_VARREDURA) as executor:
                        executor.map(analisar_par, pares_filtrados)

                    # --- Modo CNS: detecta volume anormal nos ativos prioritários ---
                    sinais_cns = detectar_sinais_cns(client, simbolos_abertos)
                    sinais_encontrados.extend(sinais_cns)

                    # Ordena: CNS (ativos prioritários) primeiro, depois COPY, depois GUARDIAO
                    ordem_qualidade = {"CNS": 0, "COPY": 1, "GUARDIAO": 2, "PREMIUM": 3, "NORMAL": 4}
                    sinais_encontrados.sort(key=lambda x: ordem_qualidade.get(x[4], 9))

                    MAX_ENTRADAS_POR_SCAN = 10
                    abertos_scan = 0
                    for symbol, sinal, direcao_tf, preco, qualidade in sinais_encontrados:
                        if abertos_scan >= MAX_ENTRADAS_POR_SCAN:
                            log.info(f"Limite de {MAX_ENTRADAS_POR_SCAN} entradas por varredura atingido.")
                            break
                        if len(posicoes_abertas(client)) >= max_pos_dinamico:
                            break
                        if get_racio_margem(client) >= RACIO_MARGEM_MAX:
                            log.info(f"Racio limite atingido — parando entradas.")
                            break
                        log.info(f"Sinal {sinal} [{qualidade}] em {symbol} | Preco: {preco}")
                        abrir_posicao(client, symbol, sinal, preco, banca, qualidade, risco_dinamico)
                        ultimo_entrada = time.time()
                        abertos_scan += 1

                    n_cns = len(sinais_cns)
                    n_normal = len(sinais_encontrados) - n_cns
                    log.info(f"Varredura concluida: {len(pares_filtrados)} pares | {n_normal} sinais Guardiao | {n_cns} sinais CNS")

            # Resumo horário
            if time.time() - ultimo_resumo_hora >= 3600:
                enviar_resumo_hora(client, saldo_abertura)
                ultimo_resumo_hora = time.time()

            # Auto-update: verifica GitHub a cada 5 minutos e reinicia se houver nova versão
            if time.time() - ultimo_check_update >= 300:
                verificar_atualizacao(reiniciar=True)
                ultimo_check_update = time.time()

            # --- CHECKPOINT AUTOMATICO BASEDUSDT (caso de estudo) ---
            # Grava snapshot a cada 30 min enquanto o caso nao tiver veredicto
            if time.time() - ultimo_checkpoint_basedusdt >= 1800:
                try:
                    ticker_bd = client.futures_symbol_ticker(symbol="BASEDUSDT")
                    preco_bd = float(ticker_bd["price"])
                    # Carrega aprendizados
                    try:
                        with open(APRENDIZADOS_FILE, "r", encoding="utf-8") as f:
                            apr_dados = json.load(f)
                    except (FileNotFoundError, json.JSONDecodeError):
                        apr_dados = []

                    # Pega o preco do primeiro checkpoint
                    cps = [d for d in apr_dados if d.get("symbol") == "BASEDUSDT" and "caso_estudo" in d.get("tipo", "")]
                    if cps:
                        preco_inicial = cps[0]["preco"]
                        mov_short = (preco_inicial - preco_bd) / preco_inicial * 100

                        # Padrao adverso ainda ativo?
                        bloqueado, motivo = detectar_padrao_reversao(client, "BASEDUSDT", "SHORT")

                        checkpoint_auto = {
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "symbol": "BASEDUSDT",
                            "direcao": "SHORT",
                            "tipo": "caso_estudo_auto",
                            "preco": preco_bd,
                            "roi_final": 0.0,
                            "detalhes": f"Checkpoint automatico a cada 30 min",
                            "movimento_desde_inicio": round(mov_short, 3),
                            "padrao_adverso_ativo": bloqueado,
                            "motivo_padrao": motivo if bloqueado else "nenhum",
                        }
                        apr_dados.append(checkpoint_auto)
                        with open(APRENDIZADOS_FILE, "w", encoding="utf-8") as f:
                            json.dump(apr_dados, f, indent=2, ensure_ascii=False)
                        log.info(f"  [CASO BASEDUSDT] checkpoint auto | preco {preco_bd} | mov SHORT {mov_short:+.2f}%")
                except Exception as e:
                    log.debug(f"Erro checkpoint BASEDUSDT: {e}")
                ultimo_checkpoint_basedusdt = time.time()

            # Acompanhamento em 3 niveis:
            # - 0.5s: alguma posicao ja esta em 3x (trailing pos-3x precisa pegar picos rapidos)
            # - 1.0s: alguma posicao esta perto do gatilho de 3x (ROI <= -100%)
            # - INTERVALO_POSICOES (15s): operacao normal
            if dca_aplicado:
                time.sleep(0.5)  # 3x ativo: monitoramento ultra rapido para trailing/reversao
            elif any(calcular_roi(p) <= -100 for p in abertas if float(p["positionAmt"]) != 0):
                time.sleep(1)    # perto do gatilho de 3x: monitora para disparar na hora certa
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
