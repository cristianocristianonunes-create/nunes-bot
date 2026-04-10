#!/usr/bin/env python3
"""
Auditor Continuo — Guardiao CNS
Roda de hora em hora. Analisa, DECIDE, APLICA e avisa.

O auditor escreve em dois arquivos que o nunes.py le em tempo real:
  - config_dinamico.json: parametros (cascata, score, modo, risco)
  - blacklist.json: ativos bloqueados

Toda mudanca eh logada e enviada no Telegram com o motivo.

USO: python auditor_continuo.py
"""

import os
import json
import time
import logging
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from dotenv import load_dotenv
from binance.client import Client
import requests

load_dotenv()

API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

BLACKLIST_FILE = "C:/robo-trade/blacklist.json"
CONFIG_FILE = "C:/robo-trade/config_dinamico.json"
AUDITOR_LOG = "C:/robo-trade/auditor.log"
AUDITOR_STATE = "C:/robo-trade/auditor_estado.json"
APRENDIZADOS_FILE = "C:/robo-trade/aprendizados.json"

INTERVALO_HORAS = 1
DIAS_HISTORICO = 14

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [AUDITOR] %(message)s",
    handlers=[
        logging.FileHandler(AUDITOR_LOG, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


def telegram(msg: str) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception:
        pass


# ============================================================
# ESTADO / CONFIG
# ============================================================

def carregar_estado() -> dict:
    try:
        with open(AUDITOR_STATE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"ciclos_executados": 0, "pf_historico": [], "wr_historico": [],
                "cobertura_historico": [], "mudancas_aplicadas": []}


def salvar_estado(estado: dict) -> None:
    with open(AUDITOR_STATE, "w", encoding="utf-8") as f:
        json.dump(estado, f, indent=2)


def carregar_config() -> dict:
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def salvar_config(config: dict, motivo: str) -> None:
    config["ultima_atualizacao"] = datetime.now().isoformat()
    config["motivo_ultima_mudanca"] = motivo
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)


# ============================================================
# COLETA DE DADOS
# ============================================================

def puxar_incomes(client: Client, dias: int) -> list:
    todos = []
    start = int((datetime.now() - timedelta(days=dias)).timestamp() * 1000)
    end = int(datetime.now().timestamp() * 1000)
    cursor = start
    while cursor < end:
        chunk_end = min(cursor + 7 * 86400 * 1000, end)
        try:
            incomes = client.futures_income_history(startTime=cursor, endTime=chunk_end, limit=1000)
            todos.extend(incomes)
            cursor = chunk_end if len(incomes) < 1000 else int(incomes[-1]["time"]) + 1
        except Exception as e:
            log.warning(f"Erro income: {e}")
            cursor = chunk_end
    return todos


def puxar_incomes_recentes(client: Client, horas: int = 6) -> list:
    """Puxa incomes das ultimas N horas (janela curta pra detectar mudancas rapidas)."""
    start = int((datetime.now() - timedelta(hours=horas)).timestamp() * 1000)
    try:
        return client.futures_income_history(incomeType="REALIZED_PNL", startTime=start, limit=1000)
    except Exception:
        return []


def posicoes_abertas(client: Client) -> list:
    account = client.futures_account()
    return [p for p in account["positions"] if abs(float(p["positionAmt"])) > 0]


def get_saldo_total(client: Client) -> float:
    return float(client.futures_account().get("totalMarginBalance", 0))


def get_racio_margem(client: Client) -> float:
    account = client.futures_account()
    maint = float(account.get("totalMaintMargin", 0))
    balance = float(account.get("totalMarginBalance", 0))
    return (maint / balance * 100) if balance > 0 else 0


# ============================================================
# ANALISE
# ============================================================

def analisar_performance(incomes: list) -> dict:
    pnl_por_symbol = defaultdict(float)
    trades_por_symbol = defaultdict(int)
    wins_por_symbol = defaultdict(int)
    losses_por_symbol = defaultdict(int)
    total_pnl = 0
    total_taxas = 0
    wins = 0
    losses = 0
    pnl_trades = []
    pnl_por_hora = defaultdict(float)

    for i in incomes:
        tipo = i.get("incomeType", "")
        valor = float(i.get("income", 0))
        symbol = i.get("symbol", "")
        ts = int(i.get("time", 0))
        dt = datetime.fromtimestamp(ts / 1000)

        if tipo == "REALIZED_PNL" and symbol:
            pnl_por_symbol[symbol] += valor
            trades_por_symbol[symbol] += 1
            pnl_trades.append(valor)
            total_pnl += valor
            pnl_por_hora[dt.hour] += valor
            if valor > 0:
                wins += 1
                wins_por_symbol[symbol] += 1
            elif valor < 0:
                losses += 1
                losses_por_symbol[symbol] += 1
        elif tipo == "COMMISSION":
            total_taxas += valor

    total_trades = wins + losses
    win_rate = (wins / total_trades * 100) if total_trades > 0 else 50
    avg_win = sum(v for v in pnl_trades if v > 0) / wins if wins > 0 else 0
    avg_loss = sum(v for v in pnl_trades if v < 0) / losses if losses > 0 else 0
    sum_loss = sum(v for v in pnl_trades if v < 0)
    profit_factor = abs(sum(v for v in pnl_trades if v > 0) / sum_loss) if sum_loss != 0 else 999

    # Drawdown
    running = peak = max_dd = 0
    for v in pnl_trades:
        running += v
        peak = max(peak, running)
        max_dd = max(max_dd, peak - running)

    # Piores horarios
    horarios_ruins = [h for h, pnl in pnl_por_hora.items() if pnl < -5]

    return {
        "total_pnl": total_pnl, "total_taxas": total_taxas,
        "total_trades": total_trades, "win_rate": win_rate,
        "profit_factor": profit_factor, "avg_win": avg_win,
        "avg_loss": avg_loss, "max_dd": max_dd, "wins": wins, "losses": losses,
        "pnl_por_symbol": dict(pnl_por_symbol),
        "trades_por_symbol": dict(trades_por_symbol),
        "wins_por_symbol": dict(wins_por_symbol),
        "losses_por_symbol": dict(losses_por_symbol),
        "horarios_ruins": horarios_ruins,
    }


def analisar_formiguinhas(abertas: list) -> dict:
    pnl_pos = sum(float(p.get("unrealizedProfit", 0)) for p in abertas if float(p.get("unrealizedProfit", 0)) > 0)
    pnl_neg = sum(float(p.get("unrealizedProfit", 0)) for p in abertas if float(p.get("unrealizedProfit", 0)) < 0)
    cobertura = abs(pnl_pos / pnl_neg) if pnl_neg != 0 else 999
    n_pos = sum(1 for p in abertas if float(p.get("unrealizedProfit", 0)) > 0)
    n_neg = len(abertas) - n_pos

    # Quantas geram lucro real (> $0.05)?
    lucro_real = sum(1 for p in abertas if float(p.get("unrealizedProfit", 0)) > 0.05)

    return {
        "pnl_positivas": pnl_pos, "pnl_negativas": pnl_neg,
        "cobertura": cobertura, "n_positivas": n_pos, "n_negativas": n_neg,
        "lucro_real": lucro_real, "total": len(abertas),
    }


# ============================================================
# BLACKLIST
# ============================================================

def atualizar_blacklist(metricas: dict) -> set:
    pnl = metricas["pnl_por_symbol"]
    trades = metricas["trades_por_symbol"]
    wins = metricas["wins_por_symbol"]

    try:
        with open(BLACKLIST_FILE, "r", encoding="utf-8") as f:
            dados = json.load(f)
        blacklist_atual = set(dados.get("symbols", []))
        motivos = dados.get("motivos", {})
    except (FileNotFoundError, json.JSONDecodeError):
        blacklist_atual = set()
        motivos = {}

    novos = set()
    for symbol in pnl:
        n = trades.get(symbol, 0)
        w = wins.get(symbol, 0)
        perda = pnl[symbol]

        if w == 0 and n >= 3:
            novos.add(symbol)
            motivos[symbol] = f"0 wins em {n} trades (perda ${perda:.2f})"
        if perda < -10:
            novos.add(symbol)
            motivos[symbol] = f"Perda ${perda:.2f} em {n} trades (WR {w/n*100:.0f}%)" if n > 0 else f"Perda ${perda:.2f}"
        if n >= 10 and w / n < 0.20:
            novos.add(symbol)
            motivos[symbol] = f"WR {w/n*100:.0f}% em {n} trades (perda ${perda:.2f})"

    blacklist_final = blacklist_atual | novos
    adicionados = novos - blacklist_atual

    dados_salvar = {
        "symbols": sorted(list(blacklist_final)),
        "motivos": motivos,
        "ultima_atualizacao": datetime.now().isoformat(),
        "total": len(blacklist_final),
    }
    with open(BLACKLIST_FILE, "w", encoding="utf-8") as f:
        json.dump(dados_salvar, f, indent=2, ensure_ascii=False)

    return adicionados


# ============================================================
# DECISOES — O CEREBRO DO AUDITOR
# ============================================================

def decidir_e_aplicar(metricas: dict, metricas_curtas: dict, formiguinhas: dict,
                      racio: float, saldo: float, estado: dict) -> list:
    """
    Analisa dados, toma decisoes, escreve no config_dinamico.json.
    Retorna lista de mudancas aplicadas com motivo.
    """
    config = carregar_config()
    mudancas = []
    pf = metricas["profit_factor"]
    wr = metricas["win_rate"]
    pf_curto = metricas_curtas["profit_factor"]
    cobertura = formiguinhas["cobertura"]

    # --- MODO OPERACIONAL ---
    modo_anterior = config.get("modo_operacional", "NORMAL")

    if racio >= 30:
        modo = "PAUSA"
    elif pf < 0.8 or metricas["max_dd"] > saldo * 0.5:
        modo = "PROTECAO"
    elif pf < 1.3 or wr < 50 or racio >= 20:
        modo = "CAUTELA"
    else:
        modo = "NORMAL"

    if modo != modo_anterior:
        config["modo_operacional"] = modo
        mudancas.append(f"Modo: {modo_anterior} -> {modo} (PF {pf:.2f}, WR {wr:.0f}%, racio {racio:.0f}%)")

    # --- CASCATA: ajusta baseado na cobertura das formiguinhas ---
    cascata_1 = config.get("cascata_1_roi", 50)

    # Cascata minima: 500% (decisao do Cristiano — formiguinha so realiza apos 5x)
    # Sem teto: deixa correr quanto for preciso
    CASCATA_PISO = 500

    # Se cobertura < 0.5x: boas nao cobrem nem metade das mas -> subir cascata
    if cobertura < 0.5:
        config["cascata_1_roi"] = cascata_1 + 50
        config["cascata_2_roi"] = config["cascata_1_roi"] * 2
        config["cascata_3_roi"] = config["cascata_1_roi"] * 4
        mudancas.append(f"Cascata subida: {cascata_1}% -> {config['cascata_1_roi']}% (cobertura {cobertura:.1f}x fraca)")

    # Se cobertura >= 3x: exercito muito forte, pode realizar um pouco mais cedo
    elif cobertura >= 3.0 and cascata_1 > CASCATA_PISO:
        config["cascata_1_roi"] = max(cascata_1 - 25, CASCATA_PISO)
        config["cascata_2_roi"] = config["cascata_1_roi"] * 2
        config["cascata_3_roi"] = config["cascata_1_roi"] * 4
        mudancas.append(f"Cascata desceu: {cascata_1}% -> {config['cascata_1_roi']}% (cobertura {cobertura:.1f}x forte)")

    # --- SCORE 3x: ajusta baseado no historico recente de DCAs ---
    score_atual = config.get("score_minimo_3x", 50)

    # Se PF curto (6h) < 0.5: 3x recentes estao falhando -> endurecer
    if pf_curto < 0.5 and metricas_curtas["total_trades"] >= 5 and score_atual < 70:
        config["score_minimo_3x"] = min(score_atual + 10, 80)
        mudancas.append(f"Score 3x subiu: {score_atual} -> {config['score_minimo_3x']} (PF 6h: {pf_curto:.2f})")

    # Se PF curto > 1.5: 3x estao funcionando bem -> pode relaxar
    elif pf_curto > 1.5 and metricas_curtas["total_trades"] >= 5 and score_atual > 40:
        config["score_minimo_3x"] = max(score_atual - 5, 40)
        mudancas.append(f"Score 3x desceu: {score_atual} -> {config['score_minimo_3x']} (PF 6h: {pf_curto:.2f})")

    # --- HORARIOS BLOQUEADOS ---
    horarios_ruins = metricas.get("horarios_ruins", [])
    if horarios_ruins and horarios_ruins != config.get("horarios_bloqueados", []):
        config["horarios_bloqueados"] = horarios_ruins
        mudancas.append(f"Horarios bloqueados: {horarios_ruins} (PnL negativo > $5)")

    # --- DETECCAO DE DEGRADACAO ---
    pf_hist = estado.get("pf_historico", [])
    pf_hist.append(pf)
    if len(pf_hist) > 72:  # 3 dias
        pf_hist = pf_hist[-72:]
    estado["pf_historico"] = pf_hist

    if len(pf_hist) >= 6:
        pf_recente = sum(pf_hist[-3:]) / 3
        pf_anterior = sum(pf_hist[-6:-3]) / 3
        if pf_anterior > 0 and pf_recente < pf_anterior * 0.6:
            mudancas.append(f"ALERTA DEGRADACAO: PF caiu {pf_anterior:.2f} -> {pf_recente:.2f} (-{(1-pf_recente/pf_anterior)*100:.0f}%)")
            # Endurece tudo
            if config.get("score_minimo_3x", 50) < 70:
                config["score_minimo_3x"] = 70
                mudancas.append(f"Score 3x forcado a 70 por degradacao")

    # --- DIRECAO PREFERIDA baseada no BTC ---
    # Se 80%+ dos wins recentes sao LONG ou SHORT, sugere direcao
    # (nao bloqueia, so informa — o sinal_guardiao ja filtra por BTC)

    # Salva config
    if mudancas:
        motivo = " | ".join(mudancas[:3])  # primeiras 3 mudancas como motivo
        salvar_config(config, motivo)

    return mudancas


# ============================================================
# CICLO PRINCIPAL
# ============================================================

def executar_ciclo(client: Client, estado: dict) -> dict:
    ciclo = estado.get("ciclos_executados", 0) + 1
    log.info(f"{'=' * 50}")
    log.info(f"CICLO #{ciclo}")
    log.info(f"{'=' * 50}")

    # 1. Coleta
    log.info("Coletando dados...")
    incomes_14d = puxar_incomes(client, DIAS_HISTORICO)
    incomes_6h = puxar_incomes_recentes(client, 6)
    abertas = posicoes_abertas(client)
    saldo = get_saldo_total(client)
    racio = get_racio_margem(client)

    # 2. Analisa
    metricas = analisar_performance(incomes_14d)
    metricas_curtas = analisar_performance(incomes_6h)
    formiguinhas = analisar_formiguinhas(abertas)

    log.info(f"  14d: PnL ${metricas['total_pnl']:+.2f} | PF {metricas['profit_factor']:.2f} | WR {metricas['win_rate']:.0f}%")
    log.info(f"  6h:  PnL ${metricas_curtas['total_pnl']:+.2f} | PF {metricas_curtas['profit_factor']:.2f} | WR {metricas_curtas['win_rate']:.0f}%")
    log.info(f"  Saldo: ${saldo:.2f} | Racio: {racio:.1f}% | Posicoes: {formiguinhas['total']}")
    log.info(f"  Cobertura: {formiguinhas['cobertura']:.1f}x | Lucro real: {formiguinhas['lucro_real']}")

    # 3. Blacklist
    novos_bl = atualizar_blacklist(metricas)
    if novos_bl:
        log.info(f"  Blacklist: +{len(novos_bl)}")

    # 4. DECIDE E APLICA
    mudancas = decidir_e_aplicar(metricas, metricas_curtas, formiguinhas, racio, saldo, estado)

    if mudancas:
        log.info("  MUDANCAS APLICADAS:")
        for m in mudancas:
            log.info(f"    -> {m}")
    else:
        log.info("  Nenhuma mudanca necessaria.")

    # 5. Telegram
    config = carregar_config()
    msg = (
        f"<b>Auditor #{ciclo} — {datetime.now().strftime('%d/%m %H:%M')}</b>\n\n"
        f"Saldo: ${saldo:.2f} | Racio: {racio:.1f}%\n"
        f"Posicoes: {formiguinhas['total']} ({formiguinhas['n_positivas']}+/{formiguinhas['n_negativas']}-)\n"
        f"Cobertura: {formiguinhas['cobertura']:.1f}x\n\n"
        f"<b>14d:</b> PF {metricas['profit_factor']:.2f} | WR {metricas['win_rate']:.0f}%\n"
        f"<b>6h:</b> PF {metricas_curtas['profit_factor']:.2f} | WR {metricas_curtas['win_rate']:.0f}%\n\n"
        f"<b>Config ativo:</b>\n"
        f"  Modo: {config.get('modo_operacional', '?')}\n"
        f"  Cascata: {config.get('cascata_1_roi', '?')}% / {config.get('cascata_2_roi', '?')}% / {config.get('cascata_3_roi', '?')}%\n"
        f"  Score 3x: >= {config.get('score_minimo_3x', '?')}"
    )

    if mudancas:
        msg += "\n\n<b>Mudancas aplicadas:</b>"
        for m in mudancas:
            msg += f"\n  • {m}"

    if novos_bl:
        msg += f"\n\nBlacklist: +{len(novos_bl)} ({', '.join(list(novos_bl)[:5])})"

    telegram(msg)

    # 6. Atualiza estado
    estado["ciclos_executados"] = ciclo
    estado["ultima_analise"] = time.time()
    estado["ultimo_saldo"] = saldo
    estado["ultimo_racio"] = racio
    estado["ultimo_pf"] = metricas["profit_factor"]
    estado["ultimo_wr"] = metricas["win_rate"]
    estado["ultimo_cobertura"] = formiguinhas["cobertura"]
    estado["ultimo_modo"] = config.get("modo_operacional")

    # Registra mudancas
    hist = estado.get("mudancas_aplicadas", [])
    for m in mudancas:
        hist.append({"ts": datetime.now().isoformat(), "mudanca": m})
    if len(hist) > 200:
        hist = hist[-200:]
    estado["mudancas_aplicadas"] = hist

    log.info(f"Ciclo #{ciclo} concluido. Proximo em {INTERVALO_HORAS}h.")
    return estado


def main():
    log.info("=" * 50)
    log.info("AUDITOR CONTINUO INICIADO — MODO AUTONOMO")
    log.info(f"Intervalo: {INTERVALO_HORAS}h | Historico: {DIAS_HISTORICO}d")
    log.info("Analisa -> Decide -> Aplica -> Avisa")
    log.info("=" * 50)

    client = Client(API_KEY, API_SECRET)
    estado = carregar_estado()

    telegram(
        f"<b>Auditor Continuo iniciado — AUTONOMO</b>\n"
        f"Analisa, decide, aplica e avisa.\n"
        f"Intervalo: {INTERVALO_HORAS}h\n"
        f"Ciclos anteriores: {estado.get('ciclos_executados', 0)}"
    )

    while True:
        try:
            estado = executar_ciclo(client, estado)
            salvar_estado(estado)
        except Exception as e:
            log.error(f"Erro no ciclo: {e}")
            telegram(f"<b>Erro no auditor:</b> {e}")

        time.sleep(INTERVALO_HORAS * 3600)


if __name__ == "__main__":
    main()
