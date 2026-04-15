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

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BLACKLIST_FILE = os.path.join(_BASE_DIR, "blacklist.json")
CONFIG_FILE = os.path.join(_BASE_DIR, "config_dinamico.json")
AUDITOR_LOG = os.path.join(_BASE_DIR, "auditor.log")
AUDITOR_STATE = os.path.join(_BASE_DIR, "auditor_estado.json")
APRENDIZADOS_FILE = os.path.join(_BASE_DIR, "aprendizados.json")
ACOES_FILE = os.path.join(_BASE_DIR, "acoes_recentes.json")

INTERVALO_MINUTOS = 5   # Ciclo rapido: detecta degradacao e reforcos em tempo util
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

def _income_call_com_retry(client: Client, max_tentativas: int = 3, **kwargs):
    """Chama futures_income_history com retry e backoff."""
    for tentativa in range(max_tentativas):
        try:
            return client.futures_income_history(**kwargs)
        except Exception as e:
            if tentativa == max_tentativas - 1:
                raise
            wait = 2 ** tentativa  # 1s, 2s, 4s
            log.warning(f"Erro income (tentativa {tentativa+1}/{max_tentativas}): {e} — retry em {wait}s")
            time.sleep(wait)
    return []


def puxar_incomes(client: Client, dias: int) -> list:
    """
    Puxa income history dos ultimos N dias.
    BUG CORRIGIDO: a paginacao por chunks puxava 4769 trades (toda a historia)
    em vez de 275 (14 dias). Resultado: PF 0.66 (errado) vs PF 1.57 (real).
    Fix: usa chamada direta com limit=1000 que retorna os mais recentes.
    """
    start = int((datetime.now() - timedelta(days=dias)).timestamp() * 1000)
    try:
        return _income_call_com_retry(client, startTime=start, limit=1000)
    except Exception as e:
        log.warning(f"Erro income: {e}")
        return []


def puxar_incomes_recentes(client: Client, horas: int = 6) -> list:
    """Puxa incomes das ultimas N horas (janela curta pra detectar mudancas rapidas)."""
    start = int((datetime.now() - timedelta(hours=horas)).timestamp() * 1000)
    try:
        return _income_call_com_retry(client, incomeType="REALIZED_PNL", startTime=start, limit=1000)
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

    # Piores horarios — threshold escala com saldo (1% do total de trades)
    # Com $40: bloqueia se perdeu > $5. Com $1000: bloqueia se perdeu > $50
    _threshold_horario = max(5, abs(total_pnl) * 0.10) if total_pnl != 0 else 5
    horarios_ruins = [h for h, pnl in pnl_por_hora.items() if pnl < -_threshold_horario]

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


def calcular_rsi_simples(closes, periodo=14):
    """RSI simples sem dependencia do nunes.py"""
    import pandas as pd
    delta = closes.diff()
    ganho = delta.clip(lower=0).rolling(periodo).mean()
    perda = (-delta.clip(upper=0)).rolling(periodo).mean()
    rs = ganho / perda
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else 50.0


def analise_tecnica_posicao(client: Client, symbol: str, direcao: str) -> dict:
    """
    Faz leitura tecnica da posicao: RSI, MA, volume, fibonacci, momentum.
    Retorna dict com sinais e recomendacao.
    """
    import pandas as pd
    resultado = {"sinais_reversao": 0, "detalhes": [], "recomendacao": "AGUARDAR"}
    try:
        # 5min
        klines5 = client.futures_klines(symbol=symbol, interval="5m", limit=30)
        df5 = pd.DataFrame(klines5, columns=['t','o','h','l','c','v','ct','qav','tr','tbav','tqav','i'])
        for col in ['o','h','l','c','v']:
            df5[col] = df5[col].astype(float)

        df5["ma7"] = df5["c"].rolling(7).mean()
        df5["ma25"] = df5["c"].rolling(25).mean()
        ma7_atual = df5["ma7"].iloc[-1]
        ma25_atual = df5["ma25"].iloc[-1]

        # 1h
        klines1h = client.futures_klines(symbol=symbol, interval="1h", limit=20)
        df1h = pd.DataFrame(klines1h, columns=['t','o','h','l','c','v','ct','qav','tr','tbav','tqav','i'])
        for col in ['o','h','l','c','v']:
            df1h[col] = df1h[col].astype(float)
        rsi_1h = calcular_rsi_simples(df1h["c"])

        # Volume
        vol_atual = df5["v"].iloc[-1]
        vol_media = df5["v"].iloc[-20:].mean()
        vol_ratio = vol_atual / vol_media if vol_media > 0 else 0

        # Range
        high_20 = df5["h"].iloc[-20:].max()
        low_20 = df5["l"].iloc[-20:].min()
        preco = df5["c"].iloc[-1]

        # Var 50min
        var_50min = (df5["c"].iloc[-1] - df5["c"].iloc[-10]) / df5["c"].iloc[-10] * 100

        # 5 sinais de reversao
        score = 0
        detalhes = []

        if direcao == "LONG":
            if rsi_1h >= 68:
                score += 1
                detalhes.append(f"RSI 1h {rsi_1h:.0f}")
            if ma7_atual < ma25_atual:
                score += 1
                detalhes.append("MA5m contra")
            if vol_ratio < 0.3:
                score += 1
                detalhes.append(f"vol {vol_ratio:.1f}x")
            dist_topo = (high_20 - preco) / preco * 100
            if dist_topo < 3.0:
                score += 1
                detalhes.append(f"topo {dist_topo:.1f}%")
            if var_50min < -0.5:
                score += 1
                detalhes.append(f"var50m {var_50min:+.1f}%")
        else:
            if rsi_1h <= 32:
                score += 1
                detalhes.append(f"RSI 1h {rsi_1h:.0f}")
            if ma7_atual > ma25_atual:
                score += 1
                detalhes.append("MA5m contra")
            if vol_ratio < 0.3:
                score += 1
                detalhes.append(f"vol {vol_ratio:.1f}x")
            dist_fundo = (preco - low_20) / preco * 100
            if dist_fundo < 3.0:
                score += 1
                detalhes.append(f"fundo {dist_fundo:.1f}%")
            if var_50min > 0.5:
                score += 1
                detalhes.append(f"var50m {var_50min:+.1f}%")

        resultado["sinais_reversao"] = score
        resultado["detalhes"] = detalhes
        resultado["rsi_1h"] = rsi_1h
        resultado["vol_ratio"] = vol_ratio
        resultado["var_50min"] = var_50min

        if score >= 4:
            resultado["recomendacao"] = "REALIZAR_JA"
        elif score >= 3:
            resultado["recomendacao"] = "REALIZAR"
        elif score >= 2:
            resultado["recomendacao"] = "ATENCAO"
        else:
            resultado["recomendacao"] = "DEIXAR_CORRER"
    except Exception as e:
        resultado["erro"] = str(e)
    return resultado


def carregar_acoes_recentes(minutos: int = 60) -> list:
    """Le acoes_recentes.json e filtra as ultimas N minutos."""
    try:
        with open(ACOES_FILE, "r", encoding="utf-8") as f:
            acoes = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutos)
    recentes = []
    for a in acoes:
        try:
            ts = datetime.fromisoformat(a.get("ts", "").replace("Z", "+00:00"))
            if ts >= cutoff:
                recentes.append(a)
        except Exception:
            continue
    return recentes


def formatar_acoes_para_telegram(acoes: list, max_acoes: int = 15) -> str:
    """Formata acoes recentes em texto pro telegram."""
    if not acoes:
        return ""
    # Agrupa por tipo
    por_tipo = {}
    for a in acoes:
        tipo = a.get("tipo", "?")
        por_tipo.setdefault(tipo, []).append(a)

    icones = {
        "compra": "+",
        "venda": "-",
        "cascata": "*",
        "reforco": "^",
        "topup": ">",
        "dca_3x": "!",
    }

    linhas = []
    # Mostra mais relevante primeiro
    ordem = ["dca_3x", "cascata", "reforco", "topup", "venda", "compra"]
    for tipo in ordem:
        if tipo not in por_tipo:
            continue
        items = por_tipo[tipo]
        ico = icones.get(tipo, "•")
        nome = tipo.upper()
        linhas.append(f"\n<b>{ico} {nome} ({len(items)})</b>")
        for a in items[-max_acoes:]:  # max N de cada
            ts = a.get("ts", "")[11:16]  # HH:MM
            sym = a.get("symbol", "?")
            motivo = a.get("motivo", "")[:80]
            linhas.append(f"  {ts} {sym}: {motivo}")
    return "\n".join(linhas)


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
        # Threshold escala: 1% do PnL total absoluto (min $10)
        _bl_threshold = max(10, abs(sum(pnl.values())) * 0.05)
        if perda < -_bl_threshold:
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

    # --- CASCATA por LUCRO ABSOLUTO: auditor nao mexe nos thresholds ---
    # Decisao do Cristiano: cascata em 3%/6%/10% do saldo (fixo)
    # Auditor so monitora se esta funcionando, nao ajusta valores

    # --- SCORE 3x: ajusta baseado no historico recente de DCAs ---
    # OVERRIDE MANUAL: se user setou score_minimo_3x_override, auditor nao mexe.
    # Permite user forcar valor especifico sem ser sobrescrito a cada ciclo.
    # Pra desativar o override, basta setar score_minimo_3x_override: null
    score_override = config.get("score_minimo_3x_override")
    SCORE_PISO = 75   # afrouxado: 80 travava demais (1021 bloqueios/dia)
    SCORE_TETO = 110  # teto tambem desce (nao exigir demais)
    score_atual = config.get("score_minimo_3x", 75)

    # Se PF curto (6h) < 0.5: 3x recentes estao falhando -> endurecer
    if pf_curto < 0.5 and metricas_curtas["total_trades"] >= 5 and score_atual < SCORE_TETO:
        config["score_minimo_3x"] = min(score_atual + 10, SCORE_TETO)
        mudancas.append(f"Score 3x subiu: {score_atual} -> {config['score_minimo_3x']} (PF 6h: {pf_curto:.2f})")

    # Se PF curto > 1.5: 3x estao funcionando bem -> pode relaxar
    elif pf_curto > 1.5 and metricas_curtas["total_trades"] >= 5 and score_atual > SCORE_PISO:
        config["score_minimo_3x"] = max(score_atual - 5, SCORE_PISO)
        mudancas.append(f"Score 3x desceu: {score_atual} -> {config['score_minimo_3x']} (PF 6h: {pf_curto:.2f})")

    # --- HORARIOS BLOQUEADOS ---
    horarios_ruins = metricas.get("horarios_ruins", [])
    if horarios_ruins and horarios_ruins != config.get("horarios_bloqueados", []):
        config["horarios_bloqueados"] = horarios_ruins
        mudancas.append(f"Horarios bloqueados: {horarios_ruins} (PnL negativo significativo)")

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
            if config.get("score_minimo_3x", 75) < 100:
                config["score_minimo_3x"] = 100
                mudancas.append(f"Score 3x forcado a 100 por degradacao")

    # --- DETECCAO DE EXCESSO DE 3x ---
    # Licao madrugada 12/04: 10 DCAs em 6h, 6 deram piso zero.
    try:
        acoes_6h = carregar_acoes_recentes(minutos=360)
        dcas_6h = [a for a in acoes_6h if a.get("tipo") == "dca_3x"]
        vendas_6h = [a for a in acoes_6h if a.get("tipo") == "venda" and "piso zero" in a.get("motivo", "").lower()]
        n_dcas = len(dcas_6h)
        n_pisos_zero = len(vendas_6h)
        if n_dcas >= 5 and n_pisos_zero >= 3:
            taxa_falha = n_pisos_zero / n_dcas
            if taxa_falha >= 0.5 and config.get("score_minimo_3x", 80) < SCORE_TETO:
                config["score_minimo_3x"] = min(config.get("score_minimo_3x", 80) + 10, SCORE_TETO)
                mudancas.append(f"EXCESSO DE 3x: {n_dcas} DCAs em 6h, {n_pisos_zero} piso_zero ({taxa_falha*100:.0f}% falha). Score subiu pra {config['score_minimo_3x']}")
    except Exception:
        pass

    # --- MONITORAMENTO DOS REFORCOS DE FORMIGAS ---
    # Auditor monitora se reforcos estao gerando lucro real ou virando prejuizo
    try:
        with open(APRENDIZADOS_FILE, "r", encoding="utf-8") as f:
            apr = json.load(f)
        reforcos = [d for d in apr if d.get("tipo") == "reforco_aplicado"]
        if len(reforcos) >= 5:
            symbols_reforcados = [r["symbol"] for r in reforcos[-10:]]
            pnl_sym = metricas.get("pnl_por_symbol", {})
            wins_ref = sum(1 for s in symbols_reforcados if pnl_sym.get(s, 0) > 0)
            losses_ref = sum(1 for s in symbols_reforcados if pnl_sym.get(s, 0) < 0)
            total_ref = wins_ref + losses_ref
            if total_ref >= 5:
                wr_ref = wins_ref / total_ref
                if wr_ref < 0.4 and config.get("reforco_habilitado", True):
                    config["reforco_habilitado"] = False
                    mudancas.append(f"REFORCO DESABILITADO: WR {wr_ref*100:.0f}% em {total_ref} reforcos (auditor desligou)")
                elif wr_ref >= 0.7 and not config.get("reforco_habilitado", True):
                    config["reforco_habilitado"] = True
                    mudancas.append(f"REFORCO REABILITADO: WR {wr_ref*100:.0f}% em {total_ref} reforcos")
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    # --- DIRECAO DA COLONIA: exercito marcha junto ---
    # Formiguinhas crescem juntas quando vao na mesma direcao.
    # Auditor define a direcao baseado em:
    # 1. BTC 24h trend
    # 2. % do mercado subindo vs caindo
    # 3. Performance recente por direcao
    try:
        btc_ticker = client.futures_ticker(symbol="BTCUSDT")
        btc_var = float(btc_ticker.get("priceChangePercent", 0))

        # Conta mercado geral
        all_tickers = client.futures_ticker()
        usdt_t = [t for t in all_tickers if t["symbol"].endswith("USDT") and float(t.get("quoteVolume", 0)) > 1000000]
        caindo = sum(1 for t in usdt_t if float(t.get("priceChangePercent", 0)) < 0)
        subindo = len(usdt_t) - caindo
        pct_caindo = caindo / len(usdt_t) * 100 if usdt_t else 50

        # Decide direcao da colonia
        direcao_anterior = config.get("direcao_colonia")

        if btc_var <= -2.0 and pct_caindo >= 60:
            direcao_colonia = "SHORT"
            motivo_dir = f"BTC {btc_var:+.1f}% + {pct_caindo:.0f}% do mercado caindo"
        elif btc_var >= 2.0 and pct_caindo <= 40:
            direcao_colonia = "LONG"
            motivo_dir = f"BTC {btc_var:+.1f}% + {100-pct_caindo:.0f}% do mercado subindo"
        elif pct_caindo >= 65:
            direcao_colonia = "SHORT"
            motivo_dir = f"{pct_caindo:.0f}% do mercado caindo (independente do BTC)"
        elif pct_caindo <= 35:
            direcao_colonia = "LONG"
            motivo_dir = f"{100-pct_caindo:.0f}% do mercado subindo"
        else:
            direcao_colonia = "AMBOS"
            motivo_dir = f"Mercado dividido ({pct_caindo:.0f}% caindo, BTC {btc_var:+.1f}%)"

        config["direcao_colonia"] = direcao_colonia
        config["direcao_colonia_motivo"] = motivo_dir

        if direcao_colonia != direcao_anterior:
            mudancas.append(f"Colonia: {direcao_anterior or '?'} -> {direcao_colonia} ({motivo_dir})")

    except Exception as e:
        log.debug(f"Erro direcao colonia: {e}")
    # (nao bloqueia, so informa — o sinal_guardiao ja filtra por BTC)

    # OVERRIDE final: se user setou score_minimo_3x_override, forca o valor.
    # Isso garante que nao importa o que os blocos acima fizeram, o valor final
    # respeita o override do user.
    if score_override is not None:
        if config.get("score_minimo_3x") != int(score_override):
            config["score_minimo_3x"] = int(score_override)
            mudancas.append(f"Score 3x fixado em {score_override} (override manual user)")

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

    # 5. Analise tecnica das top 5 vencedoras (leitura por posicao)
    abertas_pos = sorted([p for p in abertas if float(p.get("unrealizedProfit", 0)) > 0],
                          key=lambda p: -float(p.get("unrealizedProfit", 0)))[:5]
    analise_tecnica = []
    for p in abertas_pos:
        sym = p["symbol"]
        amt = float(p["positionAmt"])
        dire = "LONG" if amt > 0 else "SHORT"
        pnl_p = float(p.get("unrealizedProfit", 0))
        margem_p = float(p.get("positionInitialMargin", 0))
        roi_p = (pnl_p / margem_p * 100) if margem_p > 0 else 0
        try:
            tec = analise_tecnica_posicao(client, sym, dire)
            analise_tecnica.append({
                "symbol": sym,
                "direcao": dire,
                "pnl": pnl_p,
                "roi": roi_p,
                "score_reversao": tec.get("sinais_reversao", 0),
                "detalhes": tec.get("detalhes", []),
                "recomendacao": tec.get("recomendacao", "?"),
            })
        except Exception as e:
            log.debug(f"Erro analise tecnica {sym}: {e}")

    alertas_tecnicos = [a for a in analise_tecnica if a["score_reversao"] >= 3]

    # 6. Carrega acoes recentes pra mostrar no telegram
    acoes_recentes = carregar_acoes_recentes(minutos=INTERVALO_MINUTOS * 2)
    acoes_importantes = [a for a in acoes_recentes if a.get("tipo") in ("dca_3x", "cascata", "reforco")]

    # 7. Telegram — envia se houver mudanca, alerta, acao importante, ou alerta tecnico
    config = carregar_config()
    agora_dt = datetime.now()
    # Relatorio "regular" a cada 30 min (alem do horario e dos alertas)
    relatorio_30min = agora_dt.minute < INTERVALO_MINUTOS or (15 <= agora_dt.minute < 15 + INTERVALO_MINUTOS)
    tem_degradacao = any("DEGRADACAO" in m or "REFORCO DESABILITADO" in m for m in mudancas)
    deve_enviar = bool(mudancas) or bool(novos_bl) or relatorio_30min or tem_degradacao or bool(acoes_importantes) or bool(alertas_tecnicos)

    if deve_enviar:
        cascata_1_pct = config.get('cascata_1_pct_saldo', 3)
        cascata_2_pct = config.get('cascata_2_pct_saldo', 6)
        cascata_3_pct = config.get('cascata_3_pct_saldo', 10)
        msg = (
            f"<b>Auditor #{ciclo} — {agora_dt.strftime('%d/%m %H:%M')}</b>\n\n"
            f"Saldo: ${saldo:.2f} | Racio: {racio:.1f}%\n"
            f"Posicoes: {formiguinhas['total']} ({formiguinhas['n_positivas']}+/{formiguinhas['n_negativas']}-)\n"
            f"Cobertura: {formiguinhas['cobertura']:.1f}x\n\n"
            f"<b>14d:</b> PF {metricas['profit_factor']:.2f} | WR {metricas['win_rate']:.0f}%\n"
            f"<b>6h:</b> PF {metricas_curtas['profit_factor']:.2f} | WR {metricas_curtas['win_rate']:.0f}%\n\n"
            f"<b>Config ativo:</b>\n"
            f"  Modo: {config.get('modo_operacional', '?')}\n"
            f"  Cascata: {cascata_1_pct}%/{cascata_2_pct}%/{cascata_3_pct}% do saldo\n"
            f"  Score 3x: >= {config.get('score_minimo_3x', '?')}"
        )

        if mudancas:
            msg += "\n\n<b>Mudancas aplicadas:</b>"
            for m in mudancas:
                msg += f"\n  • {m}"

        if novos_bl:
            msg += f"\n\nBlacklist: +{len(novos_bl)} ({', '.join(list(novos_bl)[:5])})"

        # Leitura tecnica das top vencedoras
        if analise_tecnica:
            msg += f"\n\n<b>=== Leitura tecnica top {len(analise_tecnica)} vencedoras ===</b>"
            icones_rec = {
                "REALIZAR_JA": "[!!]",
                "REALIZAR": "[!]",
                "ATENCAO": "[?]",
                "DEIXAR_CORRER": "[ok]",
            }
            for a in analise_tecnica:
                ico = icones_rec.get(a["recomendacao"], "")
                det_str = ", ".join(a["detalhes"][:3]) if a["detalhes"] else "tudo ok"
                msg += (
                    f"\n  {ico} {a['symbol']:14s} {a['direcao']} ROI {a['roi']:+.0f}% PnL ${a['pnl']:+.2f}"
                    f"\n     {a['score_reversao']}/5 sinais: {det_str}"
                )

        # Acoes recentes do bot (ultimos N minutos)
        if acoes_recentes:
            acoes_txt = formatar_acoes_para_telegram(acoes_recentes, max_acoes=8)
            if acoes_txt:
                msg += f"\n\n<b>=== Acoes do bot (ultimos {INTERVALO_MINUTOS*2}min) ===</b>{acoes_txt}"

        telegram(msg)
    else:
        log.info("  (sem mudancas — telegram silencioso)")

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

    log.info(f"Ciclo #{ciclo} concluido. Proximo em {INTERVALO_MINUTOS}min.")
    return estado


def verificar_atualizacao() -> bool:
    """
    Verifica GitHub e auto-atualiza o auditor.
    Se houver nova versao, faz git pull e reinicia o processo.
    Retorna True se atualizou (mas nao retorna porque reinicia).
    """
    try:
        import subprocess, sys
        subprocess.run(["git", "fetch"], capture_output=True, text=True, timeout=15, cwd=_BASE_DIR)
        status = subprocess.run(
            ["git", "status", "-uno"],
            capture_output=True, text=True, timeout=10, cwd=_BASE_DIR
        )
        if "Your branch is behind" in status.stdout:
            log.warning("=" * 50)
            log.warning("Auditor: nova versao detectada! Atualizando...")
            log.warning("=" * 50)
            telegram("<b>Auditor: nova versao detectada!</b>\nBaixando e reiniciando...")
            pull = subprocess.run(["git", "pull"], capture_output=True, text=True, timeout=30, cwd=_BASE_DIR)
            log.info(f"git pull: {pull.stdout.strip()}")
            log.info("Reiniciando auditor...")
            os.execv(sys.executable, [sys.executable] + sys.argv)
    except Exception as e:
        log.debug(f"verificar_atualizacao falhou: {e}")
    return False


def main():
    verificar_atualizacao()  # check inicial
    log.info("=" * 50)
    log.info("AUDITOR CONTINUO INICIADO — MODO AUTONOMO")
    log.info(f"Intervalo: {INTERVALO_MINUTOS}min | Historico: {DIAS_HISTORICO}d")
    log.info("Analisa -> Decide -> Aplica -> Avisa")
    log.info("=" * 50)

    client = Client(API_KEY, API_SECRET, requests_params={"timeout": 30})
    estado = carregar_estado()

    telegram(
        f"<b>Auditor Continuo iniciado — AUTONOMO</b>\n"
        f"Analisa, decide, aplica e avisa.\n"
        f"Intervalo: {INTERVALO_MINUTOS}min\n"
        f"Ciclos anteriores: {estado.get('ciclos_executados', 0)}"
    )

    while True:
        try:
            verificar_atualizacao()  # check antes de cada ciclo
            estado = executar_ciclo(client, estado)
            salvar_estado(estado)
        except Exception as e:
            log.error(f"Erro no ciclo: {e}")
            telegram(f"<b>Erro no auditor:</b> {e}")

        time.sleep(INTERVALO_MINUTOS * 60)


if __name__ == "__main__":
    main()
