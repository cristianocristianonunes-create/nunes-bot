#!/usr/bin/env python3
"""
Auditoria Completa — Guardiao CNS
Puxa historico real da Binance e analisa tudo.
"""

import os
import json
import time
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from dotenv import load_dotenv
from binance.client import Client

load_dotenv()

API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")
client = Client(API_KEY, API_SECRET)

# ============================================================
# 1. PUXA HISTORICO COMPLETO DA BINANCE
# ============================================================

def puxar_income_history(dias=30):
    """Puxa todo PnL realizado dos ultimos N dias."""
    todos = []
    start = int((datetime.now() - timedelta(days=dias)).timestamp() * 1000)
    end = int(datetime.now().timestamp() * 1000)

    # Pagina por 7 dias (limite API)
    cursor = start
    while cursor < end:
        chunk_end = min(cursor + 7 * 86400 * 1000, end)
        try:
            incomes = client.futures_income_history(
                startTime=cursor, endTime=chunk_end, limit=1000
            )
            todos.extend(incomes)
            if len(incomes) < 1000:
                cursor = chunk_end
            else:
                cursor = int(incomes[-1]["time"]) + 1
        except Exception as e:
            print(f"Erro puxando income: {e}")
            cursor = chunk_end
    return todos

def puxar_trades_recentes(dias=30):
    """Puxa trades de todos os symbols que tiveram PnL."""
    # Primeiro descobre quais symbols tiveram atividade
    incomes = puxar_income_history(dias)
    symbols = set()
    for i in incomes:
        if i.get("symbol"):
            symbols.add(i["symbol"])

    print(f"Symbols com atividade: {len(symbols)}")

    all_trades = []
    start = int((datetime.now() - timedelta(days=dias)).timestamp() * 1000)

    for sym in sorted(symbols):
        try:
            trades = client.futures_account_trades(symbol=sym, startTime=start, limit=1000)
            all_trades.extend(trades)
        except Exception as e:
            print(f"  Erro {sym}: {e}")

    return all_trades, incomes

# ============================================================
# 2. ANALISE
# ============================================================

def analisar(trades, incomes):
    print("\n" + "=" * 70)
    print("AUDITORIA COMPLETA — GUARDIAO CNS")
    print("=" * 70)

    # --- INCOME (PnL realizado) ---
    pnl_por_symbol = defaultdict(float)
    pnl_por_tipo = defaultdict(float)
    pnl_por_dia = defaultdict(float)
    pnl_por_hora = defaultdict(float)
    pnl_por_dia_semana = defaultdict(float)
    trades_por_symbol = defaultdict(int)
    wins_por_symbol = defaultdict(int)
    losses_por_symbol = defaultdict(int)

    total_pnl = 0
    total_taxas = 0
    total_funding = 0
    wins = 0
    losses = 0
    maior_win = 0
    maior_loss = 0
    sequencia_wins = 0
    sequencia_losses = 0
    max_seq_wins = 0
    max_seq_losses = 0

    pnl_trades = []  # so REALIZED_PNL

    for i in incomes:
        tipo = i.get("incomeType", "")
        valor = float(i.get("income", 0))
        symbol = i.get("symbol", "GERAL")
        ts = int(i.get("time", 0))
        dt = datetime.fromtimestamp(ts / 1000)

        if tipo == "REALIZED_PNL":
            pnl_por_symbol[symbol] += valor
            pnl_por_dia[dt.strftime("%Y-%m-%d")] += valor
            pnl_por_hora[dt.hour] += valor
            pnl_por_dia_semana[dt.strftime("%A")] += valor
            trades_por_symbol[symbol] += 1
            pnl_trades.append(valor)
            total_pnl += valor

            if valor > 0:
                wins += 1
                wins_por_symbol[symbol] += 1
                maior_win = max(maior_win, valor)
                sequencia_wins += 1
                max_seq_wins = max(max_seq_wins, sequencia_wins)
                sequencia_losses = 0
            elif valor < 0:
                losses += 1
                losses_por_symbol[symbol] += 1
                maior_loss = min(maior_loss, valor)
                sequencia_losses += 1
                max_seq_losses = max(max_seq_losses, sequencia_losses)
                sequencia_wins = 0

        elif tipo == "COMMISSION":
            total_taxas += valor

        elif tipo == "FUNDING_FEE":
            total_funding += valor

        pnl_por_tipo[tipo] += valor

    total_trades = wins + losses
    win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
    avg_win = sum(v for v in pnl_trades if v > 0) / wins if wins > 0 else 0
    avg_loss = sum(v for v in pnl_trades if v < 0) / losses if losses > 0 else 0
    payoff = abs(avg_win / avg_loss) if avg_loss != 0 else 0
    expectativa = (win_rate / 100 * avg_win) + ((1 - win_rate / 100) * avg_loss)
    profit_factor = abs(sum(v for v in pnl_trades if v > 0) / sum(v for v in pnl_trades if v < 0)) if sum(v for v in pnl_trades if v < 0) != 0 else float('inf')

    # Drawdown
    equity_curve = []
    running = 0
    peak = 0
    max_dd = 0
    for v in pnl_trades:
        running += v
        equity_curve.append(running)
        peak = max(peak, running)
        dd = peak - running
        max_dd = max(max_dd, dd)

    recovery_factor = abs(total_pnl / max_dd) if max_dd > 0 else float('inf')

    # ============================================================
    # RESUMO EXECUTIVO
    # ============================================================
    print("\n--- 1. RESUMO EXECUTIVO ---")
    print(f"Periodo: {min(pnl_por_dia.keys()) if pnl_por_dia else 'N/A'} a {max(pnl_por_dia.keys()) if pnl_por_dia else 'N/A'}")
    print(f"PnL realizado total: ${total_pnl:+.2f}")
    print(f"Taxas pagas: ${total_taxas:.2f}")
    print(f"Funding fees: ${total_funding:+.2f}")
    print(f"PnL liquido: ${total_pnl + total_taxas + total_funding:+.2f}")
    print(f"Total trades: {total_trades}")
    print(f"Win rate: {win_rate:.1f}%")
    print(f"Profit factor: {profit_factor:.2f}")
    print(f"Expectativa matematica: ${expectativa:+.4f} por trade")
    print(f"Payoff (avg win / avg loss): {payoff:.2f}")
    print(f"Media de ganho: ${avg_win:+.4f}")
    print(f"Media de perda: ${avg_loss:+.4f}")
    print(f"Maior ganho: ${maior_win:+.4f}")
    print(f"Maior perda: ${maior_loss:+.4f}")
    print(f"Max sequencia wins: {max_seq_wins}")
    print(f"Max sequencia losses: {max_seq_losses}")
    print(f"Drawdown maximo: ${max_dd:.2f}")
    print(f"Recovery factor: {recovery_factor:.2f}")

    # Veredito
    if profit_factor >= 1.5 and win_rate >= 55:
        veredito = "SAUDAVEL — vantagem estatistica presente"
    elif profit_factor >= 1.0:
        veredito = "MARGINAL — lucra mas sem folga. Risco de degradacao."
    else:
        veredito = "DEGRADADO — sem vantagem estatistica. Parar e revisar."
    print(f"\nVEREDITO: {veredito}")

    # ============================================================
    # PnL POR TIPO DE INCOME
    # ============================================================
    print("\n--- 2. PnL POR TIPO ---")
    for tipo, valor in sorted(pnl_por_tipo.items(), key=lambda x: x[1]):
        print(f"  {tipo}: ${valor:+.2f}")

    # ============================================================
    # TOP 20 ATIVOS (LUCRO E PREJUIZO)
    # ============================================================
    print("\n--- 3. TOP 20 ATIVOS MAIS LUCRATIVOS ---")
    top_lucro = sorted(pnl_por_symbol.items(), key=lambda x: x[1], reverse=True)[:20]
    for sym, pnl in top_lucro:
        n = trades_por_symbol[sym]
        w = wins_por_symbol.get(sym, 0)
        l = losses_por_symbol.get(sym, 0)
        wr = (w / n * 100) if n > 0 else 0
        print(f"  {sym:20s} ${pnl:+8.4f} | {n:4d} trades | WR {wr:.0f}% ({w}W/{l}L)")

    print("\n--- 4. TOP 20 ATIVOS MAIS PREJUDICIAIS ---")
    top_perda = sorted(pnl_por_symbol.items(), key=lambda x: x[1])[:20]
    for sym, pnl in top_perda:
        n = trades_por_symbol[sym]
        w = wins_por_symbol.get(sym, 0)
        l = losses_por_symbol.get(sym, 0)
        wr = (w / n * 100) if n > 0 else 0
        print(f"  {sym:20s} ${pnl:+8.4f} | {n:4d} trades | WR {wr:.0f}% ({w}W/{l}L)")

    # ============================================================
    # PnL POR DIA
    # ============================================================
    print("\n--- 5. PnL POR DIA ---")
    dias_positivos = 0
    dias_negativos = 0
    for dia in sorted(pnl_por_dia.keys()):
        pnl_d = pnl_por_dia[dia]
        sinal = "+" if pnl_d >= 0 else ""
        barra = "=" * min(int(abs(pnl_d) * 2), 50)
        if pnl_d >= 0:
            dias_positivos += 1
            print(f"  {dia} ${sinal}{pnl_d:.2f} |{'=' * min(int(pnl_d * 2), 50)}")
        else:
            dias_negativos += 1
            print(f"  {dia} ${sinal}{pnl_d:.2f} |{'#' * min(int(abs(pnl_d) * 2), 50)}")
    print(f"\n  Dias positivos: {dias_positivos} | Negativos: {dias_negativos}")

    # ============================================================
    # PnL POR HORA
    # ============================================================
    print("\n--- 6. PnL POR HORA (UTC) ---")
    for hora in range(24):
        pnl_h = pnl_por_hora.get(hora, 0)
        sinal = "+" if pnl_h >= 0 else ""
        print(f"  {hora:02d}h: ${sinal}{pnl_h:.4f}")

    melhor_hora = max(pnl_por_hora.items(), key=lambda x: x[1]) if pnl_por_hora else (0, 0)
    pior_hora = min(pnl_por_hora.items(), key=lambda x: x[1]) if pnl_por_hora else (0, 0)
    print(f"\n  Melhor hora: {melhor_hora[0]:02d}h (${melhor_hora[1]:+.4f})")
    print(f"  Pior hora: {pior_hora[0]:02d}h (${pior_hora[1]:+.4f})")

    # ============================================================
    # PnL POR DIA DA SEMANA
    # ============================================================
    print("\n--- 7. PnL POR DIA DA SEMANA ---")
    for dia in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]:
        pnl_ds = pnl_por_dia_semana.get(dia, 0)
        print(f"  {dia:12s}: ${pnl_ds:+.4f}")

    # ============================================================
    # CUSTOS OPERACIONAIS
    # ============================================================
    print("\n--- 8. CUSTOS OPERACIONAIS ---")
    print(f"  Taxas (commission): ${total_taxas:.4f}")
    print(f"  Funding fees: ${total_funding:+.4f}")
    custo_total = abs(total_taxas) + abs(total_funding)
    print(f"  Custo total: ${custo_total:.4f}")
    if total_pnl != 0:
        print(f"  Custo como % do PnL bruto: {custo_total / abs(total_pnl) * 100:.1f}%")

    # ============================================================
    # POSICOES ABERTAS ATUAIS
    # ============================================================
    print("\n--- 9. POSICOES ABERTAS ATUAIS ---")
    try:
        account = client.futures_account()
        abertas = [p for p in account["positions"] if abs(float(p["positionAmt"])) > 0]
        saldo_total = float(account.get("totalMarginBalance", 0))
        maint_margin = float(account.get("totalMaintMargin", 0))
        racio = (maint_margin / saldo_total * 100) if saldo_total > 0 else 0

        pnl_aberto_total = 0
        margem_total = 0
        n_long = 0
        n_short = 0
        positivas = 0
        negativas = 0

        posicoes_detalhadas = []
        for p in abertas:
            amt = float(p["positionAmt"])
            lado = "LONG" if amt > 0 else "SHORT"
            if lado == "LONG":
                n_long += 1
            else:
                n_short += 1
            pnl = float(p.get("unrealizedProfit", 0))
            margem = float(p.get("positionInitialMargin", 0))
            roi = (pnl / margem * 100) if margem > 0 else 0
            pnl_aberto_total += pnl
            margem_total += margem
            if pnl >= 0:
                positivas += 1
            else:
                negativas += 1
            posicoes_detalhadas.append((p["symbol"], lado, roi, pnl, margem))

        posicoes_detalhadas.sort(key=lambda x: x[2])  # por ROI

        print(f"  Saldo total: ${saldo_total:.2f}")
        print(f"  Racio de Margem: {racio:.2f}%")
        print(f"  Posicoes: {len(abertas)} ({n_long} LONG / {n_short} SHORT)")
        print(f"  Positivas: {positivas} | Negativas: {negativas}")
        print(f"  PnL aberto: ${pnl_aberto_total:+.2f}")
        print(f"  Margem em uso: ${margem_total:.2f}")
        print()

        # Top 10 piores
        print("  TOP 10 PIORES:")
        for sym, lado, roi, pnl, margem in posicoes_detalhadas[:10]:
            print(f"    {sym:20s} {lado:5s} ROI {roi:+8.1f}% PnL ${pnl:+.4f} Margem ${margem:.4f}")

        print()
        print("  TOP 10 MELHORES:")
        for sym, lado, roi, pnl, margem in posicoes_detalhadas[-10:]:
            print(f"    {sym:20s} {lado:5s} ROI {roi:+8.1f}% PnL ${pnl:+.4f} Margem ${margem:.4f}")

        # Concentracao de margem
        if margem_total > 0:
            print(f"\n  CONCENTRACAO DE MARGEM:")
            top5_margem = sorted(posicoes_detalhadas, key=lambda x: x[4], reverse=True)[:5]
            for sym, lado, roi, pnl, margem in top5_margem:
                pct = margem / margem_total * 100
                print(f"    {sym:20s} ${margem:.4f} ({pct:.1f}% da margem total)")

    except Exception as e:
        print(f"  Erro ao puxar posicoes: {e}")

    # ============================================================
    # PADROES VENCEDORES E PERDEDORES
    # ============================================================
    print("\n--- 10. PADROES ---")

    # Ativos que SEMPRE perdem
    sempre_perdem = [(s, pnl_por_symbol[s]) for s in pnl_por_symbol
                     if wins_por_symbol.get(s, 0) == 0 and losses_por_symbol.get(s, 0) >= 3]
    if sempre_perdem:
        print("\n  ATIVOS QUE SO PERDEM (0 wins, 3+ trades):")
        for sym, pnl in sorted(sempre_perdem, key=lambda x: x[1]):
            print(f"    {sym}: ${pnl:+.4f} ({losses_por_symbol[sym]} losses)")

    # Ativos com WR >= 80% e 5+ trades
    consistentes = [(s, pnl_por_symbol[s], wins_por_symbol.get(s, 0), trades_por_symbol[s])
                    for s in pnl_por_symbol
                    if trades_por_symbol[s] >= 5 and wins_por_symbol.get(s, 0) / trades_por_symbol[s] >= 0.8]
    if consistentes:
        print("\n  ATIVOS CONSISTENTES (WR >= 80%, 5+ trades):")
        for sym, pnl, w, n in sorted(consistentes, key=lambda x: x[1], reverse=True):
            print(f"    {sym}: ${pnl:+.4f} | WR {w/n*100:.0f}% ({w}/{n})")

    # ============================================================
    # ERROS RECORRENTES
    # ============================================================
    print("\n--- 11. ERROS RECORRENTES DETECTADOS ---")

    erros = []

    # Excesso de posicoes
    if len(abertas) > 50:
        erros.append(f"EXCESSO DE POSICOES: {len(abertas)} abertas. Diluicao extrema de capital.")

    # Racio alto
    if racio > 15:
        erros.append(f"RACIO DE MARGEM ALTO: {racio:.1f}%. Risco de liquidacao aumentado.")

    # Custos corroendo
    if custo_total > abs(total_pnl) * 0.3:
        erros.append(f"CUSTOS CORROENDO RESULTADO: taxas+funding = {custo_total / abs(total_pnl) * 100:.0f}% do PnL bruto")

    # Muitas posicoes negativas
    if negativas > positivas * 2:
        erros.append(f"DESEQUILIBRIO: {negativas} negativas vs {positivas} positivas. Mais formiguinhas mas estao morrendo.")

    # Payoff baixo
    if payoff < 1.0:
        erros.append(f"PAYOFF BAIXO: {payoff:.2f}. Ganhos menores que perdas em media.")

    # Win rate baixo
    if win_rate < 50:
        erros.append(f"WIN RATE BAIXO: {win_rate:.1f}%. Mais da metade dos trades perde.")

    for e in erros:
        print(f"  ** {e}")

    if not erros:
        print("  Nenhum erro critico detectado.")

    # ============================================================
    # RECOMENDACOES
    # ============================================================
    print("\n--- 12. RECOMENDACOES PRATICAS ---")

    recomendacoes = []

    # Baseado nos dados
    if len(abertas) > 50:
        recomendacoes.append("REDUZIR posicoes: fechar as 20 piores em ROI que nao tem DCA ativo. Margem liberada fortalece as boas.")

    if profit_factor < 1.5:
        recomendacoes.append("AUMENTAR SELETIVIDADE: endurecer filtros de entrada. So entrar com score >= 70 em vez de 50.")

    if custo_total > abs(total_pnl) * 0.2:
        recomendacoes.append("REDUZIR FREQUENCIA: menos trades = menos taxas. Qualidade > quantidade.")

    # A QUESTAO DAS FORMIGUINHAS
    recomendacoes.append(
        "DEIXAR VENCEDORAS CORREREM: o trailing esta DESABILITADO (if False: linha 3793), "
        "mas a cascata esta cortando em +20%/+40%/+80%. Se a formiguinha chega a +20% e a "
        "cascata fecha 30%, ela perde forca. Considerar subir o limiar da cascata 1 para +50% "
        "ou remover a cascata e deixar SÓ o trailing por MA reversal."
    )

    recomendacoes.append(
        "FORMIGA SOLDADO CONFLITA COM CASCATA: o soldado triplica margem em +15% e a cascata "
        "fecha 30% em +20%. Resultado: soldado entra e 5 min depois cascata corta. Escolher UM."
    )

    for i, r in enumerate(recomendacoes, 1):
        print(f"  {i}. {r}")

    # ============================================================
    # MODO OPERACIONAL RECOMENDADO
    # ============================================================
    print("\n--- 13. MODO OPERACIONAL RECOMENDADO ---")
    if racio > 25:
        print("  >>> MODO PAUSA: racio perigoso. Nao abrir posicoes novas ate < 15%.")
    elif max_dd > 20 or profit_factor < 1.0:
        print("  >>> MODO PROTECAO: vantagem degradada. Reduzir risco por trade.")
    elif profit_factor < 1.3 or win_rate < 55:
        print("  >>> MODO CAUTELA: resultados marginais. Apertar filtros.")
    else:
        print("  >>> MODO NORMAL: vantagem presente. Manter estrategia.")

    print("\n" + "=" * 70)
    print("FIM DA AUDITORIA")
    print("=" * 70)

    return {
        "total_pnl": total_pnl,
        "total_taxas": total_taxas,
        "total_funding": total_funding,
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "payoff": payoff,
        "max_dd": max_dd,
        "total_trades": total_trades,
        "posicoes_abertas": len(abertas) if abertas else 0,
        "racio": racio,
    }


if __name__ == "__main__":
    print("Puxando dados da Binance...")
    trades, incomes = puxar_trades_recentes(dias=30)
    print(f"Trades: {len(trades)} | Incomes: {len(incomes)}")

    # Salva dados brutos
    with open("C:/robo-trade/auditoria_dados.json", "w") as f:
        json.dump({"trades": trades, "incomes": incomes}, f, indent=2)
    print("Dados salvos em auditoria_dados.json")

    resultado = analisar(trades, incomes)

    # Salva resultado
    with open("C:/robo-trade/auditoria_resultado.json", "w") as f:
        json.dump(resultado, f, indent=2)
    print("\nResultado salvo em auditoria_resultado.json")
