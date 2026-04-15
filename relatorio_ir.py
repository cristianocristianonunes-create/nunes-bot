#!/usr/bin/env python3
"""
Relatorio para Imposto de Renda — Operacoes Cripto Futuros
Gera relatorios mensais com PnL realizado, taxas, funding.
Para todas as contas (sua, Lisa, Gabriel).

USO: python relatorio_ir.py
"""

import os
import json
import sys
from datetime import datetime, timedelta
from collections import defaultdict
from dotenv import load_dotenv
from binance.client import Client

sys.stdout.reconfigure(encoding='utf-8')


def gerar_relatorio(nome_conta: str, env_path: str):
    """Gera relatorio IR para uma conta."""
    load_dotenv(env_path, override=True)
    client = Client(os.getenv("BINANCE_API_KEY"), os.getenv("BINANCE_API_SECRET"))

    print(f"\n{'=' * 70}")
    print(f"RELATORIO IR — {nome_conta.upper()}")
    print(f"{'=' * 70}")

    # Saldo atual
    acc = client.futures_account()
    saldo = float(acc.get("totalMarginBalance", 0))
    try:
        usd_brl = float(client.get_symbol_ticker(symbol="USDTBRL")["price"])
    except:
        usd_brl = 5.0

    print(f"Saldo atual: ${saldo:.2f} (R${saldo * usd_brl:.2f})")
    print(f"Cotacao USD/BRL: R${usd_brl:.2f}")
    print(f"Data: {datetime.now().strftime('%d/%m/%Y %H:%M')}")

    # Puxa historico completo (ate 90 dias pela API)
    inicio = int((datetime.now() - timedelta(days=90)).timestamp() * 1000)
    todos_incomes = []
    cursor = inicio
    end = int(datetime.now().timestamp() * 1000)

    while cursor < end:
        try:
            incomes = client.futures_income_history(startTime=cursor, limit=1000)
            if not incomes:
                break
            todos_incomes.extend(incomes)
            if len(incomes) < 1000:
                break
            cursor = int(incomes[-1]["time"]) + 1
        except Exception as e:
            print(f"  Erro: {e}")
            break

    print(f"Total de registros: {len(todos_incomes)}")

    # Agrupa por mes
    por_mes = defaultdict(lambda: {
        "pnl_realizado": 0, "comissoes": 0, "funding": 0,
        "transferencias": 0, "trades": 0, "wins": 0, "losses": 0,
        "maior_win": 0, "maior_loss": 0,
        "pnl_por_symbol": defaultdict(float),
    })

    for i in todos_incomes:
        tipo = i.get("incomeType", "")
        valor = float(i.get("income", 0))
        ts = int(i.get("time", 0))
        dt = datetime.fromtimestamp(ts / 1000)
        mes = dt.strftime("%Y-%m")
        symbol = i.get("symbol", "")

        if tipo == "REALIZED_PNL":
            por_mes[mes]["pnl_realizado"] += valor
            por_mes[mes]["trades"] += 1
            por_mes[mes]["pnl_por_symbol"][symbol] += valor
            if valor > 0:
                por_mes[mes]["wins"] += 1
                por_mes[mes]["maior_win"] = max(por_mes[mes]["maior_win"], valor)
            else:
                por_mes[mes]["losses"] += 1
                por_mes[mes]["maior_loss"] = min(por_mes[mes]["maior_loss"], valor)
        elif tipo == "COMMISSION":
            por_mes[mes]["comissoes"] += valor
        elif tipo == "FUNDING_FEE":
            por_mes[mes]["funding"] += valor
        elif tipo == "TRANSFER":
            por_mes[mes]["transferencias"] += valor

    # Imprime relatorio mensal
    print(f"\n{'MES':10s} {'PnL BRUTO':>12} {'COMISSOES':>12} {'FUNDING':>10} {'LIQUIDO':>12} {'R$ LIQUIDO':>12} {'TRADES':>8} {'WR':>6}")
    print("-" * 85)

    total_liq = 0
    total_taxas = 0
    total_funding = 0

    for mes in sorted(por_mes.keys()):
        d = por_mes[mes]
        liquido = d["pnl_realizado"] + d["comissoes"] + d["funding"]
        wr = (d["wins"] / d["trades"] * 100) if d["trades"] > 0 else 0
        liq_brl = liquido * usd_brl

        total_liq += liquido
        total_taxas += d["comissoes"]
        total_funding += d["funding"]

        print(f"  {mes:10s} ${d['pnl_realizado']:>+10.2f} ${d['comissoes']:>10.2f} ${d['funding']:>+8.2f} ${liquido:>+10.2f} R${liq_brl:>+10.2f} {d['trades']:>6} {wr:>5.0f}%")

    print("-" * 85)
    print(f"  {'TOTAL':10s} {'':>12} ${total_taxas:>10.2f} ${total_funding:>+8.2f} ${total_liq:>+10.2f} R${total_liq * usd_brl:>+10.2f}")

    # Detalhamento por ativo (top 10 lucro e top 10 prejuizo)
    print(f"\n--- TOP 10 ATIVOS MAIS LUCRATIVOS ---")
    all_symbols = defaultdict(float)
    for mes in por_mes:
        for sym, val in por_mes[mes]["pnl_por_symbol"].items():
            all_symbols[sym] += val

    top_lucro = sorted(all_symbols.items(), key=lambda x: x[1], reverse=True)[:10]
    for sym, val in top_lucro:
        if val > 0:
            print(f"  {sym:20s} ${val:>+10.2f} (R${val * usd_brl:>+10.2f})")

    print(f"\n--- TOP 10 ATIVOS MAIS PREJUDICIAIS ---")
    top_perda = sorted(all_symbols.items(), key=lambda x: x[1])[:10]
    for sym, val in top_perda:
        if val < 0:
            print(f"  {sym:20s} ${val:>+10.2f} (R${val * usd_brl:>+10.2f})")

    # Transferencias (depositos/saques)
    print(f"\n--- DEPOSITOS E SAQUES ---")
    for mes in sorted(por_mes.keys()):
        transf = por_mes[mes]["transferencias"]
        if transf != 0:
            print(f"  {mes}: ${transf:>+10.2f} (R${transf * usd_brl:>+10.2f})")

    # Salva em arquivo
    arquivo = os.path.join(os.path.dirname(env_path), f"ir_{nome_conta}_{datetime.now().strftime('%Y%m%d')}.txt")
    return {
        "conta": nome_conta,
        "saldo_usd": saldo,
        "saldo_brl": saldo * usd_brl,
        "total_liquido_usd": total_liq,
        "total_liquido_brl": total_liq * usd_brl,
        "total_taxas": total_taxas,
        "meses": dict(por_mes),
    }


if __name__ == "__main__":
    resultados = []

    # SUA CONTA
    if os.path.exists("C:/robo-trade/.env"):
        r = gerar_relatorio("Cristiano", "C:/robo-trade/.env")
        resultados.append(r)

    # LISA
    if os.path.exists("C:/robo-trade-lisa/.env"):
        r = gerar_relatorio("Lisa", "C:/robo-trade-lisa/.env")
        resultados.append(r)

    # GABRIEL
    if os.path.exists("C:/robo-trade-gabriel/.env"):
        r = gerar_relatorio("Gabriel", "C:/robo-trade-gabriel/.env")
        resultados.append(r)

    # Resumo geral
    print(f"\n{'=' * 70}")
    print(f"RESUMO GERAL — TODAS AS CONTAS")
    print(f"{'=' * 70}")
    total_geral = 0
    for r in resultados:
        print(f"  {r['conta']:15s}: Saldo R${r['saldo_brl']:>10,.2f} | PnL liquido R${r['total_liquido_brl']:>+10,.2f}")
        total_geral += r["total_liquido_brl"]
    print(f"  {'TOTAL':15s}: PnL liquido R${total_geral:>+10,.2f}")
    print()
    print("IMPORTANTE: Este relatorio e informativo. Consulte seu contador")
    print("para a declaracao oficial do Imposto de Renda.")
