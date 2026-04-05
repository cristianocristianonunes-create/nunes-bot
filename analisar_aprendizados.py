#!/usr/bin/env python3
"""
Analisador de aprendizados do bot CNS Guardião.
Lê aprendizados.json e extrai padrões de sucesso e fracasso.
"""

import json
from collections import Counter, defaultdict
from datetime import datetime

APRENDIZADOS_FILE = "C:/robo-trade/aprendizados.json"


def carregar_aprendizados():
    try:
        with open(APRENDIZADOS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Erro ao carregar: {e}")
        return []


def classificar_tipo(tipo: str) -> str:
    """Retorna 'sucesso' ou 'fracasso' baseado no tipo do registro."""
    if "sucesso" in tipo.lower():
        return "sucesso"
    if "racio_fechamento" in tipo.lower():
        return "fracasso"
    if "fracasso" in tipo.lower():
        return "fracasso"
    return "neutro"


def analisar(aprendizados: list):
    total = len(aprendizados)
    if total == 0:
        print("Nenhum aprendizado registrado ainda.")
        return

    sucessos = [a for a in aprendizados if classificar_tipo(a["tipo"]) == "sucesso"]
    fracassos = [a for a in aprendizados if classificar_tipo(a["tipo"]) == "fracasso"]

    print("=" * 70)
    print("ANALISE DE APRENDIZADOS — CNS GUARDIAO")
    print("=" * 70)
    print(f"\nTotal de registros: {total}")
    print(f"Sucessos: {len(sucessos)} ({len(sucessos)/total*100:.1f}%)")
    print(f"Fracassos: {len(fracassos)} ({len(fracassos)/total*100:.1f}%)")

    # --- TIPOS DE OPERAÇÃO ---
    print("\n" + "-" * 70)
    print("TIPOS DE OPERACAO")
    print("-" * 70)
    tipos = Counter(a["tipo"] for a in aprendizados)
    for tipo, qtd in tipos.most_common():
        print(f"  {tipo}: {qtd}")

    # --- ATIVOS MAIS FREQUENTES ---
    print("\n" + "-" * 70)
    print("ATIVOS MAIS FREQUENTES")
    print("-" * 70)
    ativos = Counter(a["symbol"] for a in aprendizados)
    for symbol, qtd in ativos.most_common(10):
        s_count = sum(1 for a in aprendizados
                     if a["symbol"] == symbol and classificar_tipo(a["tipo"]) == "sucesso")
        f_count = qtd - s_count
        print(f"  {symbol}: {qtd} ops | {s_count} sucesso | {f_count} fracasso")

    # --- DIREÇÃO (LONG vs SHORT) ---
    print("\n" + "-" * 70)
    print("DIRECAO")
    print("-" * 70)
    for dir_tipo in ["LONG", "SHORT"]:
        total_dir = sum(1 for a in aprendizados if a.get("direcao") == dir_tipo)
        s_dir = sum(1 for a in aprendizados
                   if a.get("direcao") == dir_tipo and classificar_tipo(a["tipo"]) == "sucesso")
        if total_dir > 0:
            taxa = s_dir / total_dir * 100
            print(f"  {dir_tipo}: {total_dir} ops | {s_dir} sucessos | win rate {taxa:.1f}%")

    # --- PADRÕES DE MA (1min, 5min, 1H) ---
    print("\n" + "-" * 70)
    print("PADROES DE MA NOS SUCESSOS")
    print("-" * 70)
    if sucessos:
        padroes_sucesso = defaultdict(int)
        for a in sucessos:
            snap = a.get("snapshot", {})
            m1 = snap.get("1min", {}).get("ma7_acima_ma25")
            m5 = snap.get("5min", {}).get("ma7_acima_ma25")
            m1h = snap.get("1h", {}).get("ma7_acima_ma25")
            key = f"1min={'>' if m1 else '<'} | 5min={'>' if m5 else '<'} | 1h={'>' if m1h else '<'}"
            padroes_sucesso[key] += 1
        for padrao, qtd in sorted(padroes_sucesso.items(), key=lambda x: -x[1]):
            print(f"  {padrao}: {qtd}x")

    print("\n" + "-" * 70)
    print("PADROES DE MA NOS FRACASSOS")
    print("-" * 70)
    if fracassos:
        padroes_fracasso = defaultdict(int)
        for a in fracassos:
            snap = a.get("snapshot", {})
            m1 = snap.get("1min", {}).get("ma7_acima_ma25")
            m5 = snap.get("5min", {}).get("ma7_acima_ma25")
            m1h = snap.get("1h", {}).get("ma7_acima_ma25")
            key = f"1min={'>' if m1 else '<'} | 5min={'>' if m5 else '<'} | 1h={'>' if m1h else '<'}"
            padroes_fracasso[key] += 1
        for padrao, qtd in sorted(padroes_fracasso.items(), key=lambda x: -x[1]):
            print(f"  {padrao}: {qtd}x")

    # --- ZONA FIBONACCI ---
    print("\n" + "-" * 70)
    print("ZONA FIBONACCI NO MOMENTO")
    print("-" * 70)
    zonas_sucesso = Counter()
    zonas_fracasso = Counter()
    for a in aprendizados:
        zona = a.get("snapshot", {}).get("fibonacci", {}).get("preco_zona", "desconhecida")
        if classificar_tipo(a["tipo"]) == "sucesso":
            zonas_sucesso[zona] += 1
        else:
            zonas_fracasso[zona] += 1
    print("Sucessos:")
    for zona, qtd in zonas_sucesso.most_common():
        print(f"  {zona}: {qtd}x")
    print("Fracassos:")
    for zona, qtd in zonas_fracasso.most_common():
        print(f"  {zona}: {qtd}x")

    # --- ROI FINAL ---
    print("\n" + "-" * 70)
    print("ROI FINAL")
    print("-" * 70)
    if sucessos:
        rois_s = [a.get("roi_final", 0) for a in sucessos]
        print(f"  Sucessos: media {sum(rois_s)/len(rois_s):+.1f}% | max {max(rois_s):+.1f}% | min {min(rois_s):+.1f}%")
    if fracassos:
        rois_f = [a.get("roi_final", 0) for a in fracassos]
        print(f"  Fracassos: media {sum(rois_f)/len(rois_f):+.1f}% | max {max(rois_f):+.1f}% | min {min(rois_f):+.1f}%")

    # --- HORÁRIO ---
    print("\n" + "-" * 70)
    print("HORARIO (BRT) DOS SUCESSOS")
    print("-" * 70)
    if sucessos:
        horas = Counter()
        for a in sucessos:
            try:
                ts = datetime.fromisoformat(a["timestamp"].replace("Z", "+00:00"))
                hora_brt = (ts.hour - 3) % 24  # UTC-3
                horas[hora_brt] += 1
            except Exception:
                continue
        for hora, qtd in sorted(horas.items()):
            barra = "#" * qtd
            print(f"  {hora:02d}h: {barra} ({qtd})")

    # --- CONCLUSÕES AUTOMÁTICAS ---
    print("\n" + "=" * 70)
    print("CONCLUSOES AUTOMATICAS")
    print("=" * 70)

    if total < 20:
        print("\n⚠  Amostra ainda pequena (< 20 registros). Conclusoes sao preliminares.")
    else:
        print("\n✓ Amostra estatisticamente relevante.")

    # Taxa de sucesso geral
    if total > 0:
        win_rate = len(sucessos) / total * 100
        print(f"\nWin rate geral: {win_rate:.1f}%")
        if win_rate >= 60:
            print("  → SISTEMA POSITIVO. Padrao funcionando.")
        elif win_rate >= 40:
            print("  → SISTEMA NEUTRO. Precisa ajuste fino.")
        else:
            print("  → SISTEMA NEGATIVO. Revisar criterios de entrada.")

    # Direção preferida
    if sucessos:
        longs = sum(1 for a in sucessos if a.get("direcao") == "LONG")
        shorts = sum(1 for a in sucessos if a.get("direcao") == "SHORT")
        if longs > shorts * 1.5:
            print(f"\n→ LONG dominando ({longs}x vs {shorts}x). Tendencia geral de alta.")
        elif shorts > longs * 1.5:
            print(f"\n→ SHORT dominando ({shorts}x vs {longs}x). Tendencia geral de baixa.")
        else:
            print(f"\n→ LONG/SHORT equilibrados ({longs}x vs {shorts}x). Mercado lateral.")

    # Ativos de alta recorrência com fracasso alto
    ativos_ruins = []
    for symbol, qtd in ativos.items():
        if qtd >= 2:
            f = sum(1 for a in aprendizados
                   if a["symbol"] == symbol and classificar_tipo(a["tipo"]) == "fracasso")
            if f / qtd >= 0.7:
                ativos_ruins.append((symbol, qtd, f))
    if ativos_ruins:
        print("\n→ Ativos com muito fracasso (considere remover da lista CNS):")
        for sym, q, f in ativos_ruins:
            print(f"    {sym}: {f}/{q} fracassos")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    aprendizados = carregar_aprendizados()
    analisar(aprendizados)
