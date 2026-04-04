#!/usr/bin/env python3
"""
Jarvis — Assistente Claude no Telegram + Controle Total do Bot Nunes
Acesso ao log, estado, controle de operações, leitura/edição de código e comandos do sistema.
"""

import os
import json
import logging
import asyncio
import subprocess
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, MessageHandler, CommandHandler, filters, ContextTypes
import anthropic
from binance.client import Client
from binance.exceptions import BinanceAPIException

load_dotenv()

ANTHROPIC_API_KEY  = os.getenv("ANTHROPIC_API_KEY")
TELEGRAM_TOKEN     = os.getenv("JARVIS_TELEGRAM_TOKEN")
CHAT_ID_AUTORIZADO = int(os.getenv("TELEGRAM_CHAT_ID", "0"))
API_KEY            = os.getenv("BINANCE_API_KEY")
API_SECRET         = os.getenv("BINANCE_API_SECRET")
ALAVANCAGEM        = int(os.getenv("ALAVANCAGEM", "20"))
LOG_FILE           = "C:/robo-trade/robo.log"
ESTADO_FILE        = "C:/robo-trade/estado_bot.json"
PROJETO_DIR        = "C:/robo-trade"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("jarvis")

cliente_claude  = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
cliente_binance = Client(API_KEY, API_SECRET)

historico: dict[int, list] = {}
MAX_HISTORICO = 20


# ---------------------------------------------------------------------------
# Ferramentas para o Claude (tool use)
# ---------------------------------------------------------------------------
TOOLS = [
    {
        "name": "ler_arquivo",
        "description": "Lê um arquivo do projeto. Retorna o conteúdo com números de linha.",
        "input_schema": {
            "type": "object",
            "properties": {
                "caminho": {"type": "string", "description": "Caminho do arquivo (ex: C:/robo-trade/nunes.py)"},
                "linha_inicio": {"type": "integer", "description": "Linha inicial (opcional, default 1)"},
                "linha_fim": {"type": "integer", "description": "Linha final (opcional, default até o fim)"}
            },
            "required": ["caminho"]
        }
    },
    {
        "name": "editar_arquivo",
        "description": "Edita um arquivo substituindo um trecho de texto por outro.",
        "input_schema": {
            "type": "object",
            "properties": {
                "caminho": {"type": "string", "description": "Caminho do arquivo"},
                "texto_antigo": {"type": "string", "description": "Texto exato a ser substituído"},
                "texto_novo": {"type": "string", "description": "Texto novo que vai no lugar"}
            },
            "required": ["caminho", "texto_antigo", "texto_novo"]
        }
    },
    {
        "name": "executar_comando",
        "description": "Executa um comando no terminal (git, python, etc).",
        "input_schema": {
            "type": "object",
            "properties": {
                "comando": {"type": "string", "description": "Comando a executar (ex: git status, git push)"}
            },
            "required": ["comando"]
        }
    },
    {
        "name": "ler_log_bot",
        "description": "Lê as últimas N linhas do log do bot Nunes.",
        "input_schema": {
            "type": "object",
            "properties": {
                "linhas": {"type": "integer", "description": "Número de linhas (default 40)"}
            }
        }
    },
    {
        "name": "posicoes_abertas",
        "description": "Retorna as posições abertas na Binance com ROI e PnL.",
        "input_schema": {"type": "object", "properties": {}}
    },
    {
        "name": "fechar_posicao",
        "description": "Fecha uma posição aberta na Binance.",
        "input_schema": {
            "type": "object",
            "properties": {
                "symbol": {"type": "string", "description": "Par (ex: KATUSDT)"}
            },
            "required": ["symbol"]
        }
    },
    {
        "name": "aplicar_dca",
        "description": "Aplica DCA (35% da margem) em uma posição aberta.",
        "input_schema": {
            "type": "object",
            "properties": {
                "symbol": {"type": "string", "description": "Par (ex: KATUSDT)"}
            },
            "required": ["symbol"]
        }
    },
    {
        "name": "saldo_conta",
        "description": "Retorna saldo disponível, total e rácio de margem.",
        "input_schema": {"type": "object", "properties": {}}
    }
]


def executar_tool(nome: str, params: dict) -> str:
    """Executa uma ferramenta e retorna o resultado como string."""
    try:
        if nome == "ler_arquivo":
            caminho = params["caminho"]
            with open(caminho, "r", encoding="utf-8", errors="replace") as f:
                linhas = f.readlines()
            inicio = params.get("linha_inicio", 1) - 1
            fim = params.get("linha_fim", len(linhas))
            trecho = linhas[max(0, inicio):fim]
            resultado = ""
            for i, linha in enumerate(trecho, start=inicio + 1):
                resultado += f"{i}\t{linha}"
            if len(resultado) > 6000:
                resultado = resultado[:6000] + "\n... (truncado)"
            return resultado

        elif nome == "editar_arquivo":
            caminho = params["caminho"]
            with open(caminho, "r", encoding="utf-8") as f:
                conteudo = f.read()
            if params["texto_antigo"] not in conteudo:
                return "ERRO: texto_antigo nao encontrado no arquivo."
            novo = conteudo.replace(params["texto_antigo"], params["texto_novo"], 1)
            with open(caminho, "w", encoding="utf-8") as f:
                f.write(novo)
            return "Arquivo editado com sucesso."

        elif nome == "executar_comando":
            resultado = subprocess.run(
                params["comando"], shell=True, capture_output=True,
                text=True, timeout=30, cwd=PROJETO_DIR
            )
            saida = resultado.stdout + resultado.stderr
            if len(saida) > 4000:
                saida = saida[:4000] + "\n... (truncado)"
            return saida or "(sem saida)"

        elif nome == "ler_log_bot":
            n = params.get("linhas", 40)
            with open(LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
                todas = f.readlines()
            return "".join(todas[-n:])

        elif nome == "posicoes_abertas":
            posicoes = get_posicoes_abertas()
            if not posicoes:
                return "Nenhuma posicao aberta."
            linhas = []
            for p in posicoes:
                amt = float(p["positionAmt"])
                direcao = "LONG" if amt > 0 else "SHORT"
                pnl = float(p.get("unrealizedProfit", p.get("unRealizedProfit", 0)))
                margem = float(p.get("initialMargin", p.get("positionInitialMargin", 0)))
                roi = (pnl / margem * 100) if margem > 0 else 0
                linhas.append(f"{p['symbol']} | {direcao} | ROI: {roi:+.1f}% | PnL: {pnl:+.2f} USDT")
            return "\n".join(linhas)

        elif nome == "fechar_posicao":
            return fechar_posicao_fn(params["symbol"])

        elif nome == "aplicar_dca":
            return aplicar_dca_fn(params["symbol"])

        elif nome == "saldo_conta":
            saldo = get_saldo()
            racio = get_racio_margem()
            return (
                f"Disponivel: ${saldo['disponivel']:.2f} USDT\n"
                f"Total: ${saldo['total']:.2f} USDT\n"
                f"Racio de Margem: {racio:.1f}%"
            )

        return f"Ferramenta desconhecida: {nome}"
    except Exception as e:
        return f"Erro ao executar {nome}: {e}"


# ---------------------------------------------------------------------------
# Funções Binance (renomeadas para evitar conflito)
# ---------------------------------------------------------------------------
def get_posicoes_abertas() -> list:
    posicoes = cliente_binance.futures_position_information()
    return [p for p in posicoes if float(p["positionAmt"]) != 0]


def get_saldo() -> dict:
    balances = cliente_binance.futures_account_balance()
    for b in balances:
        if b["asset"] == "USDT":
            return {"disponivel": float(b["availableBalance"]), "total": float(b["balance"])}
    return {"disponivel": 0, "total": 0}


def get_racio_margem() -> float:
    try:
        conta = cliente_binance.futures_account()
        maint = float(conta["totalMaintMargin"])
        balance = float(conta["totalMarginBalance"])
        if balance > 0:
            return round((maint / balance) * 100, 2)
    except Exception:
        pass
    return 0.0


def fechar_posicao_fn(symbol: str) -> str:
    posicoes = get_posicoes_abertas()
    posicao = next((p for p in posicoes if p["symbol"] == symbol.upper()), None)
    if not posicao:
        return f"Nenhuma posicao aberta em {symbol.upper()}"
    amt = float(posicao["positionAmt"])
    direcao = "LONG" if amt > 0 else "SHORT"
    side = "SELL" if direcao == "LONG" else "BUY"
    try:
        cliente_binance.futures_cancel_all_open_orders(symbol=symbol.upper())
        cliente_binance.futures_create_order(
            symbol=symbol.upper(), side=side, type="MARKET",
            quantity=abs(amt), reduceOnly=True
        )
        pnl = float(posicao.get("unrealizedProfit", posicao.get("unRealizedProfit", 0)))
        return f"Posicao {direcao} {symbol.upper()} fechada. PnL: {pnl:+.2f} USDT"
    except BinanceAPIException as e:
        return f"Erro ao fechar {symbol.upper()}: {e.message}"


def aplicar_dca_fn(symbol: str) -> str:
    posicoes = get_posicoes_abertas()
    posicao = next((p for p in posicoes if p["symbol"] == symbol.upper()), None)
    if not posicao:
        return f"Nenhuma posicao aberta em {symbol.upper()}"
    amt = float(posicao["positionAmt"])
    direcao = "LONG" if amt > 0 else "SHORT"
    entry = float(posicao["entryPrice"])
    leverage = float(posicao.get("leverage", ALAVANCAGEM))
    margem_atual = round((entry * abs(amt)) / leverage, 2)
    adicional = round(margem_atual * 0.35, 2)
    preco = float(cliente_binance.futures_symbol_ticker(symbol=symbol.upper())["price"])
    try:
        info = cliente_binance.futures_exchange_info()
        precisao = 3
        for s in info["symbols"]:
            if s["symbol"] == symbol.upper():
                precisao = int(s.get("quantityPrecision", 3))
                break
    except Exception:
        precisao = 3
    quantidade = round((adicional * ALAVANCAGEM) / preco, precisao)
    if quantidade <= 0:
        return "DCA bloqueado: quantidade = 0 (saldo insuficiente)"
    side = "BUY" if direcao == "LONG" else "SELL"
    try:
        cliente_binance.futures_create_order(
            symbol=symbol.upper(), side=side, type="MARKET", quantity=quantidade
        )
        return f"DCA aplicado em {symbol.upper()} ({direcao})\nMargem +${adicional:.2f} (35%) | Qtd: {quantidade}"
    except BinanceAPIException as e:
        return f"Erro DCA {symbol.upper()}: {e.message}"


# ---------------------------------------------------------------------------
# System prompt e Claude com tool use
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """Voce e o Jarvis, assistente pessoal do Cristiano no Telegram.
Responda sempre em portugues brasileiro, de forma direta e concisa.
Sem emojis, sem formalidades excessivas.

Voce e o assistente do bot de trade Nunes — um robo de Binance Futuros.
Voce tem acesso TOTAL ao sistema:
- Ler e editar arquivos de codigo (nunes.py, jarvis.py, .env)
- Executar comandos no terminal (git, python, etc)
- Ver posicoes, saldo, racio de margem em tempo real
- Fechar posicoes e aplicar DCA
- Ler o log do bot

Quando o usuario pedir para alterar o codigo, use as ferramentas:
1. ler_arquivo para ver o codigo atual
2. editar_arquivo para fazer a alteracao
3. executar_comando para fazer git add, commit e push

SEMPRE leia o arquivo antes de editar. NUNCA edite sem ler primeiro.
Ao fazer commit, use mensagens descritivas em portugues.

Estrategia do bot:
- Filtro de entrada: 1H (direcao) > 5min (alinhamento) > M1 (gatilho + ADX + volume)
- Trailing escalonado: pico >= 5% (tol 40%), >= 20% (tol 20%), >= 50% (tol 15%)
- Stop -30% sem sinal de MA: fecha entrada errada
- Stop loss maximo: -100% ROI
- DCA com prioridade por sinal (nao FIFO)
- Meta formiguinha: 2% por ciclo, sobe 0.5% a cada 5 ciclos positivos
- Protecao racio: 15%/18%/20%/25% escalonada"""


def auth(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_chat.id != CHAT_ID_AUTORIZADO:
            return
        return await func(update, context)
    return wrapper


# ---------------------------------------------------------------------------
# Comandos rápidos (sem Claude API — economia de tokens)
# ---------------------------------------------------------------------------
@auth
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Jarvis online — acesso total.\n\n"
        "Comandos rapidos:\n"
        "/posicoes — posicoes abertas\n"
        "/saldo — saldo e racio\n"
        "/fechar SYMBOL — fechar posicao\n"
        "/fechar_tudo — fechar todas\n"
        "/dca SYMBOL — aplicar DCA\n"
        "/status — log recente\n"
        "/cmd COMANDO — executar comando no terminal\n"
        "/ler ARQUIVO — ler arquivo\n"
        "/limpar — resetar conversa\n\n"
        "Ou mande qualquer mensagem — eu leio o log, edito codigo, faço commit."
    )


@auth
async def cmd_posicoes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    resultado = executar_tool("posicoes_abertas", {})
    racio = get_racio_margem()
    await update.message.reply_text(f"Racio: {racio:.1f}%\n\n{resultado}")


@auth
async def cmd_saldo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    resultado = executar_tool("saldo_conta", {})
    await update.message.reply_text(resultado)


@auth
async def cmd_fechar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("Uso: /fechar SYMBOL")
        return
    resultado = fechar_posicao_fn(args[0].upper())
    await update.message.reply_text(resultado)


@auth
async def cmd_fechar_tudo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    posicoes = get_posicoes_abertas()
    if not posicoes:
        await update.message.reply_text("Nenhuma posicao aberta.")
        return
    resultados = [fechar_posicao_fn(p["symbol"]) for p in posicoes]
    await update.message.reply_text("\n".join(resultados))


@auth
async def cmd_dca(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("Uso: /dca SYMBOL")
        return
    resultado = aplicar_dca_fn(args[0].upper())
    await update.message.reply_text(resultado)


@auth
async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    resultado = executar_tool("ler_log_bot", {"linhas": 20})
    await update.message.reply_text(resultado[-3500:])


@auth
async def cmd_executar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Uso: /cmd git status")
        return
    comando = " ".join(context.args)
    resultado = executar_tool("executar_comando", {"comando": comando})
    await update.message.reply_text(resultado[-3500:])


@auth
async def cmd_ler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Uso: /ler nunes.py [inicio] [fim]")
        return
    arquivo = context.args[0]
    if "/" not in arquivo and "\\" not in arquivo:
        arquivo = f"C:/robo-trade/{arquivo}"
    params = {"caminho": arquivo}
    if len(context.args) >= 2:
        params["linha_inicio"] = int(context.args[1])
    if len(context.args) >= 3:
        params["linha_fim"] = int(context.args[2])
    resultado = executar_tool("ler_arquivo", params)
    if len(resultado) > 3500:
        resultado = resultado[:3500] + "\n... (truncado — use linha_inicio e linha_fim)"
    await update.message.reply_text(resultado)


@auth
async def cmd_limpar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    historico.pop(update.effective_chat.id, None)
    await update.message.reply_text("Historico limpo.")


# ---------------------------------------------------------------------------
# Chat livre com Claude + tool use (oficina completa)
# ---------------------------------------------------------------------------
@auth
async def responder(update: Update, context: ContextTypes.DEFAULT_TYPE):
    mensagem = update.message.text
    if not mensagem:
        return

    chat_id = update.effective_chat.id
    log.info(f"Recebido: {mensagem}")
    await update.message.chat.send_action("typing")

    if chat_id not in historico:
        historico[chat_id] = []

    historico[chat_id].append({"role": "user", "content": mensagem})

    if len(historico[chat_id]) > MAX_HISTORICO:
        historico[chat_id] = historico[chat_id][-MAX_HISTORICO:]

    try:
        # Loop de tool use — Claude pode chamar ferramentas múltiplas vezes
        messages = list(historico[chat_id])
        max_iteracoes = 10

        for _ in range(max_iteracoes):
            resposta = cliente_claude.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=2048,
                system=SYSTEM_PROMPT,
                tools=TOOLS,
                messages=messages
            )

            # Verifica se Claude quer usar ferramentas
            if resposta.stop_reason == "tool_use":
                # Processa cada tool call
                tool_results = []
                texto_parcial = ""

                for bloco in resposta.content:
                    if bloco.type == "text":
                        texto_parcial += bloco.text
                    elif bloco.type == "tool_use":
                        log.info(f"Tool: {bloco.name}({bloco.input})")
                        resultado = executar_tool(bloco.name, bloco.input)
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": bloco.id,
                            "content": resultado
                        })

                # Envia texto parcial se houver
                if texto_parcial.strip():
                    await update.message.reply_text(texto_parcial[:4000])

                # Adiciona resposta do assistente e resultados das tools
                messages.append({"role": "assistant", "content": resposta.content})
                messages.append({"role": "user", "content": tool_results})

            else:
                # Resposta final (sem tool use)
                texto_final = ""
                for bloco in resposta.content:
                    if bloco.type == "text":
                        texto_final += bloco.text

                if texto_final.strip():
                    historico[chat_id].append({"role": "assistant", "content": texto_final})
                    if len(texto_final) > 4000:
                        for i in range(0, len(texto_final), 4000):
                            await update.message.reply_text(texto_final[i:i+4000])
                    else:
                        await update.message.reply_text(texto_final)

                log.info(f"Respondido: {texto_final[:100]}...")
                break

    except Exception as e:
        log.error(f"Erro: {e}")
        await update.message.reply_text(f"Erro: {e}")


def main():
    log.info("Jarvis iniciando...")

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("posicoes", cmd_posicoes))
    app.add_handler(CommandHandler("saldo", cmd_saldo))
    app.add_handler(CommandHandler("racio", cmd_posicoes))
    app.add_handler(CommandHandler("fechar", cmd_fechar))
    app.add_handler(CommandHandler("fechar_tudo", cmd_fechar_tudo))
    app.add_handler(CommandHandler("dca", cmd_dca))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("cmd", cmd_executar))
    app.add_handler(CommandHandler("ler", cmd_ler))
    app.add_handler(CommandHandler("limpar", cmd_limpar))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, responder))

    log.info("Jarvis online — acesso total ativado.")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
