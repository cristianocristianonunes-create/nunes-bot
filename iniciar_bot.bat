@echo off
REM Inicia Nunes Bot (nunes.py + auditor_continuo.py) em 2 janelas separadas.
REM Usado pelo Task Scheduler no boot do Windows.
REM Licao 15/04: reboot sem auto-start = 4h offline = -$200 em 1 posicao.

cd /d C:\robo-trade

REM Aguarda 30s pra rede estar pronta e outros servicos do Windows
timeout /t 30 /nobreak > NUL

REM Janela 1: bot principal
start "Nunes Bot" cmd /k "cd /d C:\robo-trade && python nunes.py"

REM Pausa 5s pra nao competir na inicializacao
timeout /t 5 /nobreak > NUL

REM Janela 2: auditor
start "Nunes Auditor" cmd /k "cd /d C:\robo-trade && python auditor_continuo.py"

exit
