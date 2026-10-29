@echo off
rem Abre o Spark Query Studio.
rem Na primeira vez, quando o requirements.txt muda ou se a instalacao anterior falhou no meio,
rem (re)instala as dependencias em .venv antes de abrir.
cd /d "%~dp0"
set MARCA=.venv\.instalado
fc /b requirements.txt "%MARCA%" >nul 2>&1
if errorlevel 1 goto instalar
goto abrir

:instalar
echo Preparando o ambiente em .venv...
if not exist .venv\Scripts\python.exe (
  python -m venv .venv
  if errorlevel 1 goto erro_venv
)
.venv\Scripts\python -m pip install -r requirements.txt
if errorlevel 1 goto erro_pip
copy /y requirements.txt "%MARCA%" >nul

:abrir
.venv\Scripts\python -m streamlit run app.py --server.address localhost --browser.gatherUsageStats false
goto :eof

:erro_venv
echo Nao consegui criar o ambiente. Confira se o Python 3.10+ esta instalado.
pause
exit /b 1

:erro_pip
echo A instalacao das dependencias falhou (veja o erro acima). Rode de novo: ela recomeca de onde parou.
pause
exit /b 1
