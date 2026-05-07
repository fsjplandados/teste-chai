@echo off
cls
echo ==========================================
echo    ETL Dashboard CRM - Streamlit
echo ==========================================
echo.
echo Escolha uma opcao:
echo.
echo [1] Atualizar Dados do Snowflake (Gerar Partes Parquet)
echo [2] Iniciar Dashboard Streamlit (Local)
echo.
set /p opcao="Digite a opcao (1 ou 2): "

if "%opcao%"=="1" goto etl
if "%opcao%"=="2" goto app

:etl
cls
echo Iniciando Extracao de Dados...
python etl_parquet.py
pause
goto fim

:app
cls
echo Iniciando Servidor Streamlit...
python -m streamlit run app.py
pause
goto fim

:fim
