@echo OFF
echo.
echo =================================================================
echo           SCRIPT PARA GERAR O EXECUTAVEL DO PROJETO
echo =================================================================
echo.

REM --- Passo 1: Criar e ativar o ambiente virtual ---
echo [PASSO 1 de 4] Verificando o ambiente virtual...
IF NOT EXIST venv (
    echo    -> Criando ambiente virtual 'venv'...
    python -m venv venv
    IF %ERRORLEVEL% NEQ 0 (
        echo.
        echo X ERRO: Falha ao criar o ambiente virtual. Verifique se o Python esta no PATH.
        pause
        exit /b
    )
)

echo    -> Ativando o ambiente virtual...
call .\venv\Scripts\activate.bat

REM --- Passo 2: Instalar dependencias ---
echo.
echo [PASSO 2 de 4] Instalando dependencias do projeto e o PyInstaller...
pip install -r requirements.txt
pip install pyinstaller

IF %ERRORLEVEL% NEQ 0 (
    echo.
    echo X ERRO: Falha ao instalar as dependencias. Verifique o arquivo requirements.txt e sua conexao.
    pause
    exit /b
)

REM --- Passo 3: Gerar o executavel com PyInstaller ---
echo.
echo [PASSO 3 de 4] Gerando o executavel... Isso pode demorar alguns minutos.
pyinstaller --onefile ^
    --name="ProcessadorDeVendas" ^
    --add-data "skilful-firefly-434016-b2-e08690ec5004.json;." ^
    --add-data "dados_bling;dados_bling" ^
    --add-data "Relatorio_vendas;Relatorio_vendas" ^
    --hidden-import="google.cloud.bigquery" ^
    --hidden-import="pandas._libs.tslibs.timedeltas" ^
    mainvendas.py

IF %ERRORLEVEL% NEQ 0 (
    echo.
    echo X ERRO: O PyInstaller falhou ao gerar o executavel. Verifique os logs acima.
    pause
    exit /b
)

REM --- Passo 4: Finalizacao ---
echo.
echo [PASSO 4 de 4] Limpando arquivos temporarios...
rmdir /S /Q build
del ProcessadorDeVendas.spec

echo.
echo =================================================================
echo      EXECUTAVEL GERADO COM SUCESSO!
echo      Voce pode encontra-lo em: .\dist\ProcessadorDeVendas.exe
echo =================================================================
echo.
pause