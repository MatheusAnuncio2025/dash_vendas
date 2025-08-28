@echo OFF
echo.
echo [PASSO 1 de 2] Gerando o arquivo de especificacao (.spec)...

call .\venv\Scripts\activate.bat
pyinstaller --onefile --name="ProcessadorDeVendas" mainvendas.py

echo.
echo [PASSO 2 de 2] Arquivo 'ProcessadorDeVendas.spec' gerado!
echo.
echo >> IMPORTANTE: Agora, copie o conteudo que a Karen te enviou para dentro deste arquivo e rode o proximo comando.
echo.
pause