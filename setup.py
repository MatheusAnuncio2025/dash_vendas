# matheusanuncio2025/dash_vendas/dash_vendas-6d3feb241c1a3fb70df9019d9f8c29be5f0ce6fd/setup.py
import sys
from cx_Freeze import setup, Executable

# ★★★ ATUALIZADO ★★★
# Lista de arquivos e pastas para incluir no executável
# DEIXAMOS APENAS O ARQUIVO DE CHAVE, QUE É FIXO.
include_files = [
    # ATUALIZADO com o nome da sua nova chave:
    ('projeto-dashvendas-9121a04b5446.json', 'projeto-dashvendas-9121a04b5446.json')
]

# Lista de pacotes que o cx_Freeze pode não encontrar sozinho
packages = [
    "pandas",
    "google.cloud.bigquery",
    "pandas_gbq",
    "requests",
    "openpyxl",
    "tabulate",
    "pyarrow",
    "db_dtypes"
]

# Configurações para o build
build_exe_options = {
    "packages": packages,
    "include_files": include_files,
    "excludes": [],
}

# Define o script principal e a base (console)
base = None
if sys.platform == "win32":
    base = "Console"

setup(
    name="ProcessadorDeVendas",
    version="1.0",
    description="Processador de relatórios de vendas.",
    options={"build_exe": build_exe_options},
    executables=[Executable("mainvendas.py", base=base, target_name="ProcessadorDeVendas.exe")]
)