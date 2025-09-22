import sys
from cx_Freeze import setup, Executable

# Lista de arquivos e pastas para incluir no executável
# DEIXAMOS APENAS O ARQUIVO DE CHAVE, QUE É FIXO.
include_files = [
    ('skilful-firefly-434016-b2-364eae284f30.json', 'skilful-firefly-434016-b2-364eae284f30.json')
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