import pandas as pd
import decimal
import time

# Importa as configurações globais
import config

# Importa as funções de cada módulo necessárias
import data_loaders
import pareto_analyzer

# --- Fluxo de Execução Principal para a Análise de Pareto ---
if __name__ == "__main__":
    try:
        # Autentica no Google Cloud Platform
        data_loaders.autenticar_gcp()

        # Carrega o DataFrame principal do arquivo Markdown local
        print(f"\n📥 Carregando dados do arquivo Markdown local: '{config.ARQUIVO_SAIDA_MD}' para análise de Pareto...")

        # Lê a tabela Markdown, pulando as linhas de cabeçalho e separação
        df_vendas_para_pareto = pd.read_csv(config.ARQUIVO_SAIDA_MD, sep='|', skiprows=[1, 2], skipinitialspace=True)

        # Limpa e remove colunas extras geradas pela leitura de Markdown
        df_vendas_para_pareto.columns = df_vendas_para_pareto.columns.str.strip()
        df_vendas_para_pareto = df_vendas_para_pareto.iloc[:, :-1]
        df_vendas_para_pareto = df_vendas_para_pareto.dropna(axis=1, how='all')

        # Converte colunas numéricas de volta para Decimal
        for col in ['valor_total_produto', 'custo_total_produto', 'cashback_cupom', 'Comissão', 'custo_unitario']:
            if col in df_vendas_para_pareto.columns:
                df_vendas_para_pareto[col] = df_vendas_para_pareto[col].astype(str).replace(',', '.', regex=False)
                df_vendas_para_pareto[col] = pd.to_numeric(df_vendas_para_pareto[col], errors='coerce').fillna(decimal.Decimal('0.000')).apply(decimal.Decimal)
            else:
                df_vendas_para_pareto[col] = decimal.Decimal('0.000')

        # Garante que 'quantidade' seja inteiro
        if 'quantidade' in df_vendas_para_pareto.columns:
            df_vendas_para_pareto['quantidade'] = pd.to_numeric(df_vendas_para_pareto['quantidade'], errors='coerce').fillna(0).astype(int)
        else:
            df_vendas_para_pareto['quantidade'] = 0

        # Garante que 'data_do_pedido' seja datetime
        if 'data_do_pedido' in df_vendas_para_pareto.columns:
            df_vendas_para_pareto['data_do_pedido'] = pd.to_datetime(df_vendas_para_pareto['data_do_pedido'], errors='coerce')
        else:
            df_vendas_para_pareto['data_do_pedido'] = pd.NaT

        # Converte colunas de texto para string e preenche NaNs
        for col_str in ['Estq', 'Categoria', 'Subcategoria', 'Fornecedores', 'hora_do_pedido']:
            if col_str in df_vendas_para_pareto.columns:
                df_vendas_para_pareto[col_str] = df_vendas_para_pareto[col_str].astype(str).str.strip().fillna('')
            else:
                df_vendas_para_pareto[col_str] = ''

        # Garante que 'Estq' é um inteiro
        if 'Estq' in df_vendas_para_pareto.columns:
            df_vendas_para_pareto['Estq'] = pd.to_numeric(df_vendas_para_pareto['Estq'], errors='coerce').fillna(0).astype(int)

        print(f"✅ Dados carregados do arquivo Markdown. Total de linhas: {len(df_vendas_para_pareto)}")

        # Executa a análise de Pareto por loja e por mês e faz upload para o BigQuery
        pareto_analyzer.analisar_pareto_por_loja(df_vendas_para_pareto.copy())

        print("\n✅ Análise de Pareto concluída com sucesso!")
        print("Programa finalizado.")

    except FileNotFoundError as e:
        print(f"❌ ERRO: Arquivo não encontrado: {e}. Verifique se '{config.ARQUIVO_SAIDA_MD}' existe e o caminho está correto.")
    except Exception as e:
        print(f"❌ ERRO inesperado durante o processamento de Pareto: {e}")
