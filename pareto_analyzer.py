import pandas as pd
import decimal
from google.cloud import bigquery
import config
import unicodedata
import pandas_gbq

def _calcular_pareto(df, col_valor, prefixo_curva):
    """
    Função auxiliar para calcular o Pareto para uma dada coluna de valor.
    Calcula share, pareto acumulado e classifica em Curva A, B ou C.
    """
    df[col_valor] = pd.to_numeric(df[col_valor], errors='coerce').fillna(decimal.Decimal('0.000')).apply(decimal.Decimal)
    df_sorted = df.sort_values(by=col_valor, ascending=False).copy()

    total_valor = df_sorted[col_valor].sum()
    if total_valor == 0:
        df_sorted[f'share_{prefixo_curva}'] = decimal.Decimal('0.000')
    else:
        df_sorted[f'share_{prefixo_curva}'] = (df_sorted[col_valor] / total_valor * 100).apply(lambda x: decimal.Decimal(str(round(x, 3))))

    df_sorted[f'pareto_{prefixo_curva}'] = df_sorted[f'share_{prefixo_curva}'].cumsum().apply(lambda x: decimal.Decimal(str(round(x, 3))))

    def classificar_curva(pareto_acumulado):
        if pareto_acumulado <= decimal.Decimal('80.000'):
            return 'A'
        elif pareto_acumulado <= decimal.Decimal('95.000'):
            return 'B'
        else:
            return 'C'

    df_sorted[f'curva_{prefixo_curva}'] = df_sorted[f'pareto_{prefixo_curva}'].apply(classificar_curva)
    return df_sorted

def analisar_pareto_por_loja(df_vendas_original):
    """
    Realiza a análise de Pareto por loja e por mês para quantidade e GMV,
    e faz o upload dos resultados para o BigQuery em tabelas separadas POR LOJA.
    """
    print("\n📊 Iniciando análise de Pareto por loja e por mês...")

    required_cols = ['loja', 'sku', 'titulo', 'quantidade', 'valor_total_produto', 'data_do_pedido']
    for col in required_cols:
        if col not in df_vendas_original.columns:
            print(f"❌ ERRO: Coluna '{col}' não encontrada no DataFrame de vendas. Não é possível prosseguir com a análise de Pareto.")
            return

    df_vendas_original['data_do_pedido'] = pd.to_datetime(df_vendas_original['data_do_pedido'], errors='coerce')
    df_vendas_original['mes_referencia'] = df_vendas_original['data_do_pedido'].dt.strftime('%m/%B/%Y').str.lower().replace({
        'january': 'janeiro', 'february': 'fevereiro', 'march': 'marco', 'april': 'abril',
        'may': 'maio', 'june': 'junho', 'july': 'julho', 'august': 'agosto',
        'september': 'setembro', 'october': 'outubro', 'november': 'novembro', 'december': 'dezembro'
    }, regex=True)

    schema_for_pandas_gbq = [
        {
            "name": field.name,
            "type": field.field_type,
            "mode": field.mode,
            "description": field.description,
            "fields": field.fields
        }
        for field in config.ESQUEMA_BIGQUERY_PARETO
    ]

    consolidated_stores = {
        'Shopee': lambda df: df[df['loja'].astype(str).str.startswith('Shopee', na=False)],
        'Amazon': lambda df: df[df['loja'].astype(str).str.startswith('Amazon', na=False)],
        'Mercado Livre': lambda df: df[df['loja'].astype(str).str.startswith('Mercado Livre', na=False)],
        'PARETO_GERAL_MEGAJU': lambda df: df
    }

    lojas_a_processar = list(df_vendas_original['loja'].unique()) + list(consolidated_stores.keys())
    lojas_a_processar = list(dict.fromkeys(lojas_a_processar))

    for loja_nome_ou_filtro in lojas_a_processar:
        print(f"\n--- Processando análise de Pareto para: '{loja_nome_ou_filtro}' ---")

        df_para_analise = None
        if loja_nome_ou_filtro in consolidated_stores:
            df_para_analise = consolidated_stores[loja_nome_ou_filtro](df_vendas_original).copy()
        else:
            df_para_analise = df_vendas_original[df_vendas_original['loja'] == loja_nome_ou_filtro].copy()

        if df_para_analise.empty:
            print(f"⚠️ Nenhum dado encontrado para '{loja_nome_ou_filtro}'. Pulando análise.")
            continue

        df_todos_os_meses_pareto = []
        meses_unicos_para_analise = df_para_analise['mes_referencia'].unique()

        for mes in meses_unicos_para_analise:
            print(f"   Calculando Pareto para o mês: '{mes}' da análise '{loja_nome_ou_filtro}'")

            df_mes_atual = df_para_analise[df_para_analise['mes_referencia'] == mes].copy()
            df_mes_atual['titulo'] = df_mes_atual['titulo'].astype(str).fillna('Sem Título')
            df_mes_atual['quantidade'] = df_mes_atual['quantidade'].apply(lambda x: decimal.Decimal(str(x)))
            df_mes_atual['valor_total_produto'] = df_mes_atual['valor_total_produto'].apply(lambda x: decimal.Decimal(str(x)))

            df_consolidado_mensal_sku = df_mes_atual.groupby(['sku', 'titulo']).agg(
                qtde_vendas=('quantidade', 'sum'),
                gmv=('valor_total_produto', 'sum')
            ).reset_index()

            df_consolidado_mensal_sku['qtde_vendas'] = df_consolidado_mensal_sku['qtde_vendas'].apply(lambda x: decimal.Decimal(str(round(x, 3))))
            df_consolidado_mensal_sku['gmv'] = df_consolidado_mensal_sku['gmv'].apply(lambda x: decimal.Decimal(str(round(x, 3))))

            df_consolidado_mensal_sku = _calcular_pareto(df_consolidado_mensal_sku, 'qtde_vendas', 'qtde_vendas')
            df_consolidado_mensal_sku = _calcular_pareto(df_consolidado_mensal_sku, 'gmv', 'gmv')

            df_consolidado_mensal_sku = df_consolidado_mensal_sku.rename(columns={
                'qtde_vendas': 'quantidade_total_vendida',
                'gmv': 'valor_total_gmv',
                'share_qtde_vendas': 'share_quantidade_vendas',
                'pareto_qtde_vendas': 'pareto_quantidade_acumulada',
                'curva_qtde_vendas': 'curva_abc_quantidade',
                'share_gmv': 'share_gmv',
                'pareto_gmv': 'pareto_gmv_acumulado',
                'curva_gmv': 'curva_abc_gmv'
            })

            df_consolidado_mensal_sku['mes_referencia'] = mes

            final_cols_pareto = [
                'mes_referencia', 'sku', 'titulo', 'quantidade_total_vendida', 'share_quantidade_vendas',
                'pareto_quantidade_acumulada', 'curva_abc_quantidade',
                'valor_total_gmv', 'share_gmv', 'pareto_gmv_acumulado', 'curva_abc_gmv'
            ]
            df_consolidado_mensal_sku = df_consolidado_mensal_sku[final_cols_pareto]
            df_todos_os_meses_pareto.append(df_consolidado_mensal_sku)

        if not df_todos_os_meses_pareto:
            print(f"⚠️ Nenhum dado de Pareto gerado para '{loja_nome_ou_filtro}'. Pulando upload.")
            continue

        df_final_para_upload = pd.concat(df_todos_os_meses_pareto, ignore_index=True)

        # Normaliza e limpa strings
        df_final_para_upload['sku'] = df_final_para_upload['sku'].astype(str).apply(
            lambda x: unicodedata.normalize('NFKD', x).encode('ascii', errors='ignore').decode('utf-8').strip()
        ).fillna('')
        df_final_para_upload['titulo'] = df_final_para_upload['titulo'].astype(str).apply(
            lambda x: unicodedata.normalize('NFKD', x).encode('ascii', errors='ignore').decode('utf-8').strip()
        ).fillna('')

        # Garante que colunas numéricas estejam no formato correto para BigQuery
        numeric_cols_to_str = [
            field.name for field in config.ESQUEMA_BIGQUERY_PARETO
            if field.field_type == "NUMERIC"
        ]
        for col in numeric_cols_to_str:
            if col in df_final_para_upload.columns:
                df_final_para_upload[col] = df_final_para_upload[col].apply(lambda x: f"{x:.3f}" if pd.notna(x) else None)
                df_final_para_upload[col] = df_final_para_upload[col].astype(str).fillna('')

        integer_cols_in_schema = [
            field.name for field in config.ESQUEMA_BIGQUERY_PARETO
            if field.field_type == "INTEGER"
        ]
        for col in integer_cols_in_schema:
            if col in df_final_para_upload.columns:
                df_final_para_upload[col] = df_final_para_upload[col].apply(lambda x: int(x) if pd.notna(x) else None)
                df_final_para_upload[col] = df_final_para_upload[col].astype(pd.Int64Dtype())

        # Gera o nome simplificado da tabela no BigQuery
        nome_tabela_formatado = loja_nome_ou_filtro.lower().replace(' ', '_').replace('-', '_').replace('.', '').replace('/', '_').replace('__', '_')
        if nome_tabela_formatado.endswith('_'):
            nome_tabela_formatado = nome_tabela_formatado[:-1]
        if nome_tabela_formatado.startswith('_'):
            nome_tabela_formatado = nome_tabela_formatado[1:]

        id_tabela_pareto = f"{config.ID_TABELA_PARETO_PREFIXO}_{nome_tabela_formatado}"

        # Faz upload para o BigQuery
        print(f"🚀 Enviando análise de Pareto para BigQuery → Tabela: `{config.ID_PROJETO}.{config.ID_DATASET_PARETO}.{id_tabela_pareto}`...")
        try:
            pandas_gbq.to_gbq(
                df_final_para_upload,
                destination_table=f"{config.ID_DATASET_PARETO}.{id_tabela_pareto}",
                project_id=config.ID_PROJETO,
                if_exists='replace',
                table_schema=schema_for_pandas_gbq
            )
            print(f"✅ Upload da análise de Pareto para a tabela `{config.ID_PROJETO}.{config.ID_DATASET_PARETO}.{id_tabela_pareto}` finalizado!")
        except Exception as e:
            print(f"❌ ERRO ao fazer upload da análise de Pareto para '{loja_nome_ou_filtro}' para BigQuery: {e}")

    print("\n✅ Análise de Pareto concluída para todas as lojas/consolidações e uploads para BigQuery finalizados.")
