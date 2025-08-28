import pandas as pd
from google.cloud import bigquery
import decimal

# Importa as configurações do arquivo config.py
import config

def gerar_saida_markdown(df, arquivo_saida):
    """
    Gera uma tabela Markdown a partir do DataFrame.
    Aplica limpeza em colunas de texto para evitar erros de tokenização no Markdown.
    """
    df_display_md = df.copy()

    # Limpa quebras de linha e caracteres '|' de todas as colunas de string
    for col in df_display_md.select_dtypes(include=['object', 'string']).columns:
        df_display_md[col] = df_display_md[col].astype(str).apply(
            lambda x: x.replace('\n', ' ').replace('\r', ' ').replace('|', '-')
        )

    # Formata colunas numéricas para 3 casas decimais
    for col in ['valor_total_produto', 'custo_total_produto', 'cashback_cupom', 'Comissão', 'custo_unitario']:
        if col in df_display_md.columns:
            df_display_md[col] = df_display_md[col].astype(float).map('{:.3f}'.format)

    if 'valor_unitario_venda' in df_display_md.columns:
        df_display_md['valor_unitario_venda'] = df_display_md['valor_unitario_venda'].astype(float).map('{:.3f}'.format)

    tabela_markdown = df_display_md.to_markdown(index=False)
    with open(arquivo_saida, 'w', encoding='utf-8') as f:
        f.write(tabela_markdown)
    print(f"✅ Arquivo Markdown '{arquivo_saida}' gerado com sucesso.")

def fazer_upload_bigquery(df, id_projeto, id_dataset, id_tabela, esquema):
    """Faz upload do DataFrame para o BigQuery."""
    print(f"\n🚀 Enviando para BigQuery → Tabela: `{id_projeto}.{id_dataset}.{id_tabela}`...")
    client = bigquery.Client(project=id_projeto)
    table_ref = client.dataset(id_dataset).table(id_tabela)

    job_config = bigquery.LoadJobConfig(
        schema=esquema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # Sobrescreve a tabela existente
    )

    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"✅ Upload finalizado! Total de linhas enviadas: {job.output_rows}")
    except Exception as e:
        print(f"❌ ERRO ao fazer upload para BigQuery: {e}")
