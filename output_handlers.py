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


# ★★★ FUNÇÃO RENOMEADA ★★★
def fazer_upload_completo_bigquery(df, id_projeto, id_dataset, id_tabela, esquema):
    """
    Faz upload do DataFrame para o BigQuery usando WRITE_TRUNCATE (substitui a tabela inteira).
    """
    print(f"\n🚀 [CARGA COMPLETA] Enviando para BigQuery → Tabela: `{id_projeto}.{id_dataset}.{id_tabela}`...")
    client = bigquery.Client(project=id_projeto)
    table_ref = client.dataset(id_dataset).table(id_tabela)

    job_config = bigquery.LoadJobConfig(
        schema=esquema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # Sobrescreve a tabela existente
    )

    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"✅ Upload COMPLETO finalizado! Total de linhas enviadas: {job.output_rows}")
    except Exception as e:
        print(f"❌ ERRO ao fazer upload completo (TRUNCATE) para BigQuery: {e}")

# ★★★ NOVA FUNÇÃO ★★★
def atualizar_mes_vigente_bigquery(df, id_projeto, id_dataset, id_tabela, esquema, mes_atual, ano_atual):
    """
    Atualiza o BigQuery (DELETE + APPEND) apenas para o mês e ano vigentes.
    """
    print(f"\n🔄 [ATUALIZAÇÃO MÊS] Enviando para BigQuery → Tabela: `{id_projeto}.{id_dataset}.{id_tabela}`...")
    client = bigquery.Client(project=id_projeto)
    table_ref = client.dataset(id_dataset).table(id_tabela)

    # 1. Executar o DELETE
    query_delete = f"""
    DELETE FROM `{id_projeto}.{id_dataset}.{id_tabela}`
    WHERE EXTRACT(MONTH FROM data_do_pedido) = {mes_atual}
      AND EXTRACT(YEAR FROM data_do_pedido) = {ano_atual}
    """
    
    print(f"   - Executando DELETE para Mês/Ano: {mes_atual}/{ano_atual}...")
    try:
        query_job = client.query(query_delete)
        query_job.result() # Espera o DELETE completar
        print(f"   - Registros do mês {mes_atual}/{ano_atual} deletados com sucesso.")
    except Exception as e:
        print(f"❌ ERRO ao executar o DELETE no BigQuery: {e}")
        print("   - O upload (APPEND) será abortado.")
        return # Aborta a função se o DELETE falhar

    # 2. Executar o APPEND
    job_config = bigquery.LoadJobConfig(
        schema=esquema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND, # Adiciona os novos dados
    )

    print(f"   - Iniciando APPEND dos dados do mês vigente ({len(df)} linhas)...")
    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"✅ Upload (APPEND) finalizado! Total de linhas enviadas: {job.output_rows}")
    except Exception as e:
        print(f"❌ ERRO ao fazer upload (APPEND) para BigQuery: {e}")