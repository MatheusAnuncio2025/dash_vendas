import pandas as pd
import decimal
import os
from datetime import datetime, timedelta
import sys
import time

import config
import data_loaders
import data_transformers
import shopee_processor
import output_handlers
import pareto_analyzer
from dados_bling import mainbling

def add_missing_columns(df, all_cols_schema):
    """
    Adds missing columns to a DataFrame based on a schema with appropriate default values.
    """
    if df.empty:
        return pd.DataFrame(columns=all_cols_schema)

    for col in all_cols_schema:
        if col not in df.columns:
            if col in ['valor_total_produto', 'valor_unitario_venda', 'custo_total_produto', 'custo_unitario', 'cashback_cupom', 'Comissão']:
                df[col] = decimal.Decimal('0.000')
            elif col in ['quantidade', 'Estq']:
                df[col] = 0
            elif col == 'hora_do_pedido':
                df[col] = ''
            elif col in ['Fornecedores', 'Categoria', 'Subcategoria', 'tipo_de_venda']:
                df[col] = pd.NA
            else:
                df[col] = ''
    return df[all_cols_schema]


if __name__ == "__main__":
    try:
        # 0. Initial Setup and Authentication
        data_loaders.autenticar_gcp()
        print("Iniciando o processo de carga e transformação de dados de vendas...")

        # Garante que as pastas de saída existam
        os.makedirs(config.PASTA_RELATORIOS_VENDAS, exist_ok=True)
        if os.path.dirname(config.ARQUIVO_BLING_PRODUTOS_CSV):
             os.makedirs(os.path.dirname(config.ARQUIVO_BLING_PRODUTOS_CSV), exist_ok=True)
        for zip_path in config.ARQUIVOS_SHOPEE_ZIP:
            if os.path.dirname(zip_path):
                os.makedirs(os.path.dirname(zip_path), exist_ok=True)


        # Call Bling data update process
        print("\n--- Iniciando atualização dos dados do bling ---")
        mainbling.main()
        print("--- Processo atualização do bling concluído ---")

        # Determine current month and year
        now = datetime.now()
        hoje = now.date()
        current_month_num = now.month
        current_year = now.year
        month_name_map_pt = {
            1: 'janeiro', 2: 'fevereiro', 3: 'marco', 4: 'abril',
            5: 'maio', 6: 'junho', 7: 'julho', 8: 'agosto',
            9: 'setembro', 10: 'outubro', 11: 'novembro', 12: 'dezembro'
        }
        current_month_name_pt = month_name_map_pt.get(current_month_num, '').lower()

        all_possible_cols = list(dict.fromkeys(
            list(config.MAPEAMENTO_EXCEL_BIGQUERY.values()) +
            list(config.MAPEAMENTO_MAGIS5_BIGQUERY.values()) +
            ['Estq', 'Categoria', 'Subcategoria', 'Fornecedores', 'custo_unitario', 
             'cashback_cupom', 'Comissão', 'origem_dados', 'hora_do_pedido', 'tipo_de_venda']
        ))

        # --- LÓGICA DE CARREGAMENTO DE DADOS ---

        # 1. Carrega dados de meses anteriores (sempre do Excel)
        print(f"\n📥 Verificando arquivos Excel na pasta '{config.PASTA_RELATORIOS_VENDAS}' para meses anteriores...")
        excel_files_to_load = []
        if os.path.exists(config.PASTA_RELATORIOS_VENDAS):
            for filename in os.listdir(config.PASTA_RELATORIOS_VENDAS):
                if filename.endswith('.xlsx') or filename.endswith('.xls'):
                    file_base_name = os.path.splitext(filename)[0].lower()
                    if file_base_name != current_month_name_pt and not file_base_name.endswith('_processado'):
                        excel_files_to_load.append(os.path.join(config.PASTA_RELATORIOS_VENDAS, filename))

        df_vendas_excel_prev_months = pd.DataFrame()
        if excel_files_to_load:
            df_vendas_excel_prev_months = data_loaders.carregar_multiplos_excel_de_pasta(
                excel_files_to_load,
                list(config.MAPEAMENTO_EXCEL_BIGQUERY.keys()),
                config.MAPEAMENTO_EXCEL_BIGQUERY
            )
            df_vendas_excel_prev_months = add_missing_columns(df_vendas_excel_prev_months, all_possible_cols)
            print(f"✅ Arquivos Excel de meses anteriores consolidados. Total de linhas: {len(df_vendas_excel_prev_months)}")
        else:
            print("\n⚠️ Nenhum arquivo Excel de meses anteriores encontrado para carregar.")

        # 2. Carrega dados do mês vigente (Excel + API)
        print(f"\n🔄 Processando dados para o mês vigente ({current_month_name_pt.capitalize()}/{current_year})...")
        dfs_mes_vigente = []
        api_data_inicio = None
        api_data_fim = hoje

        current_month_excel_filepath = os.path.join(config.PASTA_RELATORIOS_VENDAS, f"{current_month_name_pt}.xlsx")

        if os.path.exists(current_month_excel_filepath):
            print(f"   - Arquivo Excel '{current_month_name_pt}.xlsx' encontrado. Carregando dados...")
            df_excel_mes_vigente = data_loaders.carregar_multiplos_excel_de_pasta(
                [current_month_excel_filepath],
                list(config.MAPEAMENTO_EXCEL_BIGQUERY.keys()),
                config.MAPEAMENTO_EXCEL_BIGQUERY
            )
            
            ontem = hoje - timedelta(days=1)
            api_data_inicio = ontem
            print(f"   - O Excel foi carregado. A API buscará os dados a partir de ontem ({ontem.strftime('%d/%m/%Y')}).")

            if not df_excel_mes_vigente.empty:
                linhas_originais = len(df_excel_mes_vigente)
                
                # ★★★ INÍCIO DA ALTERAÇÃO (TRATAMENTO PRÉVIO) ★★★
                # 1. Garante que a coluna é do tipo string para evitar erros de conversão
                df_excel_mes_vigente['data_do_pedido'] = df_excel_mes_vigente['data_do_pedido'].astype(str)
                # 2. Converte para data usando o formato EXATO do Excel e extrai apenas a data (sem a hora)
                df_excel_mes_vigente['data_do_pedido'] = pd.to_datetime(
                    df_excel_mes_vigente['data_do_pedido'], 
                    format='%d/%m/%Y %H:%M:%S', 
                    errors='coerce'
                ).dt.date
                # ★★★ FIM DA ALTERAÇÃO ★★★

                # 3. Aplica o filtro original
                df_excel_mes_vigente = df_excel_mes_vigente[df_excel_mes_vigente['data_do_pedido'] < api_data_inicio]
                
                linhas_filtradas = len(df_excel_mes_vigente)
                print(f"   - Filtrando Excel: Removidas {linhas_originais - linhas_filtradas} linhas de dias que serão atualizados pela API.")

            df_excel_mes_vigente = add_missing_columns(df_excel_mes_vigente, all_possible_cols)
            dfs_mes_vigente.append(df_excel_mes_vigente)

        else:
            print(f"   - Arquivo Excel '{current_month_name_pt}.xlsx' NÃO encontrado.")
            api_data_inicio = hoje.replace(day=1)
            print(f"   - A API buscará os dados do início do mês até hoje ({api_data_inicio.strftime('%d/%m/%Y')} a {api_data_fim.strftime('%d/%m/%Y')}).")

        df_vendas_api = data_loaders.carregar_vendas_magis5_api(
            config.MAGIS5_API_URL, config.MAGIS5_API_KEY, config.MAGIS5_PAGE_SIZE,
            config.MAPEAMENTO_MAGIS5_BIGQUERY,
            data_inicio=api_data_inicio,
            data_fim=api_data_fim
        )
        if not df_vendas_api.empty:
            df_vendas_api = add_missing_columns(df_vendas_api, all_possible_cols)
            dfs_mes_vigente.append(df_vendas_api)

        df_vendas_current_month_source = pd.DataFrame()
        if dfs_mes_vigente:
            df_vendas_current_month_source = pd.concat(dfs_mes_vigente, ignore_index=True)
            print(f"✅ Dados do mês vigente (Excel + API) consolidados. Total de linhas: {len(df_vendas_current_month_source)}")
        else:
            print("⚠️ Nenhum dado do mês vigente (Excel ou API) foi carregado.")

        # 3. Combinação final dos DataFrames
        print("\n🔄 Combinando DataFrames de vendas de todos os períodos...")
        if not df_vendas_excel_prev_months.empty or not df_vendas_current_month_source.empty:
            df_vendas = pd.concat([df_vendas_excel_prev_months, df_vendas_current_month_source], ignore_index=True)
            print(f"✅ DataFrames de todos os períodos combinados. Total de linhas final: {len(df_vendas)}")
        else:
            print("❌ Nenhum dado de vendas (Excel ou Magis5) foi carregado. Encerrando o script.")
            time.sleep(20)
            sys.exit(1)

        # --- FIM DA LÓGICA DE CARREGAMENTO ---
        
        df_vendas = data_transformers.pre_processar_dataframe(df_vendas)

        print("\n🔗 Carregando e mesclando dados de custo, estoque, fornecedores e categorias do Bling...")
        df_bling_data = data_loaders.carregar_dados_bling_csv()

        df_vendas = pd.merge(
            df_vendas,
            df_bling_data[['sku', 'custo_unitario', 'Estq', 'titulo_bling', 'Fornecedores', 'Categoria', 'Subcategoria', 'tipo_de_venda']],
            on='sku',
            how='left',
            suffixes=('_orig', '_bling')
        )
        
        for col_name in ['custo_unitario', 'Estq', 'titulo', 'Fornecedores', 'Categoria', 'Subcategoria', 'tipo_de_venda']:
            bling_col = f"{col_name}_bling" if col_name != 'titulo' else 'titulo_bling'
            orig_col = f"{col_name}_orig" if col_name != 'titulo' else 'titulo'

            if col_name == 'custo_unitario':
                df_vendas['custo_unitario'] = df_vendas[bling_col].combine_first(df_vendas[orig_col]).apply(data_loaders.to_decimal_safe)
            elif col_name == 'Estq':
                df_vendas['Estq'] = df_vendas[bling_col].combine_first(df_vendas[orig_col])
                df_vendas['Estq'] = pd.to_numeric(df_vendas['Estq'], errors='coerce').fillna(0).astype(int)
            elif col_name == 'titulo':
                 df_vendas['titulo'] = df_vendas['titulo'].fillna(df_vendas[bling_col])
            else:
                df_vendas[col_name] = df_vendas[bling_col].combine_first(df_vendas[orig_col] if orig_col in df_vendas.columns else pd.Series(dtype=object))
                df_vendas[col_name] = df_vendas[col_name].astype(str).str.strip().fillna('')

        cols_to_drop_after_bling_merge = [col for col in df_vendas.columns if col.endswith(('_orig', '_bling'))]
        df_vendas = df_vendas.drop(columns=cols_to_drop_after_bling_merge, errors='ignore')

        print("✅ Dados do Bling mesclados com sucesso.")

        if not df_vendas_current_month_source.empty:
            df_current_month_processed = df_vendas[
                (pd.to_datetime(df_vendas['data_do_pedido']).dt.month == current_month_num) &
                (pd.to_datetime(df_vendas['data_do_pedido']).dt.year == current_year)
            ].copy()
            df_current_month_processed = df_current_month_processed.drop(columns=['origem_dados'], errors='ignore')
            try:
                output_path_current_month_excel = os.path.join(config.PASTA_RELATORIOS_VENDAS, f"{current_month_name_pt}_PROCESSADO.xlsx")
                df_current_month_processed.to_excel(output_path_current_month_excel, index=False)
                print(f"✅ Dados processados do mês vigente exportados para '{output_path_current_month_excel}'.")
            except Exception as e:
                print(f"❌ ERRO ao exportar dados processados do mês vigente para Excel: {e}")
        else:
            print("⚠️ Nenhum dado do mês vigente processado para exportar para Excel.")

        if 'origem_dados' in df_vendas.columns:
            df_vendas = df_vendas.drop(columns=['origem_dados'])

        print("📦 Processando relatórios da Shopee...")
        df_vendas = shopee_processor.processar_relatorio_shopee(
            config.ARQUIVOS_SHOPEE_ZIP,
            df_vendas
        )

        print("🛠️ Reforçando tipos e preenchimento para colunas finais...")
        if 'Estq' in df_vendas.columns:
            df_vendas['Estq'] = pd.to_numeric(df_vendas['Estq'], errors='coerce').fillna(0).astype(int)
        else:
            df_vendas['Estq'] = 0

        for col_str in ['Categoria', 'Subcategoria', 'Fornecedores', 'tipo_de_venda']:
            if col_str in df_vendas.columns:
                df_vendas[col_str] = df_vendas[col_str].astype(str).str.strip().fillna('')
            else:
                df_vendas[col_str] = ''

        if df_vendas.columns.duplicated().any():
            print("⚠️ Aviso: Colunas duplicadas encontradas. Removendo duplicatas.")
            df_vendas = df_vendas.loc[:,~df_vendas.columns.duplicated(keep='last')]

        print("🛠️ Ajustando tipos de dados para o upload no BigQuery...")
        monetary_cols_to_convert = ['valor_total_produto', 'cashback_cupom', 'Comissão', 'custo_unitario']
        for col in monetary_cols_to_convert:
            if col in df_vendas.columns:
                df_vendas[col] = df_vendas[col].apply(data_loaders.to_decimal_safe)
            else:
                df_vendas[col] = decimal.Decimal('0.000')

        if 'data_do_pedido' in df_vendas.columns:
            df_vendas['data_do_pedido'] = pd.to_datetime(df_vendas['data_do_pedido'], errors='coerce').dt.date
        else:
            df_vendas['data_do_pedido'] = pd.NaT

        print("📝 Gerando arquivo Markdown para o dashboard...")
        output_handlers.gerar_saida_markdown(df_vendas, config.ARQUIVO_SAIDA_MD)

        print("⬆️ Preparando para upload no BigQuery...")
        colunas_finais_bigquery = [field.name for field in config.ESQUEMA_BIGQUERY]

        if 'custo_total_produto' in df_vendas.columns:
            df_vendas = df_vendas.drop(columns=['custo_total_produto'], errors='ignore')

        for col_schema in colunas_finais_bigquery:
            if col_schema not in df_vendas.columns:
                schema_field = next((field for field in config.ESQUEMA_BIGQUERY if field.name == col_schema), None)
                if schema_field:
                    if schema_field.field_type == "NUMERIC":
                        df_vendas[col_schema] = decimal.Decimal('0.000')
                    elif schema_field.field_type == "INTEGER":
                        df_vendas[col_schema] = 0
                    elif schema_field.field_type == "DATE":
                        df_vendas[col_schema] = pd.NaT
                    else:
                        df_vendas[col_schema] = ''
        
        df_vendas = df_vendas[colunas_finais_bigquery]

        output_handlers.fazer_upload_bigquery(
            df_vendas,
            config.ID_PROJETO,
            config.ID_DATASET,
            config.ID_TABELA,
            config.ESQUEMA_BIGQUERY
        )

        print("\n📊 Executando a Análise de Pareto em memória...")
        pareto_analyzer.analisar_pareto_por_loja(df_vendas.copy())


        print("\n✅ Processamento completo concluído com sucesso!")
        print("Programa finalizado. A janela fechará em 20 segundos...")
        time.sleep(20)

    except FileNotFoundError as e:
        print(f"❌ ERRO: Arquivo não encontrado: {e}. Verifique se a pasta '{config.PASTA_RELATORIOS_VENDAS}' e os arquivos ZIP necessários existem.")
        time.sleep(20)
        sys.exit(1)
    except Exception as e:
        print(f"❌ ERRO inesperado durante o processamento principal: {e}")
        time.sleep(20)
        sys.exit(1)