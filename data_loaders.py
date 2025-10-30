import pandas as pd
import os
import decimal
from decimal import Decimal, getcontext, InvalidOperation
import requests
import io
import zipfile
import unicodedata
from datetime import datetime, timedelta
import time

# Importa as configurações globais
import config

# Configura o contexto decimal para arredondamento
getcontext().prec = 10
getcontext().rounding = decimal.ROUND_HALF_UP


def to_decimal_safe(value, precision='0.00'):
    """
    Converte um valor para Decimal de forma segura, tratando NaNs e erros de conversão.
    Quantiza para a precisão especificada.
    """
    if pd.isna(value) or str(value).strip() == '':
        return Decimal(precision)
    try:
        str_value = str(value).replace(',', '.')
        return Decimal(str_value).quantize(Decimal(precision), rounding=decimal.ROUND_HALF_UP)
    except InvalidOperation:
        return Decimal(precision)
    except Exception as e:
        print(
            f"⚠️ Aviso: Erro ao converter '{value}' para Decimal: {e}. Usando padrão {precision}.")
        return Decimal(precision)


def autenticar_gcp():
    """Autentica-se com o Google Cloud usando uma conta de serviço."""
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config.ARQUIVO_CONTA_SERVICO
    print("✅ Autenticação GCP configurada.")


def carregar_multiplos_excel_de_pasta(file_paths, colunas_para_ler, mapeamento_renomear_colunas):
    """
    Carrega e concatena múltiplos arquivos Excel de uma lista de caminhos.
    Aplica o renomeamento de colunas e seleciona as colunas necessárias.
    """
    all_dfs = []

    if not file_paths:
        print("⚠️ Nenhuma lista de arquivos Excel fornecida para carregar.")
        all_possible_cols_excel = list(mapeamento_renomear_colunas.values()) + \
            ['Estq', 'Categoria', 'Subcategoria', 'Fornecedores', 'custo_unitario', 'valor_total_produto',
                'valor_unitario_venda', 'cashback_cupom', 'Comissão', 'origem_dados', 'hora_do_pedido']
        all_possible_cols_excel = list(dict.fromkeys(all_possible_cols_excel))
        return pd.DataFrame(columns=all_possible_cols_excel)

    for filepath in file_paths:
        if not os.path.exists(filepath):
            print(
                f"❌ ERRO: Arquivo Excel não encontrado: '{filepath}'. Pulando este arquivo.")
            continue

        filename = os.path.basename(filepath)
        print(f"📖 Lendo arquivo: '{filename}'")
        try:
            df_temp = pd.read_excel(
                filepath, usecols=colunas_para_ler, header=0)
            df_temp = df_temp.rename(columns=mapeamento_renomear_colunas)

            expected_cols_after_excel_load = list(config.MAPEAMENTO_EXCEL_BIGQUERY.values()) + \
                ['Estq', 'Categoria', 'Subcategoria', 'Fornecedores',
                    'custo_unitario', 'hora_do_pedido']
            expected_cols_after_excel_load = list(
                dict.fromkeys(expected_cols_after_excel_load))

            for col in expected_cols_after_excel_load:
                if col not in df_temp.columns:
                    if col in ['valor_total_produto', 'valor_unitario_venda', 'custo_total_produto', 'custo_unitario', 'cashback_cupom', 'Comissão']:
                        df_temp[col] = Decimal('0.000')
                    elif col in ['quantidade', 'Estq']:
                        df_temp[col] = 0
                    elif col == 'hora_do_pedido':
                        df_temp[col] = ''
                    elif col in ['Fornecedores', 'Categoria', 'Subcategoria']:
                        df_temp[col] = pd.NA
                    else:
                        df_temp[col] = ''

            df_temp['origem_dados'] = 'Excel'
            all_dfs.append(df_temp)
        except Exception as e:
            print(
                f"❌ ERRO ao ler o arquivo '{filename}': {e}. Pulando este arquivo.")

    if not all_dfs:
        print(
            f"⚠️ Nenhum arquivo Excel válido encontrado ou processado na lista fornecida.")
        all_possible_cols_excel = list(mapeamento_renomear_colunas.values()) + \
            ['Estq', 'Categoria', 'Subcategoria', 'Fornecedores', 'custo_unitario', 'valor_total_produto',
                'valor_unitario_venda', 'cashback_cupom', 'Comissão', 'origem_dados', 'hora_do_pedido']
        all_possible_cols_excel = list(dict.fromkeys(all_possible_cols_excel))
        return pd.DataFrame(columns=all_possible_cols_excel)

    df_consolidated = pd.concat(all_dfs, ignore_index=True)
    print(f"✅ Todos os arquivos Excel da lista fornecida foram consolidados.")
    return df_consolidated


def carregar_vendas_magis5_api(api_url, api_key, page_size, mapping_cols, data_inicio=None, data_fim=None):
    """
    Carrega dados de vendas da API do Magis5 para um intervalo de datas específico.
    Se as datas não forem fornecidas, carrega para o dia de hoje e o dia anterior.
    Retorna um DataFrame do Pandas.
    """
    print(f"\n📥 Carregando dados de vendas da API do Magis5...")
    all_orders_data = []
    page = 1
    total_pages = None

    if data_inicio is None or data_fim is None:
        hoje = datetime.now().date()
        ontem = hoje - timedelta(days=1)
        data_inicio = ontem
        data_fim = hoje
        print(
            f"   Intervalo de datas não especificado. Buscando dados para {data_inicio.strftime('%d/%m/%Y')} e {data_fim.strftime('%d/%m/%Y')}.")
        start_of_period = datetime.combine(data_inicio, datetime.min.time())
        end_of_period = datetime.combine(data_fim, datetime.max.time())
    else:
        print(
            f"   Buscando dados para o intervalo de {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}.")
        start_of_period = datetime.combine(data_inicio, datetime.min.time())
        end_of_period = datetime.combine(data_fim, datetime.max.time())

    timestamp_from = int(start_of_period.timestamp())
    timestamp_to = int(end_of_period.timestamp())

    while True:
        url = (
            f"{api_url}?page={page}&limit={page_size}"
            f"&structureType=complete&timestampFrom={timestamp_from}&timestampTo={timestamp_to}"
        )
        headers = {"X-MAGIS5-APIKEY": api_key}

        print(f"🌐 Solicitando página {page} da API Magis5...")
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()

            json_data = response.json()
            orders = json_data.get('orders', [])

            if page == 1:
                total_records = json_data.get('total')
                if total_records is not None:
                    total_pages = (total_records + page_size - 1) // page_size
                    print(
                        f"ℹ️ Total de registros esperados: {total_records}. Total de páginas calculadas: {total_pages}.")
                else:
                    print(
                        "⚠️ Campo 'total' de registros não encontrado na resposta da API Magis5. Continuará a paginar até não haver mais pedidos.")

            if not orders or (total_pages is not None and page > total_pages):
                if not orders:
                    print(
                        f"ℹ️ Nenhuma ordem encontrada na página {page}. Encerrando paginação.")
                else:
                    print(
                        f"ℹ️ Página atual ({page}) excede o total de páginas ({total_pages}). Encerrando paginação.")
                break

            for order in orders:
                data_e_hora_bruta = order.get('dateCreated')
                dt_obj = datetime.fromisoformat(data_e_hora_bruta.replace(
                    'Z', '+00:00')) if data_e_hora_bruta else None
                data_do_pedido = dt_obj.date() if dt_obj else pd.NaT
                hora_do_pedido = dt_obj.strftime('%H:%M:%S') if dt_obj else ''

                for item in (order.get('order_items') or [{'item': {}}]):
                    all_orders_data.append({
                        'dateCreated': data_do_pedido,
                        'hora_do_pedido': hora_do_pedido,
                        'channelName': order.get('channelName'),
                        'sku_item': item.get('item', {}).get('seller_custom_field', 'N/A'),
                        'status': order.get('status'),
                        'unit_price': to_decimal_safe(item.get('unit_price', '0.00')),
                        'quantity': int(item.get('quantity', 0)),
                        'order_id': order.get('id'),
                        'origem_dados': 'Magis5',
                        'erpId': order.get('erpId', ''),
                        'packId': str(order.get('packId', '')) if order.get('packId') is not None else '',
                        'item_title': item.get('item', {}).get('title', 'N/A'),
                        'channel': order.get('channel', ''),
                        'shipping_number': order.get('shipping', {}).get('shipping_number', ''),
                        'logistic_type': order.get('shipping', {}).get('logistic_type', ''),
                        'cashback_cupom': Decimal('0.000'), 'Comissão': Decimal('0.000'),
                        'custo_unitario': Decimal('0.000'), 'Fornecedores': pd.NA, 'Estq': 0,
                        'Categoria': pd.NA, 'Subcategoria': pd.NA
                    })
            page += 1
            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            print(f"❌ ERRO na requisição à API Magis5: {e}")
            break

    if not all_orders_data:
        print("⚠️ Nenhum dado de vendas do Magis5 foi carregado.")
        return pd.DataFrame()

    df_magis5 = pd.DataFrame(all_orders_data)
    df_magis5 = df_magis5.rename(columns=mapping_cols)
    print(
        f"✅ Dados da API Magis5 carregados com sucesso. Total de linhas: {len(df_magis5)}")
    return df_magis5


def carregar_dados_bling_csv():
    """
    Carrega os dados de produtos a partir do CSV gerado pelo mainbling.py (que agora usa a planilha Google).
    Renomeia as colunas e pré-processa para o DataFrame principal.
    """
    print(
        f"\n🔗 Carregando dados de produtos do arquivo CSV: '{config.ARQUIVO_BLING_PRODUTOS_CSV}'...")

    expected_bling_cols_for_merge = ['sku', 'custo_unitario', 'Estq',
                                     'titulo_bling', 'Fornecedores', 'Categoria', 'Subcategoria', 'tipo_de_venda']

    if not os.path.exists(config.ARQUIVO_BLING_PRODUTOS_CSV):
        print(
            f"❌ ERRO: Arquivo CSV do Bling não encontrado em '{config.ARQUIVO_BLING_PRODUTOS_CSV}'.")
        print(
            "Certifique-se de que o script 'mainbling.py' foi executado e gerou o arquivo.")
        return pd.DataFrame(columns=expected_bling_cols_for_merge)

    try:
        df_bling = pd.read_csv(
            config.ARQUIVO_BLING_PRODUTOS_CSV,
            sep=';',
            encoding='utf-8',
            decimal=','
        )

        df_bling = df_bling.rename(columns={
            'Código': 'sku',
            'Quantidade': 'Estq',
            'Valor unitario': 'custo_unitario',
            'Produto': 'titulo_bling',
            'Fornecedor': 'Fornecedores',
            'Tipo de Venda': 'tipo_de_venda'
        })

        if 'sku' not in df_bling.columns:
            print("⚠️ Coluna 'sku' não encontrada no CSV após renomeio. Não será possível mesclar os dados.")
            return pd.DataFrame(columns=expected_bling_cols_for_merge)

        # ★★★ INÍCIO DA CORREÇÃO 2: LÓGICA DE AGREGAÇÃO PARA SKU DUPLICADO ★★★
        # 1. Garante que as colunas a serem agregadas sejam numéricas
        if 'custo_unitario' in df_bling.columns:
            df_bling['custo_unitario'] = pd.to_numeric(
                df_bling['custo_unitario'].astype(str).str.replace(',', '.'), errors='coerce'
            ).fillna(0)
        else:
            df_bling['custo_unitario'] = 0.0

        if 'Estq' in df_bling.columns:
            df_bling['Estq'] = pd.to_numeric(
                df_bling['Estq'], errors='coerce').fillna(0).astype(int)
        else:
            df_bling['Estq'] = 0

        # 2. Agrupa por SKU e aplica as agregações
        linhas_antes = len(df_bling)
        # Cria um dicionário de agregações dinâmico com base nas colunas existentes
        agg_dict = {
            'custo_unitario': 'max', # Pega o maior custo para o SKU
            'Estq': 'sum'            # Soma o estoque de todas as entradas do SKU
        }
        # Adiciona outras colunas para pegar o primeiro valor não nulo, se existirem
        for col in ['titulo_bling', 'Fornecedores', 'Categoria', 'Subcategoria', 'tipo_de_venda']:
            if col in df_bling.columns:
                agg_dict[col] = 'first'

        df_bling_agg = df_bling.groupby('sku').agg(agg_dict).reset_index()
        df_bling = df_bling_agg
        
        linhas_depois = len(df_bling)
        if linhas_antes > linhas_depois:
            print(f"   - Alerta: {linhas_antes - linhas_depois} SKUs duplicados foram agregados da planilha de produtos.")
        # ★★★ FIM DA CORREÇÃO 2 ★★★

        df_bling['sku'] = df_bling['sku'].astype(str).str.strip()

        # A conversão para Decimal é feita aqui, após a agregação
        df_bling['custo_unitario'] = df_bling['custo_unitario'].apply(
            lambda x: to_decimal_safe(x, '0.000'))

        if 'titulo_bling' not in df_bling.columns:
            df_bling['titulo_bling'] = ''
        else:
            df_bling['titulo_bling'] = df_bling['titulo_bling'].astype(
                str).str.strip().fillna('')

        for col_to_fill in ['Fornecedores', 'Categoria', 'Subcategoria', 'tipo_de_venda']:
            if col_to_fill not in df_bling.columns:
                df_bling[col_to_fill] = pd.NA
            else:
                df_bling[col_to_fill] = df_bling[col_to_fill].astype(
                    str).str.strip().fillna(pd.NA)

        print("✅ Dados de produtos do CSV carregados e pré-processados com sucesso.")
        return df_bling[expected_bling_cols_for_merge]

    except FileNotFoundError:
        print(
            f"❌ ERRO: O arquivo '{config.ARQUIVO_BLING_PRODUTOS_CSV}' não foi encontrado.")
    except Exception as e:
        print(f"❌ ERRO ao carregar dados do CSV: {e}")
    return pd.DataFrame(columns=expected_bling_cols_for_merge)