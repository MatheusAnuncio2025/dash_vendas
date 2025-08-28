import pandas as pd
from google.cloud import bigquery
import os
import decimal

# --- Configuração Geral ---
# Caminhos de Arquivo
# Caminho para o arquivo Markdown de saída principal do dashboard
ARQUIVO_SAIDA_MD = 'base_dash_relatorio_vendas.md'

# Caminho para o arquivo CSV de produtos do Bling
ARQUIVO_BLING_PRODUTOS_CSV = 'C:\\Users\\grupo\\OneDrive\\Documentos\\VS Code\\Relatorio_vendas\\dados_bling\\relatorio_bling_otimizado.csv'

# NOVO: Caminho para o arquivo Markdown com o histórico de vendas para projeção
ARQUIVO_HISTORICO_VENDAS_MD = 'C:\\Users\\grupo\\OneDrive\\Documentos\\VS Code\\Relatorio_vendas\\base_dash_relatorio_vendas.md'

# Lista de caminhos para os arquivos ZIP da Shopee
ARQUIVOS_SHOPEE_ZIP = [
    'C:\\Users\\grupo\\OneDrive\\Documents\\VS Code\\Relatorio_vendas\\Relatório Canais\\shopee_nanu.zip',
    'C:\\Users\\grupo\\OneDrive\\Documents\\VS Code\\Relatorio_vendas\\Relatório Canais\\shopee_mada.zip',
    'C:\\Users\\grupo\\OneDrive\\Documents\\VS Code\\Relatorio_vendas\\Relatório Canais\\shopee_daril.zip',
]

# Caminho para a pasta com múltiplos arquivos de relatório de vendas (Excel)
PASTA_RELATORIOS_VENDAS = 'C:\\Users\\grupo\\OneDrive\\Documentos\\VS Code\\Relatorio_vendas\\Relatorio_vendas\\'

# Detalhes do Projeto Google Cloud
ID_PROJETO = 'skilful-firefly-434016-b2'
ID_DATASET = 'relatorio_vendas'
ID_TABELA = 'base_dash_relatorio_vendas'
ARQUIVO_CONTA_SERVICO = "skilful-firefly-434016-b2-e08690ec5004.json"

# Detalhes para as tabelas de Pareto no BigQuery
ID_DATASET_PARETO = 'relatorio_pareto'
ID_TABELA_PARETO_PREFIXO = 'pareto'

# NOVO: Detalhes para a tabela de Projeção de Vendas no BigQuery
ID_DATASET_PROJECAO = 'relatorio_vendas' # Pode ser o mesmo dataset de vendas ou um novo
ID_TABELA_PROJECAO_VENDAS = 'projecoes_vendas_ia'

# Mapeamento de Colunas do Excel para o padrão do BigQuery
MAPEAMENTO_EXCEL_BIGQUERY = {
    'Número pedido': 'numero_pedido',
    'Número pedido ERP': 'numero_pedido_erp',
    'Número carrinho': 'numero_carrinho',
    'Data do pedido': 'data_do_pedido',
    'Loja': 'loja',
    'SKU': 'sku',
    'Valor total produto': 'valor_total_produto',
    'Valor unitário venda': 'valor_unitario_venda',
    'Quantidade': 'quantidade',
    'Título': 'titulo',
    'Id Canal Marketplace': 'id_canal_marketplace',
    'Rastreio': 'rastreio',
    'Status': 'status',
    'Tipo logística': 'tipo_logistica'
}

# Esquema do BigQuery para a tabela principal de vendas
ESQUEMA_BIGQUERY = [
    bigquery.SchemaField("numero_pedido", "STRING"),
    bigquery.SchemaField("numero_pedido_erp", "STRING"),
    bigquery.SchemaField("numero_carrinho", "STRING"),
    bigquery.SchemaField("data_do_pedido", "DATE"),
    bigquery.SchemaField("hora_do_pedido", "STRING"),
    bigquery.SchemaField("loja", "STRING"),
    bigquery.SchemaField("sku", "STRING"),
    bigquery.SchemaField("valor_total_produto", "NUMERIC"),
    bigquery.SchemaField("quantidade", "INTEGER"),
    bigquery.SchemaField("titulo", "STRING"),
    bigquery.SchemaField("id_canal_marketplace", "STRING"),
    bigquery.SchemaField("rastreio", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("tipo_logistica", "STRING"),
    bigquery.SchemaField("custo_unitario", "NUMERIC"),
    bigquery.SchemaField("cashback_cupom", "NUMERIC"),
    bigquery.SchemaField("Comissão", "NUMERIC"),
    bigquery.SchemaField("Fornecedores", "STRING"),
    bigquery.SchemaField("Estq", "INTEGER"),
    bigquery.SchemaField("Categoria", "STRING"),
    bigquery.SchemaField("Subcategoria", "STRING"),
    bigquery.SchemaField("tipo_de_venda", "STRING"), # NOVO: Campo adicionado ao esquema
]

# Esquema do BigQuery para as tabelas de Pareto
ESQUEMA_BIGQUERY_PARETO = [
    bigquery.SchemaField("mes_referencia", "STRING"),
    bigquery.SchemaField("sku", "STRING"),
    bigquery.SchemaField("titulo", "STRING"),
    bigquery.SchemaField("quantidade_total_vendida", "NUMERIC"),
    bigquery.SchemaField("share_quantidade_vendas", "NUMERIC"),
    bigquery.SchemaField("pareto_quantidade_acumulada", "NUMERIC"),
    bigquery.SchemaField("curva_abc_quantidade", "STRING"),
    bigquery.SchemaField("valor_total_gmv", "NUMERIC"),
    bigquery.SchemaField("share_gmv", "NUMERIC"),
    bigquery.SchemaField("pareto_gmv_acumulado", "NUMERIC"),
    bigquery.SchemaField("curva_abc_gmv", "STRING"),
]

# NOVO: Esquema do BigQuery para a tabela de Projeção de Vendas da IA
ESQUEMA_BIGQUERY_PROJECAO = [
    bigquery.SchemaField("mes_referencia", "STRING"),
    bigquery.SchemaField("loja", "STRING"),
    bigquery.SchemaField("gmv_projetado", "NUMERIC"),
    bigquery.SchemaField("quantidade_projetada", "INTEGER"),
    bigquery.SchemaField("justificativa", "STRING"),
    bigquery.SchemaField("data_projeção", "DATE"), # Data em que a projeção foi gerada
]

# Mapeamento para padronizar nomes de lojas
PADRONIZACAO_NOMES_LOJAS = {
    'Mercado Livre - MEGAJU': 'Mercado Livre - Megaju',
    'Mercado Livre - JULISHOP': 'Mercado Livre - Julishop',
    'Shopee - NANU SHOP': 'Shopee - Nanu Shop',
    'Shopee - Loja_da_mada': 'Shopee - Loja da Mada',
    'Mercado Livre -  LOJA_THITI': 'Mercado Livre - Loja Thiti',
    'Mercado Livre - LOJA_THITI': 'Mercado Livre - Loja Thiti',
    'Kabum - JULISHOP': 'Kabum - Julishop',
    'Magazine Luiza - Shop Midas': 'Magazine Luiza - Shop Midas',
    'Amazon - Daril Vendas': 'Amazon - Daril Vendas',
    'Amazon - LOJA DA MADA': 'Amazon - Loja Da Mada',
    'Shopify - Shopify': 'Shopify - Shopify',
    'Ali Express - NANU.SHOP': 'AliExpress - Nanu Shop',
    'TikTok - NANU SHOP': 'TikTok - Nanu Shop',
    'Amazon - MEGA STAR SHOPP': 'Amazon - Mega Star Shop',
    'Shopee - Daril Vendas': 'Shopee - Daril Vendas',
    'Amazon - NANU SHOP': 'Amazon - Nanu Shop',
    'Amazon - LOJA THITI': 'Amazon - Loja Thiti'
}

# --- Configurações da API Magis5 ---
MAGIS5_API_URL = "https://app.magis5.com.br/v1/orders"
MAGIS5_API_KEY = "da19e4e762304a518b0aefd3e4942a78"
MAGIS5_PAGE_SIZE = 50

# Mapeamento das colunas da API Magis5 para as colunas do DataFrame final
MAPEAMENTO_MAGIS5_BIGQUERY = {
    'dateCreated': 'data_do_pedido',
    'channelName': 'loja',
    'sku_item': 'sku',
    'status': 'status',
    'unit_price': 'valor_unitario_venda',
    'quantity': 'quantidade',
    'order_id': 'numero_pedido',
    'erpId': 'numero_pedido_erp',
    'packId': 'numero_carrinho',
    'item_title': 'titulo',
    'channel': 'id_canal_marketplace',
    'shipping_number': 'rastreio',
    'logistic_type': 'tipo_logistica',
}

# --- Caminhos de Saída de Dados Temporários ---
ARQUIVO_SAIDA_MAGIS5_PROCESSADO_EXCEL = "C:\\Users\\grupo\\OneDrive\\Documents\\VS Code\\Relatorio_vendas\\Dados_magis\\magis5_data.xlsx"

# --- Padronização de Status e Tipos de Logística ---
PADRONIZACAO_STATUS = {
    'awaiting_send': 'Ag. envio',
    'awaiting_logistic': 'Ag. logística',
    'approved': 'Aprovado',
    'canceled': 'Cancelado',
    'delivered': 'Entregue',
    'not_delivered': 'Não Entregue',
    'ready_to_print': 'Pronto para Impressão',
    'sent': 'Enviado',
    'awaiting_invoice': 'Ag. Nota Fiscal',
    'awaiting_approval': 'Ag. Aprovação',
    'billed': 'Faturado',
    'returned_logistic': 'Logística Devolvida',
    'integration_error': 'Erro Integração',
    'processing': 'Processando',
    'awaiting_payment': 'Ag. Pagamento',
    'canceled_resolved': 'Cancelado e resolvido',
}