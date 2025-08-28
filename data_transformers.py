import pandas as pd
import decimal

# Importa as configurações globais
import config

def pre_processar_dataframe(df):
    """Aplica várias conversões de tipo de dados e regras de negócio ao DataFrame."""
    print("📆 Convertendo 'data_do_pedido' para o formato DATE...")
    df['data_do_pedido'] = pd.to_datetime(df['data_do_pedido'], dayfirst=True, errors='coerce').dt.date

    print("🔧 Convertendo 'numero_pedido_erp' para STRING...")
    df['numero_pedido_erp'] = df['numero_pedido_erp'].astype(str)

    if 'numero_pedido' in df.columns:
        df['numero_pedido'] = df['numero_pedido'].astype(str)

    print("💰 Formatando valores monetários para decimal.Decimal e arredondando para 3 casas...")
    for col in ['valor_total_produto', 'valor_unitario_venda']:
        if col in df.columns:
            df[col] = df[col].astype(str).replace(',', '.', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            df[col] = df[col].apply(lambda x: decimal.Decimal(str(round(x, 3))))

    print("🔢 Garantindo que 'quantidade' seja inteiro...")
    df['quantidade'] = pd.to_numeric(df['quantidade'], errors='coerce').fillna(0).astype(int)

    print("📦 Padronizando os nomes das lojas na coluna 'loja'...")
    df['loja'] = df['loja'].astype(str).replace(config.PADRONIZACAO_NOMES_LOJAS)

    print("🔄 Padronizando os valores da coluna 'status'...")
    if 'status' in df.columns:
        df['status'] = df['status'].astype(str).apply(
            lambda x: config.PADRONIZACAO_STATUS.get(x.strip(), x.strip())
        )
    else:
        print("⚠️ Coluna 'status' não encontrada. Criando com valores padrão.")
        df['status'] = ''

    print("🔄 Padronizando os valores da coluna 'tipo_logistica' com nova lógica hierárquica...")

    # Garante que as colunas de origem existam e sejam strings limpas
    if 'tipo_logistica' not in df.columns:
        df['tipo_logistica'] = ''
    # Salva o valor original para referência nas regras, pois a coluna 'tipo_logistica' será sobrescrita
    df['tipo_logistica_original'] = df['tipo_logistica'].astype(str).str.strip()
    # A coluna 'loja' já foi padronizada e convertida para string anteriormente no código

    # --- Início da nova lógica hierárquica ---
    # As regras são aplicadas da prioridade mais baixa para a mais alta.
    # Cada etapa subsequente pode sobrescrever a anterior.

    # Etapa 4 (Prioridade mais baixa): Valor padrão
    df['tipo_logistica'] = 'Outros'

    # Etapa 3: Mapeamentos de valores específicos
    df.loc[df['tipo_logistica_original'] == 'xd_drop_off', 'tipo_logistica'] = 'Coleta Outros'
    df.loc[df['tipo_logistica_original'] == 'drop_off', 'tipo_logistica'] = 'Correios Outros'

    # Etapa 2: Regras por loja (sobrescrevem a Etapa 3)
    df.loc[df['loja'].str.startswith('Mercado Livre', na=False), 'tipo_logistica'] = 'Mercado Envios'
    df.loc[df['loja'].str.startswith('Shopee', na=False), 'tipo_logistica'] = 'Shopee Xpress'
    df.loc[df['loja'].str.startswith('Amazon', na=False), 'tipo_logistica'] = 'Coleta Correios'

    # Etapa 1 (Prioridade mais alta): Regras para 'fulfillment' e 'self_service' (sobrescrevem todas as outras)
    df.loc[df['tipo_logistica_original'] == 'fulfillment', 'tipo_logistica'] = 'Fulfillment'
    df.loc[df['tipo_logistica_original'] == 'self_service', 'tipo_logistica'] = 'Envio Flex'

    # Remove a coluna temporária usada para a lógica
    df = df.drop(columns=['tipo_logistica_original'])
    print("✅ Nova lógica de 'tipo_logistica' aplicada.")

    print("🛠️ Aplicando regras específicas para 'Mercado Livre' e 'Amazon' e arredondando para 3 casas decimais...")
    mascara_mercado_livre = df['loja'].astype(str).str.startswith('Mercado Livre', na=False)

    df.loc[mascara_mercado_livre & (df['quantidade'] > 0), 'valor_total_produto'] = \
        (df.loc[mascara_mercado_livre & (df['quantidade'] > 0), 'valor_unitario_venda'] * \
         df.loc[mascara_mercado_livre & (df['quantidade'] > 0), 'quantidade']).apply(lambda x: decimal.Decimal(str(round(x, 3))))

    mascara_amazon = df['loja'].astype(str).str.startswith('Amazon', na=False)
    df.loc[mascara_amazon & (df['quantidade'] > 0), 'valor_total_produto'] = \
        (df.loc[mascara_amazon & (df['quantidade'] > 0), 'valor_unitario_venda'] * \
         df.loc[mascara_amazon & (df['quantidade'] > 0), 'quantidade']).apply(lambda x: decimal.Decimal(str(round(x, 3))))

    print("✅ Pré-processamento inicial do DataFrame concluído.")
    return df