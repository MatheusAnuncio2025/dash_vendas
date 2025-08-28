import pandas as pd
import decimal
import zipfile
import io

# Importa as configurações globais
import config

def processar_relatorio_shopee(caminhos_zip, df_vendas):
    """
    Processa arquivos de relatório da Shopee em formato ZIP (múltiplos), calcula novas colunas
    e mescla os dados com o DataFrame de vendas principal.
    """
    all_shopee_aggregated_dfs = []

    for caminho_zip in caminhos_zip:
        print(f"\n📦 Processando o arquivo ZIP da Shopee: '{caminho_zip}'...")
        shopee_df_list = []

        try:
            with zipfile.ZipFile(caminho_zip, 'r') as zf:
                xlsx_files = [f for f in zf.namelist() if f.endswith('.xlsx') and not f.startswith('__MACOSX/')]

                if not xlsx_files:
                    print(f"⚠️ Nenhum arquivo .xlsx encontrado no ZIP '{caminho_zip}'. Pulando este arquivo.")
                    continue

                for xlsx_file in xlsx_files:
                    print(f"📖 Lendo arquivo '{xlsx_file}' dentro do ZIP '{caminho_zip}'...")
                    with zf.open(xlsx_file) as f:
                        df_shopee_temp = pd.read_excel(f)

                        shopee_col_map = {
                            'ID do pedido': 'id_pedido_shopee',
                            'Compensar Moedas Shopee': 'compensar_moedas_shopee_raw',
                            'Cupom Shopee': 'cupom_shopee',
                            'Taxa de comissão': 'taxa_comissao',
                            'Taxa de serviço': 'taxa_servico',
                            'Preço acordado': 'preco_acordado'
                        }
                        df_shopee_temp = df_shopee_temp.rename(columns=shopee_col_map)

                        if 'id_pedido_shopee' in df_shopee_temp.columns:
                            df_shopee_temp['id_pedido_shopee'] = df_shopee_temp['id_pedido_shopee'].astype(str).str.strip()
                        else:
                            print(f"⚠️ Coluna 'ID do pedido' não encontrada no arquivo '{xlsx_file}'. Pulando este arquivo.")
                            continue

                        numeric_cols_shopee = [
                            'compensar_moedas_shopee_raw', 'cupom_shopee',
                            'taxa_comissao', 'taxa_servico', 'preco_acordado'
                        ]

                        for col in numeric_cols_shopee:
                            if col in df_shopee_temp.columns:
                                df_shopee_temp[col] = pd.to_numeric(df_shopee_temp[col], errors='coerce').fillna(decimal.Decimal('0.000')).apply(decimal.Decimal)
                            else:
                                print(f"⚠️ Coluna '{col}' não encontrada no arquivo '{xlsx_file}'. Inicializando com 0.")
                                df_shopee_temp[col] = decimal.Decimal('0.000')

                        if 'compensar_moedas_shopee_raw' in df_shopee_temp.columns:
                            df_shopee_temp['compensar_moedas_shopee_div100'] = df_shopee_temp['compensar_moedas_shopee_raw'] / decimal.Decimal('100')
                            df_shopee_temp['compensar_moedas_shopee_div100'] = df_shopee_temp['compensar_moedas_shopee_div100'].apply(lambda x: decimal.Decimal(str(round(x, 3))))
                        else:
                            df_shopee_temp['compensar_moedas_shopee_div100'] = decimal.Decimal('0.000')

                        df_shopee_temp['cashback_cupom_item'] = df_shopee_temp['compensar_moedas_shopee_div100'] + df_shopee_temp['cupom_shopee']
                        df_shopee_temp['Comissão_item'] = df_shopee_temp['taxa_comissao'] + df_shopee_temp['taxa_servico']

                        df_shopee_aggregated_single_zip = df_shopee_temp.groupby('id_pedido_shopee').agg(
                            cashback_cupom=('cashback_cupom_item', 'sum'),
                            Comissão=('Comissão_item', 'sum'),
                            preco_acordado=('preco_acordado', 'first')
                        ).reset_index()

                        df_shopee_aggregated_single_zip['cashback_cupom'] = df_shopee_aggregated_single_zip['cashback_cupom'].apply(lambda x: decimal.Decimal(str(round(x, 3))))
                        df_shopee_aggregated_single_zip['Comissão'] = df_shopee_aggregated_single_zip['Comissão'].apply(lambda x: decimal.Decimal(str(round(x, 3))))
                        df_shopee_aggregated_single_zip['preco_acordado'] = df_shopee_aggregated_single_zip['preco_acordado'].apply(lambda x: decimal.Decimal(str(round(x, 3))))

                        shopee_df_list.append(df_shopee_aggregated_single_zip[['id_pedido_shopee', 'preco_acordado', 'cashback_cupom', 'Comissão']])

            if shopee_df_list:
                all_shopee_aggregated_dfs.extend(shopee_df_list)
            else:
                print(f"❌ Nenhum dado válido consolidado do ZIP '{caminho_zip}'.")

        except FileNotFoundError:
            print(f"❌ ERRO: Arquivo ZIP da Shopee '{caminho_zip}' não encontrado. Pulando este arquivo.")
            continue
        except zipfile.BadZipFile:
            print(f"❌ ERRO: O arquivo '{caminho_zip}' não é um arquivo ZIP válido ou está corrompido. Pulando este arquivo.")
            continue
        except Exception as e:
            print(f"❌ ERRO inesperado ao processar o ZIP '{caminho_zip}': {e}")
            continue

    if not all_shopee_aggregated_dfs:
        print("⚠️ Nenhum dado da Shopee foi processado com sucesso de todos os ZIPs. Retornando DataFrame original.")
        df_vendas['valor_total_produto'] = (df_vendas['valor_unitario_venda'] * df_vendas['quantidade']).apply(lambda x: decimal.Decimal(str(round(x, 3))))
        return df_vendas

    df_shopee_combined_final = pd.concat(all_shopee_aggregated_dfs, ignore_index=True)
    df_shopee_combined_final = df_shopee_combined_final.drop_duplicates(subset=['id_pedido_shopee'], keep='last')

    print("\n🔄 Mesclando dados consolidados da Shopee com o relatório de vendas principal...")
    df_vendas_merged = pd.merge(
        df_vendas,
        df_shopee_combined_final,
        left_on='numero_pedido',
        right_on='id_pedido_shopee',
        how='left',
        suffixes=('_orig', '_shopee')
    )

    df_vendas_merged['cashback_cupom'] = df_vendas_merged['cashback_cupom_shopee'].fillna(df_vendas_merged['cashback_cupom_orig'])
    df_vendas_merged['Comissão'] = df_vendas_merged['Comissão_shopee'].fillna(df_vendas_merged['Comissão_orig'])

    mask_shopee_match = df_vendas_merged['id_pedido_shopee'].notna()
    df_vendas_merged.loc[mask_shopee_match, 'valor_total_produto'] = df_vendas_merged.loc[mask_shopee_match, 'preco_acordado']

    mask_no_shopee_match = ~mask_shopee_match
    df_vendas_merged.loc[mask_no_shopee_match, 'valor_total_produto'] = (
        df_vendas_merged.loc[mask_no_shopee_match, 'valor_unitario_venda'] * df_vendas_merged.loc[mask_no_shopee_match, 'quantidade']
    ).apply(lambda x: decimal.Decimal(str(round(x, 3))))
    print("✅ 'valor_total_produto' calculado para pedidos sem correspondência Shopee.")

    df_vendas_merged = df_vendas_merged.drop(columns=[
        'id_pedido_shopee', 'preco_acordado',
        'cashback_cupom_orig', 'cashback_cupom_shopee',
        'Comissão_orig', 'Comissão_shopee'
    ], errors='ignore')

    print("✅ Dados da Shopee processados e mesclados com sucesso.")
    return df_vendas_merged
