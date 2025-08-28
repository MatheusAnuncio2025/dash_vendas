import requests
import json
from datetime import datetime, timedelta
import config # Importa as configurações globais

def debug_magis5_api_data(api_url, api_key, page_size=10):
    """
    Busca uma amostra de pedidos da API do Magis5 e imprime os valores brutos
    dos campos relevantes para depuração.
    """
    print(f"\n--- Iniciando Depuração da API Magis5 ---")
    print(f"Buscando {page_size} pedidos da API Magis5 para o mês vigente...")

    # Calcula os timestamps para o mês vigente
    now = datetime.now()
    start_of_month = datetime(now.year, now.month, 1)
    if now.month == 12:
        end_of_month_dt = datetime(now.year + 1, 1, 1) - timedelta(seconds=1)
    else:
        end_of_month_dt = datetime(now.year, now.month + 1, 1) - timedelta(seconds=1)

    timestamp_from = int(start_of_month.timestamp())
    timestamp_to = int(now.timestamp())

    url = (
        f"{api_url}?page=1&limit={page_size}"
        f"&structureType=complete&timestampFrom={timestamp_from}&timestampTo={timestamp_to}"
    )
    headers = {"X-MAGIS5-APIKEY": api_key}

    try:
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()
        json_data = response.json()
        orders = json_data.get('orders', [])

        if not orders:
            print("⚠️ Nenhum pedido retornado da API para o mês vigente.")
            return

        print("\n--- Dados Brutos da API Magis5 (Amostra) ---")
        for i, order in enumerate(orders):
            print(f"\nPedido {i+1} (ID: {order.get('id')}):")
            print(f"  Data do Pedido (dateCreated): {order.get('dateCreated')}")
            print(f"  Loja (channelName): {order.get('channelName')}")
            print(f"  Status (status): {order.get('status')}")
            print(f"  Número Pedido ERP (erpId): {order.get('erpId')}")
            print(f"  ID Canal Marketplace (channel): {order.get('channel')}")
            print(f"  Rastreio (shipping.shipping_number): {order.get('shipping', {}).get('shipping_number')}")
            print(f"  Tipo Logística (shipping.logistic_type): {order.get('shipping', {}).get('logistic_type')}")
            print(f"  Número Carrinho (packId): {order.get('packId')}")

            if order.get('order_items'):
                for j, item in enumerate(order['order_items']):
                    print(f"    Item {j+1}:")
                    print(f"      SKU (item.seller_custom_field): {item.get('item', {}).get('seller_custom_field')}")
                    print(f"      Título (item.title): {item.get('item', {}).get('title')}")
                    print(f"      Valor Unitário (unit_price): {item.get('unit_price')}")
                    print(f"      Quantidade (quantity): {item.get('quantity')}")
            else:
                print("    Nenhum item encontrado para este pedido.")

    except requests.exceptions.HTTPError as e:
        print(f"❌ ERRO HTTP ao acessar API Magis5: {e}")
        print(f"   Resposta da API: {response.text}")
    except requests.exceptions.ConnectionError as e:
        print(f"⚠️ ERRO de Conexão ao acessar API Magis5: {e}")
        print("   Verifique sua conexão com a internet ou o status do serviço do Magis5.")
    except Exception as e:
        print(f"❌ ERRO inesperado ao processar API Magis5: {e}")

if __name__ == "__main__":
    debug_magis5_api_data(config.MAGIS5_API_URL, config.MAGIS5_API_KEY, page_size=5)
