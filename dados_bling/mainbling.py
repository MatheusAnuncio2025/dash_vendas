import requests
import json
import base64
import os
import time
import csv
import secrets
import pandas as pd
from decimal import Decimal, getcontext
import io
import unicodedata
import sys

# ★★★ INÍCIO DA CORREÇÃO ★★★
# Adiciona o diretório pai ao path para permitir a importação do config.py
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config
# ★★★ FIM DA CORREÇÃO ★★★

# --- 1. CONFIGURAÇÕES GERAIS DO BLING ---
CLIENT_ID = "eeefd981ec1f89e66a40847fc5157a9105cda608"
CLIENT_SECRET = "4268ade685394eed2608e1219371f99062aa60a8c5d017388b5b1ab58256"
REDIRECT_URI = "http://localhost:8080/"

# ★★★ CORREÇÃO: O caminho do arquivo de tokens agora é relativo ao arquivo de dados do Bling do config
TOKEN_FILE = os.path.join(os.path.dirname(config.ARQUIVO_BLING_PRODUTOS_CSV), "bling_tokens.json")

OAUTH_AUTH_URL = "https://www.bling.com.br/b/OAuth2/views/authorization.php"
OAUTH_TOKEN_URL = "https://www.bling.com.br/Api/v3/oauth/token"
PRODUCTS_API_URL = "https://api.bling.com.br/Api/v3/produtos"

SCOPES = "98309 98314 5990556 6631498 318257570 318257583 333936575 363953167 363953556 363953706 1869535257"

# --- 2. CONFIGURAÇÕES DA PLANILHA GOOGLE SHEETS ---
URL_PLANILHA_GOOGLE_CSV = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQA4DJVtDoKL2ypaInsieOXgU234SXRk8HXnAD0vkA9u8m7Jm9gaKyYkzy7O08IuFza8_VIEoRcwqTT/pub?gid=273712958&single=true&output=csv"

# Mapeamento para os dados da planilha
MAPEAMENTO_GOOGLE_SHEETS = {
   'Codigo': 'sku',
   'Produto': 'Produto',
   'Quantidade': 'Quantidade',
   'Valor unitario': 'Valor unitario',
   'Fornecedores': 'Fornecedores',
   'Categoria': 'Categoria',
   'Subcategoria': 'Subcategoria',
   'Tipo de Venda': 'tipo_de_venda'
}

# --- 3. GERENCIAMENTO DE REQUISIÇÕES E TOKENS ---
REQUEST_COUNT = 0

def increment_request_count():
   """Incrementa o contador de requisições global."""
   global REQUEST_COUNT
   REQUEST_COUNT += 1

def load_tokens():
   """Carrega os tokens de um arquivo JSON."""
   if os.path.exists(TOKEN_FILE):
       with open(TOKEN_FILE, 'r') as f:
           try:
               return json.load(f)
           except json.JSONDecodeError:
               print(f"Erro ao ler '{TOKEN_FILE}'. O arquivo pode estar corrompido ou vazio.")
               return None
   return None

def save_tokens(tokens):
   """Salva os tokens em um arquivo JSON."""
   # ★★★ CORREÇÃO: Garante que o diretório de destino (do config) exista
   os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)
   with open(TOKEN_FILE, 'w') as f:
       json.dump(tokens, f, indent=4)
   print(f"Tokens salvos em '{TOKEN_FILE}'.")

def is_access_token_expired(token_info):
   """Verifica se o access_token está prestes a expirar."""
   if not token_info or "expires_at" not in token_info:
       return True
   return time.time() >= token_info["expires_at"] - 300

def refresh_access_token(current_refresh_token):
   """Renova o access_token usando o refresh_token."""
   print("Renovando access_token...")
   auth_string = f"{CLIENT_ID}:{CLIENT_SECRET}"
   encoded_auth_string = base64.b64encode(auth_string.encode()).decode()
   headers = {"Content-Type": "application/x-www-form-urlencoded", "Authorization": f"Basic {encoded_auth_string}"}
   data = {"grant_type": "refresh_token", "refresh_token": current_refresh_token}

   try:
       response = requests.post(OAUTH_TOKEN_URL, headers=headers, data=data)
       increment_request_count()
       response.raise_for_status()
       new_tokens = response.json()
       new_tokens["expires_at"] = time.time() + new_tokens["expires_in"]
       save_tokens(new_tokens)
       print("Access_token renovado com sucesso!")
       return new_tokens.get("access_token")
   except requests.exceptions.RequestException as e:
       print(f"Erro ao renovar o access_token: {e}")
       if response is not None:
           print(f"Resposta de erro do Bling: {response.text}")
       return None

def get_valid_access_token():
   """Obtém um access_token válido, renovando ou solicitando autorização se necessário."""
   token_info = load_tokens()
   if token_info and not is_access_token_expired(token_info):
       print("Access_token válido encontrado no arquivo.")
       return token_info.get("access_token")
   if token_info and "refresh_token" in token_info:
       access_token = refresh_access_token(token_info["refresh_token"])
       if access_token:
           return access_token
   
   print("\n--- ATENÇÃO: Autorização manual necessária! ---")
   state = secrets.token_hex(16)
   auth_url = (f"{OAUTH_AUTH_URL}?client_id={CLIENT_ID}&response_type=code&"
               f"state={state}&scopes={SCOPES.replace(' ', '+')}")
   print("1. Acesse a URL abaixo no seu navegador:")
   print(auth_url)
   print("\n2. Após autorizar, copie o 'code' da URL de redirecionamento.")
   code = input("3. Cole o 'code' aqui: ").strip()
   
   auth_string = f"{CLIENT_ID}:{CLIENT_SECRET}"
   encoded_auth_string = base64.b64encode(auth_string.encode()).decode()
   headers = {"Content-Type": "application/x-www-form-urlencoded", "Authorization": f"Basic {encoded_auth_string}"}
   data = {"grant_type": "authorization_code", "code": code, "redirect_uri": REDIRECT_URI,
           "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET}
   try:
       response = requests.post(OAUTH_TOKEN_URL, headers=headers, data=data)
       increment_request_count()
       response.raise_for_status()
       initial_tokens = response.json()
       initial_tokens["expires_at"] = time.time() + initial_tokens["expires_in"]
       save_tokens(initial_tokens)
       print("Tokens iniciais obtidos e salvos com sucesso!")
       return initial_tokens.get("access_token")
   except requests.exceptions.RequestException as e:
       print(f"Erro ao obter tokens iniciais: {e}")
       if response is not None:
           print(f"Resposta de erro do Bling: {response.text}")
       return None

# --- 4. BUSCA E PROCESSAMENTO DE DADOS ---

def fetch_all_products_from_bling(access_token):
   """Busca todos os produtos da API do Bling e retorna um DataFrame."""
   print("\nIniciando a busca por todos os produtos do Bling. Isso pode levar um tempo...")
   all_products_data = []
   page = 1
   limit_per_page = 100
   
   while True:
       params = {"limite": limit_per_page, "pagina": page}
       headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
       print(f"Buscando produtos (página {page})...")
       try:
           response = requests.get(PRODUCTS_API_URL, headers=headers, params=params, timeout=30)
           increment_request_count()
           response.raise_for_status()
           products_response = response.json()
           
           products = products_response.get("data", [])
           if not products:
               break
           
           for product in products:
               all_products_data.append({
                   "sku": product.get("codigo", ""),
                   "Produto": product.get("nome", ""),
                   "UN": "UN",
                   "Quantidade": product.get("estoque", {}).get("saldoVirtualTotal", 0),
                   "Valor unitario": f"{product.get('precoCusto', 0.0):.2f}".replace('.', ',')
               })
           page += 1
           time.sleep(2)
       except requests.exceptions.RequestException as e:
           print(f"\nErro ao buscar produtos na página {page}: {e}")
           if response is not None:
               print(f"Resposta de erro do Bling: {response.text}")
           break
           
   if not all_products_data:
       print("⚠️ Nenhum produto foi encontrado no Bling.")
       return pd.DataFrame()
       
   df_bling = pd.DataFrame(all_products_data)
   # Após buscar do Bling, enriquecemos com a planilha
   return enrich_with_google_sheets(df_bling)


def fetch_products_from_google_sheets():
    """Busca e processa dados de produtos diretamente da planilha Google Sheets."""
    print("\n📥 Buscando dados de produtos diretamente da planilha Google Sheets...")
    try:
        response = requests.get(URL_PLANILHA_GOOGLE_CSV, verify=False, timeout=30)
        response.raise_for_status()
        
        df_gs = pd.read_csv(io.StringIO(response.content.decode('utf-8-sig')), header=0)
        print("✅ Planilha Google lida com sucesso.")

        # Renomeia as colunas para o padrão do script
        df_gs = df_gs.rename(columns=MAPEAMENTO_GOOGLE_SHEETS)

        if 'sku' not in df_gs.columns:
            print("❌ ERRO CRÍTICO: A coluna 'Codigo' (renomeada para 'sku') não foi encontrada na planilha.")
            return pd.DataFrame()

        # Adiciona a coluna 'UN' que existia na saída do Bling
        df_gs['UN'] = 'UN'

        # Garante que as colunas esperadas existam
        required_cols = ['sku', 'Produto', 'UN', 'Quantidade', 'Valor unitario', 'Fornecedores', 'Categoria', 'Subcategoria', 'tipo_de_venda']
        for col in required_cols:
            if col not in df_gs.columns:
                df_gs[col] = ''
                print(f"⚠️ Coluna '{col}' não encontrada na planilha e criada como vazia.")

        if 'Quantidade' in df_gs.columns:
            df_gs['Quantidade'] = pd.to_numeric(df_gs['Quantidade'], errors='coerce').fillna(0).astype(int)

        return df_gs[required_cols]

    except requests.exceptions.RequestException as e:
        print(f"❌ ERRO ao acessar Google Sheets: {e}. Verifique o link e se a planilha está publicada na web.")
    except Exception as e:
        print(f"❌ ERRO inesperado ao processar a planilha Google Sheets: {e}")
    
    return pd.DataFrame()


def enrich_with_google_sheets(df_bling_products):
    """Busca dados da Planilha Google e os mescla com o DataFrame de produtos do Bling."""
    print("\n🔗 Enriquecendo dados do Bling com a planilha Google Sheets...")
    
    try:
        response = requests.get(URL_PLANILHA_GOOGLE_CSV, verify=False, timeout=30)
        response.raise_for_status()
        
        df_gs = pd.read_csv(io.StringIO(response.content.decode('utf-8-sig')), header=0)
        print("✅ Planilha Google lida com sucesso.")

        # Renomeia apenas as colunas de enriquecimento
        df_gs = df_gs.rename(columns={
            'Codigo': 'sku', 'Fornecedores': 'Fornecedores', 'Categoria': 'Categoria', 
            'Subcategoria': 'Subcategoria', 'Tipo de Venda': 'tipo_de_venda'
        })

        if 'sku' not in df_gs.columns:
            print("❌ ERRO CRÍTICO: A coluna 'Codigo' não foi encontrada na sua planilha Google. O enriquecimento será pulado.")
            return df_bling_products

        cols_from_gs_for_merge = []
        for col in ['sku', 'Fornecedores', 'Categoria', 'Subcategoria', 'tipo_de_venda']:
            if col in df_gs.columns:
                df_gs[col] = df_gs[col].astype(str).str.strip().fillna('')
                cols_from_gs_for_merge.append(col)
        
        df_gs = df_gs.drop_duplicates(subset=['sku'], keep='last')
        
        df_merged = pd.merge(df_bling_products, df_gs[cols_from_gs_for_merge], on='sku', how='left')

        for col in ['Fornecedores', 'Categoria', 'Subcategoria', 'tipo_de_venda']:
            if col in df_merged.columns:
                df_merged[col] = df_merged[col].fillna('')
        
        print("✅ Dados de fornecedores, categorias e tipo de venda incorporados com sucesso.")
        return df_merged

    except Exception as e:
        print(f"❌ ERRO ao processar a planilha Google Sheets para enriquecimento: {e}")
    
    print("⚠️ Não foi possível enriquecer os dados. Prosseguindo apenas com dados do Bling.")
    for col in ['Fornecedores', 'Categoria', 'Subcategoria', 'tipo_de_venda']:
        if col not in df_bling_products.columns:
            df_bling_products[col] = ''
    return df_bling_products

# --- 5. GERAÇÃO DO CSV FINAL ---
def generate_csv_report(df_final):
   """Gera o arquivo CSV final a partir do DataFrame processado."""
   # ★★★ CORREÇÃO: O caminho do arquivo agora vem do config global
   csv_full_path = config.ARQUIVO_BLING_PRODUTOS_CSV

   if df_final.empty:
       print("Nenhum dado para gerar o relatório CSV.")
       return

   df_final = df_final.rename(columns={'sku': 'Código'})
   
   fieldnames = ["Código", "Produto", "UN", "Quantidade", "Valor unitario",
                 "Fornecedores", "Categoria", "Subcategoria", "Tipo de Venda"]
   
   if 'tipo_de_venda' in df_final.columns:
       df_final = df_final.rename(columns={'tipo_de_venda': 'Tipo de Venda'})
   
   for col in fieldnames:
       if col not in df_final.columns:
           df_final[col] = ''

   try:
       # ★★★ CORREÇÃO: Garante que o diretório de destino (do config) exista
       output_dir = os.path.dirname(csv_full_path)
       os.makedirs(output_dir, exist_ok=True)
       df_final.to_csv(csv_full_path, columns=fieldnames, sep=';', index=False, encoding='utf-8-sig')
       print(f"\n✅ Relatório CSV '{csv_full_path}' gerado com sucesso!")
       print(f"Total de produtos no relatório: {len(df_final)}")
   except IOError as e:
       print(f"❌ Erro ao escrever o arquivo CSV: {e}")

# --- 6. FLUXO PRINCIPAL ---
def main():
   print("Iniciando automação Bling...")
   
   # ★★★ ESCOLHA A FONTE DE DADOS AQUI ★★★
   
   # --- OPÇÃO 1: Usar a API do Bling (descomente as 3 linhas abaixo) ---
   # print("Modo API Bling ATIVADO.")
   # access_token = get_valid_access_token()
   # df_products = fetch_all_products_from_bling(access_token) if access_token else pd.DataFrame()

   # --- OPÇÃO 2: Usar a Planilha Google Sheets (deixe como está) ---
   print("Modo Planilha Google ATIVADO.")
   df_products = fetch_products_from_google_sheets()

   if not df_products.empty:
       generate_csv_report(df_products)
   else:
       print("⚠️ Processo encerrado pois nenhum produto foi retornado pela fonte de dados selecionada.")
   
   print(f"\nTotal de requisições à API Bling nesta execução: {REQUEST_COUNT}")

if __name__ == "__main__":
   main()