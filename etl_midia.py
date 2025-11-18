import pandas as pd
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from thefuzz import process, fuzz

# IMPORTA O ESPECIALISTA BRADESCO
import etl_bradesco

# --- CONFIGURAÇÃO PADRÃO (SPORTINGBET) ---
SINONIMOS_COLUNAS = {
    'code':           ['code'],
    'campaign':       ['campaign'],
    'target':         ['target'],
    'country':        ['country'],
    'market':         ['market'],
    'state':          ['state'],
    'location':       ['location'],
    'exibidor':       ['exhibitor'], 
    'media':          ['media'],
    'classification': ['classification'],
    'type':           ['type'],
    'size':           ['size'],
    'frequency':      ['frequency'],
    'period_quantity':['period quantity'],
    'insertion_faces_period': ['insertion'],
    'purchase_unit':  ['purchase unit'],
    'start_date':     ['start date'],
    'end_date':       ['end date'],
    'weekly_flow':    ['weekly flow'],
    'weekly_impact':  ['weekly impact'],
    'periodic_impact':['periodic impact'],
    'faces_x_frequency': ['faces x frequency'],
    'cpm_target':     ['cpm/target'],
    'net_unit_price': ['net unit price'],
    'net_total':      ['net total']
}

SINONIMOS_ABAS_PADRAO = ['media plan']
PALAVRA_CHAVE_PADRAO = 'CODE'

# --- FUNÇÕES DE NORMALIZAÇÃO (Mantidas Iguais) ---
def carregar_mapas(conexao):
    print("-> Carregando mapas de normalização...")
    mapas = {'exibidor': ({}, {}), 'media': ({}, {}), 'classification': ({}, {}),
             'display_type': ({}, {}), 'cliente': ({}, {}), 'campaign': ({}, {}), 'target': ({}, {})}
    def carregar(nome_dim, nome_id, tem_alias=True):
        try:
            g_df = pd.read_sql(f"SELECT {nome_id}, nome_oficial FROM {nome_dim}", conexao)
            mapas[nome_dim.replace('dim_', '')][0] = dict(zip(g_df['nome_oficial'].str.strip(), g_df[nome_id]))
            if tem_alias:
                m_df = pd.read_sql(f"SELECT alias_sujo, {nome_id}_fk FROM mapa_{nome_dim.replace('dim_', '')}_alias", conexao)
                mapas[nome_dim.replace('dim_', '')][1] = dict(zip(m_df['alias_sujo'].str.strip().str.upper(), m_df[f"{nome_id}_fk"]))
            else: mapas[nome_dim.replace('dim_', '')][1] = dict(zip(g_df['nome_oficial'].str.strip().str.upper(), g_df[nome_id]))
        except Exception: pass
    
    carregar('dim_exibidor', 'id_exibidor'); carregar('dim_media', 'id_media')
    carregar('dim_campaign', 'id_campaign'); carregar('dim_target', 'id_target')
    carregar('dim_cliente', 'id_cliente', tem_alias=False) 
    carregar('dim_classification', 'id_classification', tem_alias=False)
    carregar('dim_display_type', 'id_display_type', tem_alias=False)
    return mapas

def normalizar_dado_simples(texto_sujo, nome_dimensao, nome_id_dimensao, gabarito_dict, conexao):
    if pd.isna(texto_sujo) or str(texto_sujo).strip() == '': return None
    texto_limpo = str(texto_sujo).strip(); texto_upper = texto_limpo.upper()
    if texto_upper in gabarito_dict: return gabarito_dict[texto_upper]
    sql_novo = text(f"INSERT INTO {nome_dimensao} (nome_oficial) VALUES (:nome) ON CONFLICT (nome_oficial) DO NOTHING RETURNING {nome_id_dimensao}")
    id_novo = conexao.execute(sql_novo, {"nome": texto_limpo}).scalar()
    if id_novo is None: id_novo = conexao.execute(text(f"SELECT {nome_id_dimensao} FROM {nome_dimensao} WHERE nome_oficial = :nome"), {"nome": texto_limpo}).scalar()
    gabarito_dict[texto_upper] = id_novo
    return id_novo

def normalizar_dado_fuzzy(texto_sujo, tipo_dimensao, gabarito_dict, mapa_alias_dict, conexao, **kwargs_extra):
    if pd.isna(texto_sujo) or str(texto_sujo).strip() == '': return None
    texto_sujo_str = str(texto_sujo).strip(); texto_upper = texto_sujo_str.upper()
    if texto_upper in mapa_alias_dict: return mapa_alias_dict[texto_upper]
    if gabarito_dict: 
        melhor_match = process.extractOne(texto_upper, gabarito_dict.keys(), scorer=fuzz.token_sort_ratio)
        if melhor_match and melhor_match[1] >= 90:
            nome_oficial_match = melhor_match[0]; id_limpo = gabarito_dict[nome_oficial_match]
            sql_aprender = text(f"INSERT INTO mapa_{tipo_dimensao}_alias (alias_sujo, id_{tipo_dimensao}_fk) VALUES (:sujo, :id) ON CONFLICT (alias_sujo) DO NOTHING")
            conexao.execute(sql_aprender, {"sujo": texto_sujo_str, "id": id_limpo}) 
            mapa_alias_dict[texto_upper] = id_limpo
            return id_limpo
    params = {"nome": texto_sujo_str}; coluna_fk, valor_fk = "", ""
    if tipo_dimensao == 'media' and 'id_classification_fk' in kwargs_extra:
        id_class_fk = kwargs_extra['id_classification_fk']
        if pd.notna(id_class_fk): coluna_fk = ", id_classification_fk"; valor_fk = ", :id_class_fk"; params['id_class_fk'] = int(id_class_fk) 
    sql_novo_gabarito = text(f"INSERT INTO dim_{tipo_dimensao} (nome_oficial {coluna_fk}) VALUES (:nome {valor_fk}) ON CONFLICT (nome_oficial) DO NOTHING RETURNING id_{tipo_dimensao}")
    id_novo = conexao.execute(sql_novo_gabarito, params).scalar() 
    if id_novo is None: id_novo = conexao.execute(text(f"SELECT id_{tipo_dimensao} FROM dim_{tipo_dimensao} WHERE nome_oficial = :nome"), {"nome": texto_sujo_str}).scalar()
    sql_novo_alias = text(f"INSERT INTO mapa_{tipo_dimensao}_alias (alias_sujo, id_{tipo_dimensao}_fk) VALUES (:sujo, :id) ON CONFLICT (alias_sujo) DO NOTHING")
    conexao.execute(sql_novo_alias, {"sujo": texto_sujo_str, "id": id_novo})
    gabarito_dict[texto_sujo_str] = id_novo; mapa_alias_dict[texto_upper] = id_novo
    return id_novo

def ler_plano_padrao(caminho_completo):
    try:
        xls = pd.ExcelFile(caminho_completo)
        aba_alvo = None
        for aba in xls.sheet_names:
            if any(s in aba.lower().strip() for s in SINONIMOS_ABAS_PADRAO):
                aba_alvo = aba; break
        if not aba_alvo: return None, f"Aba padrão não encontrada."
        df_head = pd.read_excel(xls, sheet_name=aba_alvo, header=None, nrows=30)
        linha_cabecalho = -1
        for i, row in df_head.iterrows():
            if any(PALAVRA_CHAVE_PADRAO in str(c).strip().upper() for c in row[:10]):
                linha_cabecalho = i; break
        if linha_cabecalho == -1: return None, f"Cabeçalho '{PALAVRA_CHAVE_PADRAO}' não encontrado."
        df_sujo = pd.read_excel(xls, sheet_name=aba_alvo, header=linha_cabecalho)
        df_sujo.columns = df_sujo.columns.astype(str).str.replace('\n', ' ').str.strip().str.lower()
        df_limpo = pd.DataFrame()
        for col_sql, lista_sinonimos in SINONIMOS_COLUNAS.items():
            match_coluna_excel = None
            for sinonimo in lista_sinonimos:
                match = next((c for c in df_sujo.columns if sinonimo in c), None)
                if match: match_coluna_excel = match; break
            if match_coluna_excel: df_limpo[col_sql] = df_sujo[match_coluna_excel]
            else: df_limpo[col_sql] = None
        return df_limpo, "OK"
    except Exception as e: return None, str(e)

# --- SCRIPT PRINCIPAL ---
load_dotenv()
print("\n--- INICIANDO ETL v8.0 (BRADESCO + AUTO-NULL) ---")

db_user = os.getenv('DB_USER'); db_pass = os.getenv('DB_PASS')
db_host = os.getenv('DB_HOST'); db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME'); pasta_macro = os.getenv('CAMINHO_MIDIA') 

if not pasta_macro: exit("ERRO: Variável CAMINHO_MIDIA não definida.")

try:
    str_conexao = f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'
    db_connection = create_engine(str_conexao)
    print("-> Banco conectado.")
except Exception as e: exit(f"ERRO BANCO: {e}")

mapas = carregar_mapas(db_connection)
(gabarito_exibidor, mapa_exibidor) = mapas['exibidor']
(gabarito_media, mapa_media) = mapas['media']
(gabarito_classification, _) = mapas['classification']
(gabarito_display_type, _) = mapas['display_type']
(gabarito_cliente, _) = mapas['cliente'] 
(gabarito_campaign, mapa_campaign) = mapas['campaign']
(gabarito_target, mapa_target) = mapas['target']

print("-> Verificando versões...")
controle_versoes = {}
try:
    df_check = pd.read_sql("SELECT DISTINCT arquivo_origem, MAX(file_timestamp) as ts FROM fato_midia GROUP BY arquivo_origem", db_connection)
    controle_versoes = dict(zip(df_check['arquivo_origem'], df_check['ts']))
except Exception: pass

contador_novos = 0; contador_atualizados = 0; contador_ignorados = 0
arquivos_encontrados_na_pasta = []

with db_connection.begin() as conn: 
    for nome_cliente_pasta in os.listdir(pasta_macro):
        pasta_cliente = os.path.join(pasta_macro, nome_cliente_pasta)
        if not os.path.isdir(pasta_cliente): continue
        
        id_cliente = normalizar_dado_simples(nome_cliente_pasta, 'dim_cliente', 'id_cliente', gabarito_cliente, conn)
        nome_cliente_upper = str(nome_cliente_pasta).strip().upper()
        print(f"\n--- Cliente: {nome_cliente_upper} ---")
        
        for arquivo in os.listdir(pasta_cliente):
            if not (arquivo.endswith('.xlsx') and not arquivo.startswith('~$')): continue
            
            arquivo_origem = f"{nome_cliente_upper}/{arquivo}"
            arquivos_encontrados_na_pasta.append(arquivo_origem)
            caminho_completo = os.path.join(pasta_cliente, arquivo)
            timestamp_atual = os.path.getmtime(caminho_completo)
            
            modo_operacao = 'NOVO'
            if arquivo_origem in controle_versoes:
                if controle_versoes[arquivo_origem] is not None and abs(timestamp_atual - controle_versoes[arquivo_origem]) <= 0.1:
                    contador_ignorados += 1; continue
                modo_operacao = 'ATUALIZAR'

            print(f"  [{modo_operacao}] {arquivo}")
            
            if modo_operacao == 'ATUALIZAR':
                conn.execute(text("DELETE FROM fato_midia WHERE arquivo_origem = :arq"), {"arq": arquivo_origem})
                contador_atualizados += 1
            else: contador_novos += 1

            # ==================================================================
            # ESTRATÉGIA DE LEITURA
            # ==================================================================
            df_limpo = None
            erro_leitura = ""

            if nome_cliente_upper == 'BRADESCO':
                df_limpo = etl_bradesco.ler_plano_bradesco(caminho_completo)
                if df_limpo is None: erro_leitura = "Erro na leitura Bradesco"
            else:
                df_limpo, erro_leitura = ler_plano_padrao(caminho_completo)

            if df_limpo is None:
                print(f"     [PULADO] {erro_leitura}"); continue

            try:
                # --- GARANTIA DE COLUNAS (O "AUTO-NULL") ---
                # Se o df_limpo do Bradesco não tiver 'net_total', cria ela como None
                colunas_obrigatorias_db = list(SINONIMOS_COLUNAS.keys())
                for col in colunas_obrigatorias_db:
                    if col not in df_limpo.columns:
                        df_limpo[col] = None

                if 'code' not in df_limpo.columns or df_limpo['code'].isnull().all():
                     print("     [ALERTA] Coluna 'code' vazia."); continue
                     
                df_limpo = df_limpo.dropna(subset=['code'])
                try:
                    coluna_code_upper = df_limpo['code'].astype(str).str.upper().str.strip()
                    indices_total = df_limpo.index[coluna_code_upper.str.contains('TOTAL', na=False)].tolist()
                    if indices_total: df_limpo = df_limpo.loc[:indices_total[0]-1]
                except: pass
                
                df_limpo = df_limpo[df_limpo['code'].apply(lambda x: len(str(x).strip()) < 25)]

                cols_numericas = ['period_quantity', 'purchase_unit', 'net_total', 'net_unit_price', 'weekly_flow', 'weekly_impact', 'periodic_impact']
                for col in cols_numericas:
                    if col in df_limpo.columns: df_limpo[col] = pd.to_numeric(df_limpo[col], errors='coerce')
                
                if df_limpo.empty: print("     [ALERTA] Arquivo vazio após limpeza."); continue
                
                print("     -> Normalizando...")
                
                id_class_series = df_limpo['classification'].apply(lambda x: normalizar_dado_simples(x, 'dim_classification', 'id_classification', gabarito_classification, conn))
                df_limpo['id_display_type'] = df_limpo['type'].apply(lambda x: normalizar_dado_simples(x, 'dim_display_type', 'id_display_type', gabarito_display_type, conn))
                df_limpo['id_exibidor'] = df_limpo['exibidor'].apply(lambda x: normalizar_dado_fuzzy(x, 'exibidor', gabarito_exibidor, mapa_exibidor, conn))
                df_limpo['id_campaign'] = df_limpo['campaign'].apply(lambda x: normalizar_dado_fuzzy(x, 'campaign', gabarito_campaign, mapa_campaign, conn))
                df_limpo['id_target'] = df_limpo['target'].apply(lambda x: normalizar_dado_fuzzy(x, 'target', gabarito_target, mapa_target, conn))
                df_limpo['id_media'] = df_limpo.apply(lambda row: normalizar_dado_fuzzy(row['media'], 'media', gabarito_media, mapa_media, conn, id_classification_fk=id_class_series.get(row.name)), axis=1)
                
                df_limpo['id_cliente'] = id_cliente 
                df_limpo['arquivo_origem'] = arquivo_origem
                df_limpo['file_timestamp'] = timestamp_atual
                df_limpo['is_active'] = True
                
                # Filtra para garantir que só colunas que existem na 'fato_midia' sejam enviadas
                # (Evita erro se o etl_bradesco.py devolveu alguma coluna extra sem querer)
                colunas_finais = [
                    'code', 'size', 'frequency', 'period_quantity', 'insertion_faces_period', 'purchase_unit', 
                    'start_date', 'end_date', 'weekly_flow', 'weekly_impact', 'periodic_impact', 
                    'faces_x_frequency', 'cpm_target', 'net_unit_price', 'net_total', 
                    'id_display_type', 'id_exibidor', 'id_campaign', 'id_target', 'id_media', 'id_cliente',
                    'arquivo_origem', 'file_timestamp', 'is_active', 
                    'country', 'market', 'state', 'location' # Colunas de texto de geografia
                ]
                
                # Seleciona apenas as colunas existentes
                df_carga = df_limpo[[c for c in colunas_finais if c in df_limpo.columns]]
                
                df_carga.to_sql('fato_midia', con=conn, if_exists='append', index=False, method='multi')
                print(f"     -> SUCESSO! {len(df_limpo)} linhas.")

            except Exception as e: print(f"     -> ERRO NO ARQUIVO: {e}")

    print("\n--- Sincronizando arquivos deletados ---")
    try:
        set_arquivos_pasta = set(arquivos_encontrados_na_pasta) 
        query_ativos = "SELECT DISTINCT arquivo_origem FROM fato_midia WHERE is_active = true"
        df_banco_ativos = pd.read_sql(query_ativos, conn)
        set_banco_ativos = set(df_banco_ativos['arquivo_origem'])
        arquivos_para_inativar = set_banco_ativos - set_arquivos_pasta
        if arquivos_para_inativar:
            print(f"-> Inativando {len(arquivos_para_inativar)} arquivos.")
            for arq_morto in arquivos_para_inativar:
                conn.execute(text("UPDATE fato_midia SET is_active = false WHERE arquivo_origem = :arq"), {"arq": arq_morto})
        else: print("-> Banco sincronizado.")
    except Exception as e: print(f"   -> Erro na poda: {e}")

print(f"\n--- FIM: Novos: {contador_novos} | Atualizados: {contador_atualizados} | Ignorados: {contador_ignorados} ---")