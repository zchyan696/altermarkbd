import pandas as pd
import os
from thefuzz import process, fuzz
import settings # <--- Importa o arquivo de configurações

def ler_plano_bradesco(caminho_arquivo):
    print(f"     [MODO BRADESCO] Iniciando leitura complexa...")
    
    try:
        xls = pd.ExcelFile(caminho_arquivo)
        
        # 1. EXTRAÇÃO DE METADADOS (CAPA)
        codigo_demanda = None
        nome_campanha = None
        
        if 'Capa' in xls.sheet_names:
            try:
                df_capa = pd.read_excel(xls, sheet_name='Capa', header=None)
                for i, row in df_capa.head(20).iterrows():
                    celula_b = str(row[1]).strip().upper()
                    if 'DEMANDA' in celula_b: codigo_demanda = str(row[2]).strip()
                    if 'CAMPANHA' in celula_b: nome_campanha = str(row[2]).strip()
            except Exception as e: pass
        
        if not codigo_demanda: codigo_demanda = os.path.basename(caminho_arquivo)

        # 2. LEITURA DAS ABAS DE DADOS
        dfs_para_juntar = []
        abas_alvo = ['MIDIA OBRIGATÓRIA', 'MÍDIA OBRIGATÓRIA', 'MIDIA AVULSA', 'MÍDIA AVULSA', 'MIDIA OBRIGATORIA']
        
        for aba in xls.sheet_names:
            if aba.upper() in abas_alvo:
                df_temp = pd.read_excel(xls, sheet_name=aba, header=None, nrows=30)
                linha_header = -1
                for i, row in df_temp.iterrows():
                    linha_str = row.astype(str).str.upper().values
                    if 'CIDADE' in linha_str and 'EXIBIDOR' in linha_str:
                        linha_header = i; break
                
                if linha_header != -1:
                    df_dados = pd.read_excel(xls, sheet_name=aba, header=linha_header)
                    if 'Cidade' in df_dados.columns:
                        coluna_cidade = df_dados['Cidade'].astype(str).str.upper()
                        indices_total = df_dados.index[coluna_cidade.str.contains('TOTAL', na=False)].tolist()
                        if indices_total:
                            df_dados = df_dados.iloc[:indices_total[0]]
                        df_dados = df_dados.dropna(subset=['Cidade'])
                    
                    dfs_para_juntar.append(df_dados)

        if not dfs_para_juntar:
            print("     [ERRO] Nenhuma aba de mídia encontrada.")
            return None
            
        df_final = pd.concat(dfs_para_juntar, ignore_index=True)

        # 3. PADRONIZAÇÃO VIA SINÔNIMOS (USANDO SETTINGS)
        df_final.columns = df_final.columns.astype(str).str.replace('\n', ' ').str.strip().str.lower()
        
        df_padronizado = pd.DataFrame()
        
        # Usa a lista que está no arquivo settings.py
        for col_banco, lista_sinonimos in settings.SINONIMOS_BRADESCO.items():
            melhor_coluna_excel = None
            
            # --- DECISÃO: O QUE RODA O FUZZY (90%)? ---
            if col_banco in ['start_date', 'end_date', 'periodic_impact']:
                melhor_score = 0
                for sinonimo in lista_sinonimos:
                    match_tuple = process.extractOne(sinonimo, df_final.columns, scorer=fuzz.token_sort_ratio)
                    if match_tuple and match_tuple[1] >= 90 and match_tuple[1] > melhor_score:
                        melhor_score = match_tuple[1]
                        melhor_coluna_excel = match_tuple[0]
            else:
                # RODA EXACT MATCH
                for sinonimo in lista_sinonimos:
                    match = next((c for c in df_final.columns if sinonimo == c), None) 
                    if match:
                        melhor_coluna_excel = match
                        break
            
            if melhor_coluna_excel:
                df_padronizado[col_banco] = df_final[melhor_coluna_excel]
        
        # 4. INJEÇÃO DE VALORES FIXOS E METADADOS
        if len(df_padronizado) > 0:
            df_padronizado['country'] = 'BRASIL'
            
        df_padronizado['code'] = codigo_demanda
        df_padronizado['campaign'] = nome_campanha
        
        return df_padronizado

    except Exception as e:
        print(f"     [ERRO CRÍTICO BRADESCO] {e}")
        return None