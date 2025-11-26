# ==============================================================================
# ARQUIVO DE CONFIGURAÇÃO GLOBAL (settings.py)
# ==============================================================================

# --- CONFIGURAÇÃO PADRÃO (UNIVERSAL/SPORTINGBET) ---
PALAVRA_CHAVE_PADRAO = 'CODE'

SINONIMOS_ABAS_PADRAO = [
    'media plan', 
    'plano', 
    'veiculacao', 
    'mídia',
    'plano base', # Coloquei em minúsculo também por precaução
    'base'
]

SINONIMOS_COLUNAS_PADRAO = {
    # --- Identificadores ---
    'code':           ['code'],
    'campaign':       ['campaign','campanha'],
    'target':         ['target'],
    
    # --- Geografia ---
    'country':        ['country'],
    'market':         ['market','praça','praca'],
    'state':          ['state','uf'],
    'location':       ['location','local/sinal'],
    
    # --- Dimensões ---
    'exibidor':       ['exhibitor','veículo','veiculo','exibidor'], 
    'media':          ['media','formato'],
    'classification': ['classification'],
    'type':           ['type','det/seg'],
    
    # --- Técnicas ---
    'size':           ['size'],
    'frequency':      ['frequency'],
    'period_quantity':['period quantity'],
    'insertion_faces_period': ['insertion'],
    
    # --- Datas ---
    'start_date':     ['start date','data de início','data de inicio'],
    'end_date':       ['end date','data final'],
    
    # --- Métricas de Alcance ---
    'weekly_flow':    ['weekly flow'],
    'weekly_impact':  ['weekly impact','potencial de impactos semanais'],
    
    # CORREÇÃO AQUI: Tudo minúsculo
    'periodic_impact':['periodic impact', 'potencial de impactos no período'], 
    
    'faces_x_frequency': ['faces x frequency','volume'],
    'cpm_target':     ['cpm/target'],
    
    # --- FINANCEIRO (ATUALIZADO) ---
    # CORREÇÃO AQUI: Tudo minúsculo
    'net_total':      ['net total', '$tt liquido negociado'], 
    
    'total_bonus':    ['net value bonus/ reapplication'], 
    'total_final':    ['efetivo total (net + fee)', 'efetivo total']
}

# --- CONFIGURAÇÃO ESPECIAL: BRADESCO ---
SINONIMOS_BRADESCO = {
    # Fuzzy 90% (Datas e Impacto)
    'start_date':      ['data de início', 'data de inicio'], 
    'end_date':        ['data de termino', 'data de término'], 
    'periodic_impact': ['potencial de impacto pop', 'potencial de impacto  pop'],
    
    # Exact Match (Geral)
    'market':          ['cidade'],
    'state':           ['uf'],
    'exibidor':        ['exibidor'],
    'media':           ['tipo'], 
    'classification':  ['tipo de mídia'], 
    'period_quantity': ['período', 'periodo'],
    'net_total':       ['r$ total liquido'],
    'cpm_target':      ['cpm (desembolso)'],
    'insertion_faces_period': ['faces'],
    'location':        ['mídia', 'midia'], 
    'size':            ['formato'],
}