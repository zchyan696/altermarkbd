# Pipeline de Engenharia de Dados - Mídia OOH 📊

Este projeto automatiza a consolidação de dados de mídia Out-of-Home (OOH), transformando planilhas Excel despadronizadas em um Data Warehouse confiável e normalizado.

## 🏗️ Arquitetura

1.  **Extração:** Script Python lê arquivos Excel de uma pasta monitorada (SharePoint/OneDrive).
2.  **Normalização:** Utiliza lógica *Fuzzy* (semelhança de texto) para corrigir erros de digitação em nomes de Exibidores, Mídias e Campanhas automaticamente.
3.  **Modelagem:** Dados são salvos em um banco **PostgreSQL** seguindo o modelo **Star Schema** (Fato e Dimensões).
4.  **Sincronização:** Implementação de *Soft Delete* para manter o banco 100% sincronizado com a pasta de origem (se deletar o arquivo, o dado é inativado).
5.  **Visualização:** Power BI conectado diretamente ao banco para relatórios de performance.

## 🛠️ Tecnologias

* **Python 3.11** (Pandas, SQLAlchemy, TheFuzz)
* **PostgreSQL** (Banco de Dados Relacional)
* **Power BI** (Dashboard e DAX avançado para diarização de custos)
* **Git/GitHub** (Versionamento)

## 🚀 Como rodar

1.  Clone o repositório.
2.  Instale as dependências: `pip install -r requirements.txt`
3.  Crie um arquivo `.env` com as credenciais do banco (veja `.env.example`).
4.  Execute: `python etl_midia.py`

---
*Projeto desenvolvido para otimizar o fluxo de dados da Agência Altermark.*