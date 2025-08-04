import sqlite3
import pandas as pd
import locale # Para configurar o locale, caso queira formatar outros tipos de dados

# --- Configurações ---
db_file = 'D:/Analyst/DS4B/sanoyfresco/sanoyfresco.db'  # Altere para o nome do seu arquivo .db
table_name = 'tickets' # Altere para o nome da tabela que você quer
output_csv = 'D:/Analyst/DS4B/sanoyfresco/sanoyfresco_espanhol.csv' # Nome do arquivo CSV de saída

# --- Configurar o locale para o formato espanhol (opcional, mas bom para outros formatos) ---
# Tenta configurar para o locale espanhol. Pode variar ligeiramente entre OS.
# Em Windows: 'es_ES' ou 'Spanish_Spain'
# Em Linux/macOS: 'es_ES.UTF-8'
try:
    locale.setlocale(locale.LC_ALL, 'es_ES.UTF-8')
except locale.Error:
    try:
        # Tenta uma alternativa para Windows
        locale.setlocale(locale.LC_ALL, 'Spanish_Spain')
    except locale.Error:
        print("Aviso: Não foi possível configurar o locale 'es_ES'. A formatação decimal ainda funcionará via Pandas.")

# --- Processo de Exportação ---
try:
    # Conectar ao banco de dados SQLite
    conn = sqlite3.connect(db_file)

    # Ler os dados da tabela para um DataFrame do pandas
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)

    # Exportar o DataFrame para um arquivo CSV
    # Usamos decimal=',' para garantir que os pontos sejam trocados por vírgulas para decimais
    # e sep=';' é comum em CSVs europeus para evitar conflito com vírgulas em decimais
    df.to_csv(output_csv, index=False, encoding='utf-8', decimal=',', sep=';')

    print(f"Tabela '{table_name}' exportada com sucesso para '{output_csv}' com formato espanhol (vírgulas decimais).")

except sqlite3.Error as e:
    print(f"Erro ao conectar ou ler o banco de dados: {e}")
except Exception as e:
    print(f"Ocorreu um erro: {e}")
finally:
    # Fechar a conexão com o banco de dados
    if conn:
        conn.close()