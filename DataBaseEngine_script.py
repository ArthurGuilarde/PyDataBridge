# -*- coding: utf-8 -*-
"""
Created on Tue Jan 30 16:11:34 2024

@author: arthur.guilarde
"""

import pymysql
import psycopg2
from tqdm import tqdm
import pandas as pd  # Importante para definir o tipo de 'df' nos parâmetros

def create_conn(module, db_config):
    """
    Cria e retorna uma conexão e um cursor para o banco de dados especificado.
    
    Parâmetros:
    - module: Módulo do banco de dados (pymysql ou psycopg2).
    - db_config: Dicionário com as configurações do banco de dados (host, user, password, database).
    
    Retorna:
    - conn: Objeto de conexão ao banco de dados.
    - cur: Cursor para operações no banco de dados.
    """
    conn = module.connect(**db_config)
    cur = conn.cursor()
    return conn, cur

def sql_for_insert(cur, table, bd_type):
    """
    Gera a string SQL para inserção de dados, baseada no tipo do banco de dados e na tabela especificada.
    
    Parâmetros:
    - cur: Cursor do banco de dados.
    - table: Nome da tabela para inserção.
    - bd_type: Tipo do banco de dados ('mysql' ou 'postgres').
    
    Retorna:
    - SQL string para operação de INSERT.
    """
    if bd_type == 'mysql':
        cur.execute(f"DESCRIBE {table}")
    elif bd_type == 'postgres':
        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}' order by ordinal_position")
    else:
        raise ValueError(f"bd_type {bd_type} não configurado")
    
    col_names = [column[0] for column in cur.fetchall()]
    sql = "INSERT INTO {} ({}) VALUES ({})"
    colunas_sql = ', '.join(col_names)
    valores_sql = ', '.join(['%s'] * len(col_names))
    
    return sql.format(table, colunas_sql, valores_sql)

def batch_insert(df, module, bd_type, db_config, table, schema='public'):
    """
    Insere dados de um DataFrame em um banco de dados em lotes.
    
    Parâmetros:
    - df: DataFrame com os dados a serem inseridos.
    - module: Módulo do banco de dados (pymysql ou psycopg2).
    - bd_type: Tipo do banco de dados ('mysql' ou 'postgres').
    - db_config: Configurações do banco de dados.
    - table: Tabela do banco de dados
    - schema: Schema do banco de dados (opcional, principalmente para PostgreSQL).
    """
    conn, cur = create_conn(module, db_config)
    
    if bd_type == 'postgres':
        cur.execute(f"SET search_path TO {schema}")
    
    inser_sql = sql_for_insert(cur, table, bd_type)
    
    chunksize = 2000 # Processa em lotes de 1000
    
    with tqdm(total=len(df)) as pbar:
        for i in range(0, len(df), chunksize):  
            tuples = [tuple(x) for x in df.iloc[i:i + chunksize].itertuples(index=False)]
            cur.executemany(inser_sql, tuples)
            pbar.update(len(tuples))
    
    conn.commit()
    cur.close()
    conn.close()

# Exemplo de configuração do banco de dados
db_config = {
    "host": "host_do_banco",
    "user": "user_do_banco",
    "password": "pass_do_banco",
    "database": "database"
}


# Utilização
# df = pd.DataFrame(...)  # Seu DataFrame aqui
# table = 'sua_tabela' # Nome da tabela
# bd_type = 'mysql'  # Ou 'postgres'
# module = pymysql  # Ou psycopg2, dependendo do bd_type
# batch_insert(df, module, bd_type, db_config, table)