# -*- coding: utf-8 -*-
"""
Módulo DataBaseEngine: Facilita a conexão e operações com bancos de dados MySQL e PostgreSQL.

Este módulo define a classe DataBaseEngine, que abstrai a conexão e operações básicas com bancos de dados,
permitindo inserções em lote de dados provenientes de DataFrames. A classe suporta ambientes de desenvolvimento,
homologação e produção, ajustando-se automaticamente às configurações de conexão armazenadas nas variáveis 
de ambiente do Airflow.

Criado em: Ter Jan 30 16:11:34 2024
Autor: Arthur Guilarde

Requerimentos:
- pymysql
- psycopg2
- tqdm
- airflow

Exemplo de uso:
    from database_engine import DataBaseEngine
    import pandas as pd

    # DataFrame exemplo
    df = pd.DataFrame({'coluna1': [1, 2], 'coluna2': ['A', 'B']})

    # Instanciando e utilizando a classe para inserir dados no MySQL
    engine = DataBaseEngine(bd_type='mysql', database='nome_do_banco', environment='dev', schema='public')
    engine.batch_insert(df)

"""

import pymysql
import psycopg2
from tqdm import tqdm
from airflow.models import Variable
           
class DataBaseEngine:
    """
    Classe DataBaseEngine para conexão e operações em bancos de dados.
    
    A classe suporta operações em bancos de dados MySQL e PostgreSQL, permitindo a inserção em lote
    de dados a partir de DataFrames do pandas. As configurações de conexão são obtidas a partir de 
    variáveis de ambiente definidas no Airflow, suportando diferentes ambientes como desenvolvimento,
    homologação e produção.
    
    Atributos:
        bd_type (str): Tipo do banco de dados ('mysql' ou 'postgres').
        database (str): Nome do banco de dados.
        environment (str): Ambiente de execução ('dev', 'homo', 'prod').
        schema (str): Esquema do banco de dados, padrão 'public' para PostgreSQL.
        
    Métodos:
        batch_insert(df, batch): Realiza a inserção em lote dos dados contidos em um DataFrame.
    """
    
    def __init__(self, database='none', environment='none', bd_type='mysql', schema='public'):
        """
        Inicializa uma instância da DataBaseEngine com as configurações de conexão especificadas.
        
        Parâmetros:
            bd_type (str): Tipo do banco de dados ('mysql' ou 'postgres').
            database (str): Nome do banco de dados (Shema caso Postgres).
            environment (str): Ambiente de execução ('dev', 'homo', 'prod').
            schema (str): Esquema do banco de dados, padrão 'public' para PostgreSQL.
        """
        
        if environment == 'dev':
            self.user = Variable.get("DEV_USER")
            self.host = Variable.get("DEV_URL")
            password = Variable.get("DEV_PASSWORD")
            
        elif environment == 'homo':
            self.user = Variable.get("HOMO_USER", "none")
            self.host = Variable.get("HOMO_URL", "none")
            password = Variable.get("HOMO_PASSWORD", "none")  
            
        elif environment == 'prod':
            self.user = Variable.get("PROD_USER")
            self.host = Variable.get("PROD_URL")
            password = Variable.get("PROD_PASSWORD")      
            
        
        if bd_type == 'postgres':
            self.port = 5432
            self.module = psycopg2
        else:
            self.port = 3306
            self.module = pymysql
        
        
        self.table = ''
        self.conn = ''
        self.cur = ''
        self.environment = environment
        self.schema = schema
        self.database = database
        
        self.db_config = {
            "host": self.host,
            "user": self.user,
            "port": self.port,
            "database": self.database,
            "password": password
            
        }
    

    def connection(self):
        """
        Estabelece uma conexão com o banco de dados e cria um cursor.
        
        Este método tenta estabelecer uma conexão com o banco de dados usando as configurações
        fornecidas durante a inicialização da instância da classe DataBaseEngine. Se a conexão
        for bem-sucedida, um cursor é criado e armazenado junto com o objeto de conexão nos
        atributos da instância para uso posterior em operações de banco de dados.
        
        Em caso de falha ao tentar estabelecer a conexão, a exceção capturada é tratada,
        e a mensagem de erro é impressa no console.
        
        Uso:
            Após a inicialização da instância da classe, este método pode ser chamado para
            iniciar uma conexão antes de realizar operações de banco de dados, como inserções
            em lote com o método `batch_insert`.
        
        Exemplo:
            engine = DataBaseEngine(bd_type='mysql', database='nome_do_banco', environment='dev')
            engine.connection()  # Estabelece a conexão
        """
        try:
            self.conn, self.cur = self.__create_conn()
        except Exception as e:
            print(e)

    def dispose(self):
        """
        Fecha a conexão com o banco de dados e o cursor associado.
        
        Este método é responsável por liberar os recursos alocados durante a conexão com o banco
        de dados. Ele tenta fechar o cursor e a conexão de forma segura. Se ocorrer algum erro
        durante o processo de fechamento, a exceção capturada é tratada, e a mensagem de erro
        é impressa no console.
        
        Uso:
            Deve ser chamado após a conclusão de todas as operações de banco de dados necessárias,
            garantindo o fechamento apropriado da conexão e a liberação dos recursos.
        
        Exemplo:
            engine.dispose()  # Fecha a conexão e o cursor
        """
        try:
            self.cur.close()
            self.conn.close()  
        except Exception as e:
            print(e)
   
            
    def __create_conn(self):
        """
        Cria e retorna uma conexão e um cursor para o banco de dados especificado.
        
        Este método é privado e deve ser utilizado apenas internamente pela classe.
        
        Retorna:
            Tuple: Contendo o objeto de conexão e o cursor associado.
        """
        
        conn = self.module.connect(**self.db_config)
        cur = conn.cursor()
        
        return conn, cur


    def __sql_for_insert(self):
        """
        Gera a string SQL para inserção de dados, baseada no tipo do banco de dados e na tabela especificada.
        
        Este método é privado e deve ser utilizado apenas internamente pela classe para preparar a 
        string SQL de inserção.
        
        Retorna:
            str: String SQL formatada para operação de INSERT.
        """
        
        if self.bd_type == 'mysql':
            self.cur.execute(f"DESCRIBE {self.table}")
        elif self.bd_type == 'postgres':
            self.cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{self.table}' order by ordinal_position")
        else:
            raise ValueError(f"bd_type {self.bd_type} não configurado")
        
        col_names = [column[0] for column in self.cur.fetchall()]
        sql = "INSERT INTO {} ({}) VALUES ({})"
        colunas_sql = ', '.join(col_names)
        valores_sql = ', '.join(['%s'] * len(col_names))
        
        return sql.format(self.table, colunas_sql, valores_sql)


    def batch_insert(self, df, batch=0):
        """
        Insere dados de um DataFrame em um banco de dados em lotes.
        
        Este método utiliza uma barra de progresso (tqdm) para indicar o progresso da inserção em lote.
        
        Parâmetros:
            df (DataFrame): DataFrame com os dados a serem inseridos.
        """
        
        self.conn, self.cur = self.__create_conn(self)
        
        if self.bd_type == 'postgres':
            self.cur.execute(f"SET search_path TO {self.schema}")
        
        inser_sql = self.__sql_for_insert()
        
        if batch == 0:
            chunksize = max(int(len(df)/100), 1) # Processa em lotes de 1000
        else:
            chunksize = batch
        
        with tqdm(total=len(df)) as pbar:
            for i in range(0, len(df), chunksize):  
                tuples = [tuple(x) for x in df.iloc[i:i + chunksize].itertuples(index=False)]
                self.cur.executemany(inser_sql, tuples)
                pbar.update(len(tuples))
        
        self.conn.commit()