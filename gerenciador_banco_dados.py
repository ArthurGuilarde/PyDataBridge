"""
Este módulo é responsável pela gestão de conexões com bancos de dados MySQL e PostgreSQL. 
Ele permite a conexão, teste e encerramento de conexões com os bancos de dados, facilitando operações de CRUD.
As conexões com o banco de dados são configuradas através de variáveis de ambiente.

Dependências:
- os: Para acessar variáveis de ambiente.
- logging: Para registrar logs de operações e erros.
- hashlib, pymysql, psycopg2: Para operações relacionadas ao banco de dados.
- pandas: Para manipulação de dados.
- datetime: Para registrar a data e hora dos logs.
- tqdm: usado em iterações com barra de progresso.
- dotenv: Para carregar variáveis de ambiente de um arquivo .env.

"""

import os
import logging
import hashlib
import pymysql
import psycopg2
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from dotenv import load_dotenv

# Configuração inicial de logging
HOJE = datetime.now()
logging.basicConfig(filename=f"{HOJE.strftime('%d_%m_%Y')}_log_file.log", format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s', level=logging.DEBUG)


class GerenciadorBancoDados:
    """
    Classe responsável pela gestão de conexões com bancos de dados.
    
    Através desta classe, é possível conectar-se a bancos de dados MySQL e PostgreSQL,
    testar a conexão e encerrar a conexão de forma segura.
    
    Atributos:
        host (str): URL do host do banco de dados, obtido de variáveis de ambiente.
        user (str): Nome do usuário do banco de dados, obtido de variáveis de ambiente.
        __password (str): Senha do usuário do banco de dados, obtida de variáveis de ambiente.
        port (int): Porta de conexão com o banco de dados.
        module: Módulo de conexão com o banco de dados (pymysql ou psycopg2).
        table (str): Nome da tabela a ser utilizada (não implementado neste exemplo).
        conn: Objeto de conexão com o banco de dados.
        cur: Cursor para execução de comandos SQL.
        schema (str): Esquema do banco de dados para PostgreSQL.
        database (str): Nome do banco de dados.
        bd_type (str): Tipo do banco de dados ('mysql' ou 'postgres').
        __db_config (dict): Configuração da conexão com o banco de dados.
    
    Métodos:
        __create_conn(): Cria uma conexão com o banco de dados.
        conection_test(): Testa a conexão com o banco de dados.
        connect(): Configura e estabelece a conexão com o banco de dados.
        dispose(): Encerra a conexão com o banco de dados de forma segura.
    """
    def __init__(self, database, table, bd_type='mysql', schema='public'):       
        """
        Inicializador da classe GerenciadorBancoDados.
        
        Parâmetros:
            database (str): O nome do banco de dados a se conectar é um parâmetro obrigatório.
            table (str): O nome da tabela a se conectar é um parâmetro obrigatório.
            bd_type (str): Tipo do banco de dados ('mysql' ou 'postgres'), com 'mysql' como valor padrão.
            schema (str): Esquema do banco de dados para uso com PostgreSQL, 'public' por padrão.
        """
        self.host = os.getenv("DATABASE_URL")
        self.user = os.getenv("DATABASE_USER")
        self.__password = os.getenv("DATABASE_PASS")      

        if bd_type == 'postgres':
            self.port = 5432
            self.module = psycopg2
        else:
            self.port = 3306
            self.module = pymysql
        
        
        self.conn = None
        self.cur = None
        self.col_names = None
        
        self.__table = table
        self.schema = schema
        self.database = database
        self.bd_type = bd_type
        
        self.__db_config = {
            "host": self.host,
            "user": self.user,
            "port": self.port,
            "database": self.database,
            "password": self.__password 
        }
    
    @property
    def table(self):
        return self.__table
    
    @table.setter
    def table(self, table_name):
        
        if not isinstance(table_name, str) or len(table_name) == 0:
            raise ValueError("Nome da tabela deve ser uma string não vazia.")
        
        old_name = self.__table
        self.__table = table_name
        logging.info(f'Table name change from {old_name} -> {self.table}')
        self.__get_col_names()
        
    def __create_conn(self):      
        # Cria e estabelece a conexão com o banco de dados baseado nas configurações fornecidas.
        self.conn = self.module.connect(**self.__db_config)
        self.cur = self.conn.cursor()
        
    def __get_col_names (self):
        if self.bd_type == 'mysql':
            try:
                self.cur.execute(f"DESCRIBE {self.table}")
            except Exception as e:
                print(e)
                return logging.error(f'Erro ao recuperar colunas da tabela {self.table} | {e}') 
        elif self.bd_type == 'postgres':
            try:
               self.cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{self.table}' order by ordinal_position")
            except Exception as e:
                print(e)
                return logging.error(f'Erro ao recuperar colunas da tabela {self.table} | {e}')
        else:
            logging.warning(f'Banco de dados não configurado em bd_type | {self.bd_type}')
            raise ValueError(f"bd_type {self.bd_type} não configurado")
        
        col_names = [column[0] for column in self.cur.fetchall()]
        
        if len(col_names) == 0:
            logging.error('Verifique as permissões do usuário/database.')
            raise Exception("Colunas não encontradas. Verifique as permissões do usuário/database.")

        self.col_names = col_names
        
        logging.info(f'Colunas selecionadas em {self.table} | {self.col_names}')  
                
    def conection_test(self):
        # Testa a conexão com o banco de dados e registra o resultado.
        try:
        # Verificar se a variavel conn e cur são diferentes de None
            if (self.conn != None) and (self.cur != None):
                logging.warning('Test Connection | Connection already established')
            else:
                self.__create_conn()
                logging.info('Test Connection | Connection configured successfully')
                self.dispose()
        except Exception as e:
            print(e)
            logging.error(f'Test Connection failed {e}')
              
    def connect(self):
        # Estabelece a conexão com o banco de dados se não estiver previamente conectado.
        try:
            self.__create_conn()
            logging.info('Connection configured successfully')
            self.__get_col_names()
        except Exception as e:
            print(e)
            logging.error(f'Connection failed {e}')
            
    def dispose(self):
        # Encerra a conexão com o banco de dados e libera os recursos.
        try:
            self.cur.close()
            self.conn.close()
            logging.info('Connection closed \n\n')
        except Exception as e:
            print(e)
            logging.error(f'Connection close failed {e}')
    
    @staticmethod  
    def surrogated_hash(df, columns):
        """
        Gera um hash SHA-384 como identificador surrogado para cada linha de um DataFrame,
        com base nos valores de colunas especificadas.

        A função concatena os valores das colunas especificadas de cada linha, converte a concatenação
        resultante em uma string (se necessário), e aplica o algoritmo de hash SHA-384 para gerar um hash único.

        Parâmetros:
            - df (pd.DataFrame): O DataFrame do pandas contendo os dados a serem processados.
            - columns (list of str): Uma lista contendo os nomes das colunas cujos valores serão usados para gerar o hash.
        
        Retorna:
            pd.Series: Uma série do pandas contendo os hashes SHA-384 gerados para cada linha do DataFrame fornecido.

        Exceções:
            ValueError: Se 'df' não for uma instância de pd.DataFrame ou se 'columns' não for uma lista de strings.

        Exemplo de uso:
            df = pd.DataFrame({'nome': ['Alice', 'Bob', 'Charlie'], 'idade': [25, 30, 35]})
            >>> print(surrogated_hash(df, ['nome', 'idade']))
            0    <hash1>
            1    <hash2>
            2    <hash3>
            dtype: object
        """
        # Teste de tipagem para os parâmetros
        if not isinstance(df, pd.DataFrame):
            raise ValueError("O parâmetro 'df' deve ser uma instância de pd.DataFrame.")
        if not isinstance(columns, list):
            raise ValueError("O parâmetro 'columns' deve ser uma lista de nomes de colunas (strings).")
        elif not all(isinstance(col, str) for col in columns):
                raise ValueError("O parâmetro 'columns' deve ser uma lista de nomes de colunas (strings).")
            
        
        # Geração do hash SHA-384 para cada linha com base nos valores das colunas especificadas
        return df[columns].apply(lambda row: hashlib.sha384(''.join(map(str, row.values)).encode()).hexdigest(), axis=1)         
    
    def insert_sql(self):
        """
        Gera uma string SQL para inserção de dados na tabela especificada.

        Este método constrói uma string SQL de inserção com base nos nomes das colunas e na tabela definidos na instância, utilizando placeholders (%s) para os valores, adequados para a utilização com parâmetros de consulta para evitar SQL Injection.

        Atributos de Instância Esperados:
        - self.table (str): O nome da tabela no banco de dados onde os dados serão inseridos.
        - self.col_names (list): Uma lista contendo os nomes das colunas da tabela correspondente aos valores a serem inseridos.

        Retorna:
            str: Uma string SQL que pode ser utilizada em uma operação de inserção com um cursor de banco de dados, onde os valores reais devem ser fornecidos separadamente para evitar SQL Injection.

        Exemplo:
            Se `self.table` for 'usuarios' e `self.col_names` for ['nome', 'email'], o método retornará:
            "INSERT INTO usuarios (nome, email) VALUES (%s, %s)"

        Nota:
            Este método não executa a operação de inserção no banco de dados. Ele apenas gera a string SQL. A execução da query com os valores reais deve ser feita separadamente, utilizando um cursor de banco de dados e passando os valores como parâmetros.
        """            
        sql = "INSERT INTO {} ({}) VALUES ({})"
        colunas_sql = ', '.join(self.col_names)
        valores_sql = ', '.join(['%s'] * len(self.col_names))
        
        return sql.format(self.table, colunas_sql, valores_sql)
    
    def upsert_sql (self, excluded_cols=None):
        """
        Gera uma string SQL para realizar uma operação de "upsert" na tabela especificada.
        
        Um "upsert" é uma operação que insere novas linhas na tabela se não existirem, ou atualiza as linhas existentes se a chave primária ou uma constraint única for violada. Este método suporta a geração de comandos SQL de "upsert" para MySQL e PostgreSQL.

        Parâmetros:
        - excluded_cols (list, opcional): Uma lista de strings representando os nomes das colunas que devem ser excluídas da parte de atualização do comando "upsert". Se None, todas as colunas, exceto a chave primária (primeira coluna), serão incluídas na atualização. Padrão é None.

        Atributos de Instância Esperados:
        - self.col_names (list): Uma lista contendo os nomes das colunas da tabela.
        - self.table (str): O nome da tabela no banco de dados onde a operação "upsert" será realizada.
        - self.bd_type (str): O tipo do banco de dados, usado para determinar a sintaxe específica do SQL. Deve ser 'mysql' ou 'postgres'.

        Retorna:
            str: Uma string SQL para realizar a operação de "upsert" na tabela especificada.

        Raise:
            ValueError: Se 'excluded_cols' não for None ou uma lista de strings.

        Exemplos:
            Para uma tabela 'usuarios' com colunas ['id', 'nome', 'email'] e bd_type 'mysql', o método chamado sem 'excluded_cols' retornará:
            "INSERT INTO usuarios (id, nome, email) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE nome=VALUES(nome), email=VALUES(email)"

            Para o mesmo exemplo com bd_type 'postgres' e 'excluded_cols'=['email'], retornará:
            "INSERT INTO usuarios (id, nome, email) VALUES (%s, %s, %s) ON CONFLICT (id) DO UPDATE SET nome=EXCLUDED.nome"

        Nota:
            Este método não executa a operação de "upsert" no banco de dados. Ele apenas gera a string SQL. A execução da query com os valores reais deve ser feita separadamente, utilizando um cursor de banco de dados e passando os valores como parâmetros.
        """
        # Verifica se os parâmetros são do tipo correto
        if excluded_cols is not None:
            if not isinstance(excluded_cols, list):
                raise ValueError("O parâmetro 'excluded_cols' deve ser None ou uma lista de strings.")
            elif not all(isinstance(col, str) for col in excluded_cols):
                raise ValueError("O parâmetro 'excluded_cols' deve ser uma lista de strings.")
             
        if excluded_cols:
            update_cols = [col for col in self.col_names if col not in excluded_cols]
        else:
            update_cols = self.col_names[1:]  # Exclui a primeira coluna assumindo ser a chave primária
        
        placeholders = ', '.join(['%s'] * len(self.col_names))
        col_names = ', '.join(self.col_names)
        
        if self.bd_type == 'mysql':
            update_stmt = ', '.join([f"{col}=VALUES({col})" for col in update_cols])
            sql = f"INSERT INTO {self.table} ({col_names}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_stmt}"
        elif self.bd_type == 'postgres':
            update_stmt = ', '.join([f"{col}=EXCLUDED.{col}" for col in update_cols])
            key_col = self.col_names[0]  # Assumindo a primeira coluna como chave
            sql = f"INSERT INTO {self.table} ({col_names}) VALUES ({placeholders}) ON CONFLICT ({key_col}) DO UPDATE SET {update_stmt}"
        
        return sql
    
meu_obj = GerenciadorBancoDados('credito', 'f_contratos_temp')

meu_obj.connect()
sql_insert = meu_obj.insert_sql()
sql_upsert = meu_obj.upsert_sql()
meu_obj.table = "f_contratos"
meu_obj.dispose()