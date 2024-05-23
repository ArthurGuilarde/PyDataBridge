import os
import logging
import hashlib
import pymysql
import psycopg2
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder

# Configuração inicial de logging
HOJE = datetime.now()
logging.basicConfig(filename=f"{HOJE.strftime('%d_%m_%Y')}_log_file.log", format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s', level=logging.DEBUG)

class GerenciadorBancoDados:
    """
    Classe para gerenciar conexões e operações em bancos de dados MySQL e PostgreSQL.
    
    Args:
        database (str): Nome do banco de dados.
        table (str): Nome da tabela para operações.
        bd_type (str, opcional): Tipo do banco de dados, pode ser 'mysql' ou 'postgres'. Padrão é 'mysql'.
        schema (str, opcional): Esquema do banco de dados (aplicável para PostgreSQL). Padrão é 'public'.
        ssh_tunneling (bool, opcional): Indica se a conexão deve ser feita via túnel SSH. Padrão é False.
    """
    def __init__(self, database, table, bd_type='mysql', schema='public', ssh_tunneling=False):       
        self.host = os.getenv("DATABASE_URL")
        self.user = os.getenv("DATABASE_USER")
        self.__password = os.getenv("DATABASE_PASS")

        if bd_type not in ('mysql', 'postgres'):
            logging.error(f'bd_type={bd_type} | Tipo não suportado.')
            raise TypeError(f'bd_type={bd_type} | Tipo não suportado.')

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
        
        if bd_type == 'postgres':
            self.schema = schema
        else:
            self.schema = database
        
        self.database = database
        self.bd_type = bd_type
        self.__ssh_server = None
        self.ssh_server_con = None
        
        if ssh_tunneling:
            self.__ssh_server = SSHTunnelForwarder(
                (os.getenv("SSH_HOST"), 22),  # Endereço do servidor SSH e porta
                ssh_username=os.getenv("SSH_USER"),
                ssh_password=os.getenv("SSH_PASS"),
                remote_bind_address=('localhost', self.port)  # Endereço do servidor PostgreSQL e porta
            )
            self.__create_ssh_con()         
                 
        self.__db_config = {
            "host": self.host,
            "user": self.user,
            "port": self.port,
            "database": self.database,
            "password": self.__password 
        }
        
    @property
    def table(self):
        """
        Retorna o nome da tabela atual.
        
        Returns:
            str: Nome da tabela.
        """
        return self.__table
    
    @table.setter
    def table(self, table_name):
        """
        Define um novo nome para a tabela e atualiza as colunas.
        
        Args:
            table_name (str): Nome da nova tabela.
        
        Raises:
            ValueError: Se o nome da tabela não for uma string não vazia.
        """
        if not isinstance(table_name, str) or len(table_name) == 0:
            raise ValueError("Nome da tabela deve ser uma string não vazia.")
        
        old_name = self.__table
        self.__table = table_name
        logging.info(f'Table name changed from {old_name} -> {self.table}')
        self.__get_col_names()
        
    def __create_conn(self):
        """
        Cria e estabelece a conexão com o banco de dados baseado nas configurações fornecidas.
        """
        self.conn = self.module.connect(**self.__db_config)
        self.cur = self.conn.cursor()
        
    def __create_ssh_con(self):
        """
        Inicia a conexão via túnel SSH e ajusta a porta local.
        """
        self.__ssh_server.start()
        self.port = self.__ssh_server.local_bind_port
    
    def __dispose_ssh_con(self):
        """
        Encerra a conexão SSH.
        """
        self.__ssh_server.stop()
        
    def __get_col_names(self):
        """
        Recupera os nomes das colunas da tabela atual e os armazena em `self.col_names`.
        
        Raises:
            Exception: Se não for possível recuperar as colunas.
        """
        if self.bd_type in ('mysql', 'postgres'):
            try:
               self.cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{self.table}' ORDER BY ordinal_position")
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
        """
        Testa a conexão com o banco de dados e registra o resultado.
        """
        try:
            if (self.conn != None) and (self.cur != None):
                logging.warning('Test Connection | Connection already established')
            else:
                if self.__ssh_server:
                    self.__create_ssh_con()
                self.__create_conn()
                logging.info('Test Connection | Connection configured successfully')
                self.dispose()
        except Exception as e:
            print(e)
            logging.error(f'Test Connection failed {e}')
              
    def connect(self):
        """
        Estabelece a conexão com o banco de dados se não estiver previamente conectado.
        """
        try:
            if self.__ssh_server:
                self.__create_ssh_con()
            self.__create_conn()
            logging.info('Connection configured successfully')
            self.__get_col_names()
        except Exception as e:
            print(e)
            logging.error(f'Connection failed {e}')
            
    def dispose(self):
        """
        Encerra a conexão com o banco de dados e libera os recursos.
        """
        try:
            self.cur.close()
            self.conn.close()
            if self.__ssh_server:
                self.__dispose_ssh_con()
            logging.info('Connection closed \n\n')
        except Exception as e:
            print(e)
            logging.error(f'Connection close failed {e}')
    
    @staticmethod  
    def surrogated_hash(df, columns):
        """
        Gera um hash SHA-384 para cada linha de um DataFrame com base nos valores das colunas especificadas.
        
        Args:
            df (pd.DataFrame): DataFrame contendo os dados.
            columns (list): Lista de nomes de colunas a serem usadas para gerar o hash.
        
        Returns:
            pd.Series: Série contendo os hashes gerados.
        
        Raises:
            ValueError: Se `df` não for uma instância de pd.DataFrame ou `columns` não for uma lista de strings.
        """
        if not isinstance(df, pd.DataFrame):
            raise ValueError("O parâmetro 'df' deve ser uma instância de pd.DataFrame.")
        if not isinstance(columns, list):
            raise ValueError("O parâmetro 'columns' deve ser uma lista de nomes de colunas (strings).")
        elif not all(isinstance(col, str) for col in columns):
                raise ValueError("O parâmetro 'columns' deve ser uma lista de nomes de colunas (strings).")
        
        return df[columns].apply(lambda row: hashlib.sha384(''.join(map(str, row.values)).encode()).hexdigest(), axis=1)         
    
    def insert_sql(self):
        """
        Gera uma string SQL para inserção de dados na tabela atual.
        
        Returns:
            str: Comando SQL de inserção.
        """
        sql = "INSERT INTO {} ({}) VALUES ({})"
        colunas_sql = ', '.join(self.col_names)
        valores_sql = ', '.join(['%s'] * len(self.col_names))
        
        return sql.format(self.table, colunas_sql, valores_sql)
    
    def upsert_sql(self, excluded_cols=None):
        """
        Gera uma string SQL para inserção ou atualização (upsert) de dados na tabela atual.
        
        Args:
            excluded_cols (list, opcional): Lista de colunas a serem excluídas da atualização. Padrão é None.
        
        Returns:
            str: Comando SQL de upsert.
        
        Raises:
            ValueError: Se `excluded_cols` não for uma lista de strings.
        """
        if excluded_cols is not None:
            if not isinstance(excluded_cols, list):
                raise ValueError("O parâmetro 'excluded_cols' deve ser None ou uma lista de strings.")
            elif not all(isinstance(col, str) for col in excluded_cols):
                raise ValueError("O parâmetro 'excluded_cols' deve ser uma lista de strings.")
             
        if excluded_cols:
            update_cols = [col for col in self.col_names if col not in excluded_cols]
        else:
            update_cols = self.col_names
        
        placeholders = ', '.join(['%s'] * len(update_cols))
        col_names = ', '.join(update_cols)
        
        if self.bd_type == 'mysql':
            update_stmt = ', '.join([f"{col}=VALUES({col})" for col in update_cols])
            sql = f"INSERT INTO {self.table} ({col_names}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_stmt}"
        elif self.bd_type == 'postgres':
            update_stmt = ', '.join([f"{col}=EXCLUDED.{col}" for col in update_cols])
            key_col = self.col_names[0]  # Assumindo a primeira coluna como chave
            sql = f"INSERT INTO {self.table} ({col_names}) VALUES ({placeholders}) ON CONFLICT ({key_col}) DO UPDATE SET {update_stmt}"
        
        return sql
    
    def get_primary_key(self):
        """
        Recupera a chave primária da tabela atual.
        
        Returns:
            list: Lista contendo os nomes das colunas da chave primária.
        
        Raises:
            Exception: Se não for possível recuperar a chave primária.
        """
        try:
            logging.info(f'Buscando PKs de {self.schema}.{self.table} | DB type = {self.bd_type}')
            
            if self.bd_type == 'postgres':
                self.cur.execute(f"""    
                        SELECT
                            kcu.column_name
                        FROM
                            information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                        ON
                            tc.constraint_name = kcu.constraint_name
                            AND tc.table_schema = kcu.table_schema
                            AND tc.table_name = kcu.table_name
                        WHERE
                            tc.table_name = '{self.table}'
                            AND tc.table_schema = '{self.schema}'
                            AND tc.constraint_type = 'PRIMARY KEY';
                        """)
            elif self.bd_type == 'mysql':
                self.cur.execute(f"""
                        SELECT
                            k.column_name
                        FROM
                            information_schema.table_constraints t
                        JOIN information_schema.key_column_usage k
                        USING(constraint_name,
                            table_schema,
                            table_name)
                        WHERE
                            t.constraint_type = 'PRIMARY KEY'
                            AND t.table_schema = '{self.schema}'
                            AND t.table_name = '{self.table}';
                        """)
            else:
                raise Exception("DB type = {self.bd_type} não configurado!")
            
            pk_cols = self.cur.fetchone()
            logging.info(f'PK: {pk_cols}')
            
            return pk_cols
        
        except Exception as e:
            print(e)
            logging.error(f'Erro em get_primary_key | {e}')

    def select_paginado(self, col_list, batch_len, batch_offset):
        """
        Recupera um lote de registros da tabela atual, com base na lista de colunas e em um comprimento de lote e offset especificados.
        
        Args:
            col_list (list): Lista de colunas a serem selecionadas.
            batch_len (int): Número de registros a serem recuperados no lote.
            batch_offset (int): Offset para a seleção.
        
        Returns:
            list: Lista de tuplas contendo os registros selecionados.
        """
        col_names = ', '.join(col_list)
        
        sql = f"SELECT {col_names} FROM {self.schema}.{self.table} LIMIT {batch_len} OFFSET {batch_offset}"
        self.cur.execute(sql)
        results = self.cur.fetchall()
        
        return results
    
    def select_hash(self, col_list, pk, values_list, ultimo_registro=False):
        """
        Recupera registros da tabela atual com base em uma lista de valores de chave primária e uma lista de colunas,
        com uma opção para filtrar por registros mais recentes.
        
        Args:
            col_list (list): Lista de colunas a serem selecionadas.
            pk (str): Nome da coluna da chave primária.
            values_list (list): Lista de valores de chave primária para filtrar.
            ultimo_registro (bool, opcional): Se True, filtra apenas os registros mais recentes. Padrão é False.
        
        Returns:
            pd.DataFrame: DataFrame contendo os registros selecionados.
        """
        col_names = ', '.join(col_list)
        where_flag = "'{}'," * (len(values_list)-1)
        where_flag += "'{}'"
        where_values= where_flag.format(*values_list)
        
        sql = f"SELECT {col_names} FROM {self.schema}.{self.table} a WHERE a.{pk} IN ({where_values})"
        
        if ultimo_registro:
            sql += " AND ultimo_registro = 'True'"
        
        self.cur.execute(sql)
        results = self.cur.fetchall()
        
        return pd.DataFrame(results, columns=col_list)

    def batch_insert(self, df, sql):
        """
        Insere os dados do DataFrame em lotes na tabela atual usando o comando SQL fornecido.
        
        Args:
            df (pd.DataFrame): DataFrame contendo os dados a serem inseridos.
            sql (str): Comando SQL de inserção.
        """
        if self.bd_type == 'postgres':
            self.cur.execute(f"SET search_path TO {self.schema}")
        
        chunksize = min(len(df), 1000)  # Processa em lotes de 1000
        
        with tqdm(total=len(df)) as pbar:
            for i in range(0, len(df), chunksize):
                tuples = [tuple(x) for x in df.iloc[i:i + chunksize].itertuples(index=False)]
                self.cur.executemany(sql, tuples)
                pbar.update(len(tuples))
                
    def carga_dimensao(self, df_dimensao, col_dimensao, pk):
        """
        Realiza a carga de dados dimensionais na tabela atual, removendo registros antigos e inserindo novos registros.
        
        Args:
            df_dimensao (pd.DataFrame): DataFrame contendo os dados dimensionais.
            col_dimensao (list): Lista de colunas dimensionais.
            pk (str): Nome da coluna da chave primária.
        """
        aux = len(col_dimensao) + 2
        
        df_dimensao_dw = self.select_hash(col_dimensao, pk, df_dimensao['dhash'].values)
        df_dimensao_dw['dt_movimento'] = pd.to_datetime(df_dimensao_dw['dt_movimento'])
        
        data_atual = pd.to_datetime(df_dimensao['Data Movimento'].iloc[0])
        df_dimensao_dw['diferenca_dias'] = (data_atual - df_dimensao_dw['dt_movimento']).dt.days
        
        df_registros_antigos = df_dimensao_dw[df_dimensao_dw['diferenca_dias'] >= 30]
        
        if len(df_registros_antigos) > 0:
            sql = self.upsert_sql()
            self.batch_insert(df_registros_antigos, sql)
            self.conn.commit()
        
        df = pd.merge(df_dimensao, df_dimensao_dw, how='left', left_on='dhash', right_on=pk, indicator=True).loc[lambda x: x['_merge'] == 'left_only']
        df = df.iloc[:, :-aux]
        
        if len(df) > 0:
            sql = self.insert_sql()
            self.batch_insert(df, sql)
            self.conn.commit()

    def carga_scd_fato(self, df, col_list, pk, hash='scdhash'):
        """
        Realiza a carga de dados do tipo Slowly Changing Dimension (SCD) na tabela atual, atualizando registros existentes e inserindo novos.
        
        Args:
            df (pd.DataFrame): DataFrame contendo os dados.
            col_list (list): Lista de colunas a serem carregadas.
            pk (str): Nome da coluna da chave primária.
            hash (str, opcional): Nome da coluna de hash. Padrão é 'scdhash'.
        """
        aux = len(col_list) + 1
        df_dw = self.select_hash(col_list, pk, df[hash].values, ultimo_registro=True)

        df_desatualizados = pd.merge(df.iloc[4:], df_dw, how='right', left_on=hash, right_on=pk, indicator=True).loc[lambda x: x['_merge'] == 'right_only']
        df_desatualizados = df_desatualizados.iloc[:, -aux:-1]
        
        if len(df_desatualizados) > 0:
            mask = df_dw[pk].isin(df_desatualizados[pk])
            df_desatualizados = df_dw[mask]
            
            df_desatualizados.loc[:, 'ultimo_registro'] = False
            df_desatualizados.loc[:, 'dt_fim_movimento'] = pd.to_datetime(df['Data Movimento'].iloc[0])
            
            excluded_cols = [item for item in self.col_names if item not in df_desatualizados.columns.values]
            upsert_sql = self.upsert_sql(excluded_cols)
            self.batch_insert(df_desatualizados, upsert_sql)
            self.conn.commit()

        df_novos_registros = pd.merge(df, df_dw, how='left', left_on=hash, right_on=pk, indicator=True).loc[lambda x: x['_merge'] == 'left_only']
        df_novos_registros = df_novos_registros.iloc[:, :-aux]
        df_novos_registros.columns = df.columns
        
        if len(df_novos_registros) > 0:
            sql = self.insert_sql()
            self.batch_insert(df, sql)
            self.conn.commit()