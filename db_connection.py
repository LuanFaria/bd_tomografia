import psycopg2
from psycopg2.extras import execute_values

class DatabaseManager:
    """
    Classe para gerenciar a conexão e operações no banco de dados PostgreSQL.
    """

    def __init__(self, config):
        """
        Inicializa a classe DatabaseManager.

        Parameters:
        - config (dict): Configuração do banco de dados (dbname, user, password, host, port).
        """
        self.config = config

    def connect(self):
        """
        Conecta ao banco de dados PostgreSQL.
        """
        try:
            conn = psycopg2.connect(**self.config)
            return conn
        except Exception as e:
            print(f"Erro ao conectar ao banco de dados: {e}")
            raise

    def delete_rows_by_client_ids(self, schema, table, client_ids):
        """
        Exclui linhas na tabela que correspondem aos IDs fornecidos.

        Parameters:
        - schema (str): Schema do banco de dados.
        - table (str): Nome da tabela.
        - client_ids (list): Lista de IDs de clientes a serem excluídos.
        """
        query = f"DELETE FROM {schema}.{table} WHERE client_id = ANY(%s)"
        try:
            conn = self.connect()
            with conn.cursor() as cursor:
                cursor.execute(query, (client_ids,))
                print(f"{cursor.rowcount} linhas excluídas da tabela {table}.")
            conn.commit()
        except Exception as e:
            print(f"Erro ao excluir linhas: {e}")
        finally:
            conn.close()
    
    def insert_data(self, schema, table, data_frame, data_types):
        """
        Insere dados no banco a partir de um DataFrame.

        Parameters:
            - schema (str): Schema do banco de dados.
            - table (str): Nome da tabela.
            - data_frame (pd.DataFrame): Dados a serem inseridos.
            - data_types (dict): Dicionário de tipos de dados para validação.
        """
        try:
            # Certifique-se de que o DataFrame contém todas as colunas necessárias
            required_columns = list(data_types.keys())
            for column in required_columns:
                if column not in data_frame.columns:
                    data_frame[column] = None  # Adicione a coluna ausente com valores padrão

            conn = self.connect()
            with conn.cursor() as cursor:
                columns = required_columns
                values = data_frame[columns].fillna(None).to_dict(orient="records")
                
                # Gera a query dinâmica para inserção
                columns_str = ", ".join(columns)
                placeholders = ", ".join(["%s"] * len(columns))
                query = f"INSERT INTO {schema}.{table} ({columns_str}) VALUES ({placeholders})"
                
                # Insere os valores
                execute_values(cursor, query, [tuple(row.values()) for row in values])
                print(f"{cursor.rowcount} linhas inseridas na tabela {table}.")
            
            conn.commit()
        except Exception as e:
            print(f"Erro ao inserir dados: {e}")
        finally:
            conn.close()

