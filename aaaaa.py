import warnings

import numpy as np
import pandas as pd

import psycopg2

from sqlalchemy.engine.base import Engine
from sqlalchemy import create_engine, types

warnings.filterwarnings("ignore")


class DataBase:
    """
    Class for interacting with a PostgreSQL database.

    Parameters:
    - host (str): Database host address.
    - port (str): Database port.
    - user (str): Database username.
    - database (str): Name of the database.
    - password (str): Database password.
    """
    def __init__(
            self,
            host: str,
            port: str,
            user: str,
            database: str,
            password: str) -> None:
        """
        Initialize the DataBase object.

        Parameters:
        - host (str): Database host address.
        - port (str): Database port.
        - user (str): Database username.
        - database (str): Name of the database.
        - password (str): Database password.
        """
        self.host = host
        self.port = port
        self.user = user
        self.database = database
        self.password = password

    def __connection(self) -> psycopg2.connect:
        """
        Establish a connection to the PostgreSQL database.

        Returns:
        - psycopg2.connect: Database connection object.
        """
        return psycopg2.connect(
            database=self.database,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
    def insert_tch_colheita_real(self, df):
        
        df['tc_est_colheita'] = np.where(df['tc_real'] > 0, df['tc_est'], 0)
        print(df)
        return df
        
    def create_table_and_insert(
            self,
            dataframe: pd.DataFrame,
            table_name: str,
            schema: str = 'public') -> None:
        """
        Create a table in the database and insert data from a DataFrame.
        If the table already exists, it will be replaced by the new one
        referring to the dataframe

        Parameters:
        - dataframe (pd.DataFrame): DataFrame containing data to be inserted.
        - table_name (str): Name of the table to be created.
        - schema (str, optional): Database schema. Defaults to 'public'.
        """
        dataframe = self.insert_tch_colheita_real(dataframe)
        print(f'\nCriando tabela "{table_name}" '
              f'e inserindo os dados do dataframe...')
        dataframe.to_sql(
            table_name,
            self.__engine(),
            schema=schema,
            index=False,
            if_exists='replace',
            dtype=self.__get_type(dataframe)
        )

        print('Insert realizado com sucesso!')
        self.__engine().dispose()

    @staticmethod
    def __get_type(dataframe: pd.DataFrame) -> pd.DataFrame.dtypes:
        """
        Get the data types for each column in the DataFrame.

        Parameters:
        - dataframe (pd.DataFrame): DataFrame to get data types from.

        Returns:
        - pd.DataFrame.dtypes: Data types for each column.
        """
        return {
            col: types.VARCHAR(dataframe[col].str.len().max())
            for col in dataframe.columns
            if dataframe[col].dtype == 'O'
        }

    def __engine(self) -> Engine:
        """
        Create a SQLAlchemy engine for database operations.

        Returns:
        - sqlalchemy.engine.base.Engine: SQLAlchemy engine.
        """
        return create_engine(
            f'postgresql://'
            f'{self.user}:'
            f'{self.password}@'
            f'{self.host}:'
            f'{self.port}/'
            f'{self.database}')

    def drop_table(self, table_name: str) -> None:
        """
        Drop a table from the database.

        Parameters:
        - table_name (str): Name of the table to be dropped.
        """
        print(f'\nDeleting table "{table_name}"...')

        connection = self.__connection()

        cursor = connection.cursor()

        sql = f'''DROP TABLE "{table_name}"'''

        # Executing the query
        cursor.execute(sql)

        # Commit your changes in the database
        connection.commit()

        # Closing the connection
        self.close_connection(connection)

        print(f'---Table "{table_name}" deleted!---\n')

    @staticmethod
    def close_connection(connection: psycopg2.connect) -> None:
        """
        Close the database connection.

        Parameters:
        - connection (psycopg2.connect): Database connection object.
        """
        if connection is not None:
            connection.close()

    def get_data_from_table(
            self,
            table: str,
            schema: str = 'public',
            query: str = '') -> pd.DataFrame:
        """
        Retrieve data from a table in the database.

        Parameters:
        - table (str): Name of the table to retrieve data from.
        - schema (str, optional): Database schema. Defaults to 'public'.

        Returns:
        - pd.DataFrame: DataFrame containing the retrieved data.
        """
        if not query:
            query = f"SELECT * FROM {schema}.{table}"

        connection = self.__connection()
        try:
            data = pd.read_sql_query(query, con=connection)
            self.close_connection(connection)
            return data
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.close_connection(connection)
