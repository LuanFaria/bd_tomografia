import os
import pandas as pd
from dotenv import load_dotenv
from createBdAgroMerge import CreateBdAgroMerge

load_dotenv(os.path.join(os.getcwd(), '.env'))


class BdAgroTomografia:
    """
    Class for managing BD_AGRO data related to tomography without database
    connection. It generates a JSON file with merged data.

    Parameters:
    - clients_folder (str): Path to the folder containing clients' data.
    - output_file (str, optional): Path to the output JSON file. Defaults to
      'merge_bd_agro.json'.
    - export_json_file (bool, optional): Flag indicating whether to export the
      merged data to a JSON file. Defaults to True.
    """
    def __init__(
            self,
            clients_folder: str,
            output_file: str = 'merge_bd_agro.json',
            export_json_file: bool = True) -> None:
        """
        Initialize the BdAgroTomografia object.

        Parameters:
        - clients_folder (str): Path to the folder containing clients' data.
        - output_file (str, optional): Path to the output JSON file. Defaults
          to 'merge_bd_agro.json'.
        - export_json_file (bool, optional): Flag indicating whether to export
          the merged data to a JSON file. Defaults to True.
        """
        self.output_file = output_file
        self.clients_folder = clients_folder
        self.export_json_file = export_json_file

        # Dummy data simulating client data; replace with actual loading logic if needed.
        self.clients_data = pd.DataFrame({
            "id": [1, 2],
            "nome": ["Client A", "Client B"],
            "grupo": ["Group 1", "Group 2"]
        })

        self.__create_json()

    def __create_json(self) -> None:
        """
        Generate JSON with merged BD_AGRO data.
        """
        bd_agro_data = self.__get_bd_agro_merged_clients_data()

        if self.export_json_file:
            bd_agro_data.to_json(self.output_file, orient="records", indent=4)

    def __get_bd_agro_merged_clients_data(self) -> pd.DataFrame:
        """
        Get merged BD_AGRO data for tomografia.

        Returns:
        - pd.DataFrame: Merged BD_AGRO data for tomografia.
        """
        data = CreateBdAgroMerge(
            clients_folder=self.clients_folder,
            output_file=self.output_file,
            clients_data=self.clients_data,
            export_json_file=self.export_json_file).merge_clients_bd_agro_data()

        return ImproveBdAgro(bd_agro_data=data).bd_data()


class ImproveBdAgro:
    """
    Class for improving the structure and data types of BD_AGRO data.

    Parameters:
    - bd_agro_data (pd.DataFrame): Original BD_AGRO data.
    """
    def __init__(self, bd_agro_data: pd.DataFrame) -> None:
        """
        Initialize the ImproveBdAgro object.

        Parameters:
        - bd_agro_data (pd.DataFrame): Original BD_AGRO data.
        """
        self.bd_agro_data = bd_agro_data

        self.data_types = {
            'client_id': 'integer', 'client_name': 'string', 'CHAVE': 'string',
            'SAFRA': 'integer', 'OBJETIVO': 'string', 'cliente': 'string',
            'TP_PROP': 'string', 'FAZENDA': 'string', 'SETOR': 'string',
            'SECAO': 'string', 'BLOCO': 'string', 'PIVO': 'string',
            'DESC_FAZ': 'string', 'TALHAO': 'string', 'VARIEDADE': 'string',
            'MATURACAO': 'string', 'AMBIENTE': 'string', 'ESTAGIO': 'string',
            'GRUPO_DASH': 'string', 'GRUPO_NDVI': 'string',
            'NMRO_CORTE': 'float', 'TAH': 'float', 'TPH': 'float',
            'DESC_CANA': 'string', 'AREA_BD': 'float', 'A_EST_MOAGEM': 'float',
            'A_COLHIDA': 'float', 'A_EST_MUDA': 'float', 'A_MUDA': 'float',
            'TCH_EST': 'float', 'TC_EST': 'float', 'TCH_REST': 'float',
            'TC_REST': 'float', 'TCH_REAL': 'float', 'TC_REAL': 'float',
            'DT_CORTE': 'date', 'DT_ULT_CORTE': 'date', 'DT_PLANTIO': 'date',
            'IDADE_CORTE': 'float', 'ATR': 'float', 'ATR_EST': 'float',
            'IRRIGACAO': 'string', 'grupo': 'string',
        }

    def bd_data(self) -> pd.DataFrame:
        """
        Improve the structure and data types of BD_AGRO data.

        Returns:
        - pd.DataFrame: Improved BD_AGRO data.
        """
        columns_interest = [
            'client_id', 'client_name', 'CHAVE', 'SAFRA',
            'OBJETIVO', 'TP_PROP', 'FAZENDA', 'SETOR', 'SECAO', 'BLOCO',
            'PIVO', 'DESC_FAZ', 'TALHAO', 'VARIEDADE', 'MATURACAO',
            'AMBIENTE', 'ESTAGIO', 'GRUPO_DASH', 'GRUPO_NDVI', 'NMRO_CORTE',
            'DESC_CANA', 'AREA_BD', 'A_EST_MOAGEM', 'A_COLHIDA', 'A_EST_MUDA',
            'A_MUDA', 'TCH_EST', 'TC_EST', 'TCH_REST', 'TC_REST', 'TCH_REAL',
            'TC_REAL', 'DT_CORTE', 'DT_ULT_CORTE', 'DT_PLANTIO', 'IDADE_CORTE',
            'ATR', 'ATR_EST', 'IRRIGACAO', 'TAH', 'TPH', 'grupo', 'cliente']

        self.bd_agro_data = self.bd_agro_data[columns_interest]

        # Convert all columns to lowercase
        self.bd_agro_data.columns = self.bd_agro_data.columns.str.lower()

        self.__apply_data_types()

        return self.bd_agro_data

    def __apply_data_types(self) -> None:
        """
        Apply specified data types to the columns in BD_AGRO data.
        """
        dict_formats_lower = {
            key.lower(): value for key, value in self.data_types.items()}

        for column, data_type in dict_formats_lower.items():
            if data_type == 'string':
                self.bd_agro_data[column] = self.bd_agro_data[
                    column].astype('string')

            elif data_type == 'integer':
                self.bd_agro_data[column] = pd.to_numeric(
                    self.bd_agro_data[column], errors='coerce').astype('Int64')

            elif data_type == 'float':
                self.bd_agro_data[column] = pd.to_numeric(
                    self.bd_agro_data[column],
                    errors='coerce').astype('float64')

            elif data_type == 'date':
                self.bd_agro_data[column] = pd.to_datetime(
                    self.bd_agro_data[column], errors='coerce')
