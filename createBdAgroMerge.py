import os
import pandas as pd

class CreateBdAgroMerge:
    """
    Class for merging and exporting selected clients' BD_AGRO data.
    """

    def __init__(self, output_file: str, clients_folder: str, export_json_file: bool, selected_client_ids: list[int]) -> None:
        """
        Initialize the CreateBdAgroMerge object.

        Parameters:
        - output_file (str): Path to the output JSON file.
        - clients_folder (str): Path to the folder containing clients' data.
        - export_json_file (bool): Flag indicating whether to export the merged data to a JSON file.
        - selected_client_ids (list[int]): List of client IDs to include in the output.
        """
        self.output_file = output_file
        self.clients_folder = clients_folder
        self.export_json_file = export_json_file
        self.selected_client_ids = selected_client_ids

        self.list_clients_to_remove = [
            '98', '99', '126', 
            '127', '133', '134', 
            '137', '139', '140', 
            '141', '148', '149', 
            '150', '151', '152', 
            '154', '155', '999']

    def merge_clients_bd_agro_data(self) -> pd.DataFrame:
        """
        Merge clients' BD_AGRO data for selected client IDs.

        Returns:
        - pd.DataFrame: Merged BD_AGRO data for selected clients.
        """
        merged_bd_agro = pd.DataFrame()

        clients_bd_agro_file = self.__get_all_clients_bd_agro()

        for client_bd_agro in clients_bd_agro_file:
            client_id = int(client_bd_agro['client_id'])

            # Only process selected client IDs
            if client_id not in self.selected_client_ids:
                continue

            client_name = str(client_bd_agro['client_name'])
            print(f'Abrindo BD_AGRO do cliente: {client_name}')
            bd_agro = pd.read_excel(client_bd_agro['bd_agro_file'])

            bd_agro['client_id'] = client_id
            bd_agro['client_name'] = client_name

            merged_bd_agro = pd.concat(
                [merged_bd_agro, bd_agro], ignore_index=True)

        if self.export_json_file:
            print("\nGerando arquivo JSON...")
            merged_bd_agro.to_json(self.output_file, orient='records', lines=True)
            print(f"\n----- JSON exportado para: {self.output_file} -----")

        return merged_bd_agro

    def __get_all_clients_bd_agro(self) -> list[dict[str, str]]:
        """
        Get information about all clients' BD_AGRO files.

        Returns:
        - list[dict[str, str]]: List of dictionaries containing information about BD_AGRO files.
        """
        clients_bd_agro_file = []

        clients_bd_agro_folder = self.__get_all_clients_bd_agro_folder()

        for client_bd_agro_folder in clients_bd_agro_folder:
            bd_agro_file = self.__find_bd_agro_file(client_bd_agro_folder)

            if bd_agro_file:
                clients_bd_agro_file.append(bd_agro_file)

        return clients_bd_agro_file

    def __find_bd_agro_file(self, client_bd_agro_folder: str) -> dict[str, str]:
        """
        Find BD_AGRO file in the given client's folder.

        Parameters:
        - client_bd_agro_folder (str): Path to the client's BD_AGRO folder.

        Returns:
        - dict[str, str]: Dictionary containing information about the BD_AGRO file.
        """
        folder_name = os.path.basename(client_bd_agro_folder)
        client_id = int(folder_name.split('_')[0])  # Extract ID from folder name
        client_name = folder_name.split('_')[1]  # Extract name from folder name

        bd_agro_path = os.path.join(client_bd_agro_folder, "2_bd_agro")
        if not os.path.exists(bd_agro_path):
            print(f'Pasta "2_bd_agro" não encontrada para o cliente: {client_name}')
            return {}

        for file in os.listdir(bd_agro_path):
            if file.startswith('BD_AGRO_') and file.endswith('.xlsx'):
                return {
                    "client_id": client_id,
                    "client_name": client_name,
                    "bd_agro_file": os.path.join(bd_agro_path, file)
                }

        print(f'Arquivo BD_AGRO do cliente não foi encontrado: {client_name}')
        return {}

    def __get_all_clients_bd_agro_folder(self) -> list[str]:
        """
        Get a list of paths to all clients' BD_AGRO folders.

        Returns:
        - list[str]: List of paths to BD_AGRO folders.
        """
        return [os.path.join(self.clients_folder, client_folder)
                for client_folder in os.listdir(self.clients_folder)
                if client_folder.split('_')[0].isdigit()  # Ensure the folder starts with a number (ID)
                and client_folder.split('_')[0] not in self.list_clients_to_remove]
