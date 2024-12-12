from createBdAgroMerge import CreateBdAgroMerge
from db_connection import DatabaseManager
import pandas as pd

def add_group_to_json(db_manager, json_data, client_ids):
    """
    Adiciona o nome do grupo ao JSON com base nos client_ids.
    
    Parameters:
    - db_manager (DatabaseManager): Gerenciador de banco de dados.
    - json_data (pd.DataFrame): Dados do JSON como DataFrame.
    - client_ids (list): IDs dos clientes selecionados.
    
    Returns:
    - pd.DataFrame: JSON atualizado com a coluna 'grupo'.
    """
    conn = db_manager.connect()
    try:
        with conn.cursor() as cursor:
            # Obtem o grupo_id para os client_ids selecionados
            query_client_group = """
            SELECT c.id AS client_id, g.nome AS grupo_nome
            FROM clientes c
            INNER JOIN cliente_grupo g ON c.grupo_id = g.id
            WHERE c.id = ANY(%s)
            """
            cursor.execute(query_client_group, (client_ids,))
            group_data = cursor.fetchall()

            # Mapeia client_id para grupo_nome
            group_map = {row[0]: row[1] for row in group_data}

            # Adiciona a coluna 'grupo' ao JSON
            json_data['grupo'] = json_data['client_id'].map(group_map)

            # Preenche com "Sem Grupo" onde não há mapeamento
            json_data['grupo'] = json_data['grupo'].fillna("Sem Grupo")
            
    except Exception as e:
        print(f"Erro ao adicionar grupo ao JSON: {e}")
    finally:
        conn.close()
    
    return json_data

if __name__ == "__main__":
    clients_folder = "C:/TOMOGRAFIA"
    output_file = "C:/Users/luan.faria/Desktop/cod_luan/cod/SIGMA/cod/codigo_banco_tomo/output.json"
    
    selected_client_ids = [111]  # IDs dos clientes que você quer selecionar

    # Configuração e acesso ao banco de dados
    db_config = {
        "dbname": "postgis_34_sample",
        "user": "postgres",
        "password": "postgres",
        "host": "localhost",
        "port": "5432"
    }

    db_manager = DatabaseManager(db_config)

    # Excluir linhas com os IDs fornecidos na tabela bd_tomografia
    db_manager.delete_rows_by_client_ids("public", "bd_tomografia", selected_client_ids)

    # Gerar o JSON e carregar os dados
    merger = CreateBdAgroMerge(
        output_file=output_file,
        clients_folder=clients_folder,
        export_json_file=True,
        selected_client_ids=selected_client_ids
    )
    merged_data = merger.merge_clients_bd_agro_data()
    print(merged_data)  
    # Adicionar a coluna 'grupo' ao JSON
    merged_data = add_group_to_json(db_manager, merged_data, selected_client_ids)
    # print(db_manager)
    #print(merged_data)
    # print(selected_client_ids)

    # Reinsere os dados no banco
    data_types = {
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
        'IRRIGACAO': 'string', 'grupo': 'string'
    }

    db_manager.insert_data("public", "bd_tomografia", merged_data, data_types)
