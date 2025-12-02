import argparse
import os
import pandas as pd
from pyDataverse.models import Datafile
from sqlalchemy import create_engine
from pyDataverse.api import NativeApi
from datetime import datetime

# Converte filtros tipo campo=valor
# ou campo=valor1:valor2 em dict

def parse_filtros(lista_filtros):
    filtros = {}
    for item in lista_filtros:
        if '=' not in item:
            continue
        campo, valor = item.split('=', 1)
        if ':' in valor:
            minimo, maximo = valor.split(':', 1)
            filtros[campo] = [minimo, maximo]
        else:
            filtros[campo] = valor
    return filtros

def montar_clausula_where(filtros):
    clausulas = []
    for campo, valor in filtros.items():
        if isinstance(valor, list):
            clausulas.append(f'"{campo}" BETWEEN {repr(valor[0])} AND {repr(valor[1])}')
        else:
            clausulas.append(f'"{campo}" = {repr(valor)}')
    return " AND ".join(clausulas)

def consultar_mlab(conn_str, filtros):
    print(f"Executando consulta de postgres {filtros}")
    engine = create_engine(conn_str)
    where = montar_clausula_where(filtros)
    query = 'SELECT * FROM "mlabs"' # Trocar aqui com o caminho do MLAB do Google, por enquanto teste no espelho local
    if where:
        query += f' WHERE {where}'
    return pd.read_sql_query(query, engine)

def upload_csv(api, dataset_pid, caminho_csv):
    print("Realizando o upload")
    df = Datafile()
    agora = datetime.now()
    dir_data = agora.strftime("%Y-%m-%d")
    dir_hora = agora.strftime("%H-%M-%S")
    caminho = os.path.join(dir_data, dir_hora)
    df.set({"pid": dataset_pid, "directoryLabel": caminho, "filename": caminho_csv})
    df.get()
    resp = api.upload_datafile(dataset_pid, caminho_csv, df.json())
    print(f"Arquivo {caminho_csv} enviado para o dataset {dataset_pid}")


def main(args):
    filtros = parse_filtros(args.filtro)
    df = consultar_mlab(args.pg_conn_str, filtros)

    os.makedirs("saida", exist_ok=True)
    caminho_csv = os.path.join("saida", "mlabs_filtrado.csv")
    df.to_csv(caminho_csv, index=False)
    print(f"Dados extraídos em: {caminho_csv}")

    api = NativeApi(args.dataverse_url, args.api_token)
    upload_csv(api, args.dataset_pid, caminho_csv)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exporta dados da tabela mlabs e envia para Dataset existente no Dataverse.")
    parser.add_argument("--pg_conn_str", required=True, help="String de conexão PostgreSQL.")
    parser.add_argument("--dataverse_url", required=True, help="URL do Dataverse (ex: https://demo.dataverse.org)")
    parser.add_argument("--api_token", required=True, help="Token da API do Dataverse")
    parser.add_argument("--dataset_pid", required=True, help="PID do Dataset (ex: doi:10.70122/FK2/EXEMPLO)")
    parser.add_argument("--filtro", action='append', help='Filtros no formato campo=valor ou campo=valor1:valor2. Pode ser usado várias vezes.')
    args = parser.parse_args()
    main(args)