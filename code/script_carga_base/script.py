from pyDataverse.api import NativeApi
from pyDataverse.models import Dataverse, Dataset, Datafile
from pyDataverse.utils import read_file
import json
import numpy as np

# Configurações
BASE_URL = 'https://demo.dataverse.org/' # Essa URL que está aqui é do demo dataverse de Harvard
    #'https://dadosderede.rnp.br'  # Ess é a URL do Dataverse da RNP
API_TOKEN = 'ba164772-1a81-4e5b-a150-7b9d88deb57b' # Troca aqui pelo seu token
NOME_DATAVERSE = "Dv14" # ID do seu Dataverse
API = NativeApi(BASE_URL, API_TOKEN)

# Criação do Dataverse
dataverse = Dataverse()
dataverse.set({
    "alias": NOME_DATAVERSE,
    "name": "Data Verse de Teste da Carga Catálogo",
    "dataverseContacts": [{"contactEmail": "danielcmo@ic.uff.br"}],
    "affiliation": "UFF",
    "description": "Dataverse Teste de Carga",
    "permissionRoot": False
})
print(dataverse.json())

# Enviar o Dataverse
response = API.create_dataverse('dataversedaniel', dataverse.json()) # aqui tem que trocar pelo ID do dataverse parent.
print(f"Dataverse criado: {response.status_code}")

# Criação do Dataset dentro do Dataverse
dataset = Dataset()
ds_filename = "/users/danieldeoliveira/dataset3.json" # Substituir pelo caminho do JSON que descreve o dataset a ser criado.
dataset.from_json(read_file(ds_filename))
dataset.validate_json() # Sempre valida, para ver se está com todos os campos. Qualquer campo faltante, ele NÃO CRIA NADA

# Enviar o Dataset
response = API.create_dataset(NOME_DATAVERSE, dataset.json())
print(response.json())
print(f"Dataset criado: {response.status_code}")

# Obter o ID do Dataset criado (DOI)
dataset_pid = response.json()['data']['persistentId']

# Upload de um Arquivo para dentro do dataset
df = Datafile()
file_path = '/users/danieldeoliveira/Downloads/247252.pdf'  # Substitua pelo caminho do seu arquivo para ser carregado
df.set({"pid": dataset_pid, "filename": file_path})
df.get()
print(df.json())
resp = API.upload_datafile(dataset_pid, file_path, df.json())
print(resp.json())
print(f"Arquivo enviado: {response.status_code}")