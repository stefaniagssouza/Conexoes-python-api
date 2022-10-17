# %%
import requests
# biblioteca para fazer requisições via API
req = requests.get('https://public-api.convenia.com.br/api/v3/employees/dismissed',
                                   headers={'Token': ''})
# link de conexão extraído do site da convenia, informar o token de acesso gerado na plataforma

# %%
print(type(req))
# exibir tipo de dado do arquivo

# %%
import json
# biblioteca para ler/transformar arquivos em json
req_json = req.json()
# transformação do arquivo resposta em json para melhor leitura

# %%
import pandas as pd
# biblioteca principal de tratamento de dados
data = req_json['data']
# filtrando apenas os arquivos do dicionário 'data', que contém as informações relevantes
print(type(data))
# visualização de tipo de dados
df = pd.DataFrame(data)
# tranformação do dicionário 'data' em dataframe


# %%
df.keys()
# visualização das chaves do dicionário

# %%
df.head(2)
# visualização do cabeçalho do dataframe

# %%
id = df['id']
# criação da series de id
id_df = pd.DataFrame(id)
# transformação da série em dataframe


# %%
dismissal = df['dismissal']
# criação da series de dismissal
print(type(dismissal))
# visualização de tipo de dados

# %%
list_dismissal = []
# criação de lista vazia
item = df['dismissal']
# definição de item
for i in range(df['id'].count()):
    dismissal_info = (item[i]['id'], item[i]['date'], item[i]['type'], item[i]['termination_notice'], item[i]['dismissal_step_id'], item[i]['dismissal_step'], item[i]['breaking_contract'],item[i]['accountancy_date'],item[i]['remove_benefit'],item[i]['motive'],item[i]['comments'],item[i]['finished_at'],item[i]['newSupervisorId'],item[i]['supervisor'])
    list_dismissal.append(dismissal_info)
# loop passando o as informações de dismissal para extrair as informações sobre demitidos

# %%
to_columns = ["id", "date","type","termination_notice","dismissal_step_id","dismissal_step","breaking_contract","accountancy_date","remove_benefit","motive","comments","finished_at","newSupervisorId","supervisor"]
# transformação dos itens em colunas
dismissal_df = pd.DataFrame(list_dismissal , columns=to_columns)
# transformação da lista em dataframe
dismissal_df.rename(columns={'id':'id_dismissal'}, inplace = True)
# renomeação das colunas

# %%
list_supervisor = []
# criação de lista vazia
item = dismissal_df['supervisor']
# definição de item
for i in range(dismissal_df['supervisor'].count()):
    supervisor_info = (item[i])
    list_supervisor.append(supervisor_info)
# loop passando o as informações de supervisor para extrair as informações sobre supervidores

# %%
to_columns = ["id", "name"]
# transformação dos itens em colunas
supervisor_df = pd.DataFrame(list_supervisor , columns=to_columns)
# transformação da lista em dataframe
supervisor_df.rename(columns={'id':'id_supervisor', 'name':'name_supervisor'}, inplace = True)
# renomeação das colunas

# %% [markdown]
# Realizar o processo acima para tratamento de todas as colunas que tiverem dicionários dentro e que se deseje extrair as informações

# %%
df_dat= dismissal_df.join(id_df, how='outer')
# cria um dataframe juntando dois dataframes de forma 'outer'  -> união total das tabelas (utilizando a matrícula-index como coluna de ligação)

# %% [markdown]
# Repetir o código acima quanta vezes forem necessárias para cada dataframe criado

# %%
df_dataset.drop(['type','termination_notice','dismissal_step','supervisor'], axis=1, inplace=True)
# remoção das colunas (explodidas) do dataframe

# %% [markdown]
# Excluir as colunas que foram decompostas - senão não será possível transformar o dataframe em base de dados

# %%
df_dataset.columns
# visualização das colunas do dataframe

# %%
df_dataset.info()
# informações do dataframe

# %%
import pymysql
# biblioteca para instalar o driver do mysql no python
from sqlalchemy import create_engine
engine = create_engine("mysql+pymysql://user:password@localhost/nomedobancodedados")
#substituir valores conforme banco de dados criado em mysql
con = engine.connect()
df_dataset.to_sql(name='colaboradoresdesligados', con=engine.connect(), if_exists='replace', index=False)
# criação da tabela no banco de dados


