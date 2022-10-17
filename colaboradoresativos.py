# %%
import requests
# biblioteca para fazer requisições via API
import json
# biblioteca para ler/transformar arquivos em json
import pandas as pd
# biblioteca principal de tratamento de dados
import requests_ratelimiter
# biblioteca para limitar o tempo
from requests_ratelimiter import LimiterSession
# função da biblioteca de tempo para limitar a quantidade de solicitações por tempo
import pymysql
# biblioteca para instalar o driver do mysql no python
from sqlalchemy import create_engine
# bibliotaca para tranformar dataframes em base de dados

# %%
req = requests.get('https://public-api.convenia.com.br/api/v3/employees',
                                   headers={'Token': ''})
# link de conexão extraído do site da convenia, informar o token de acesso gerado na plataforma

# %%
req = req.json()
# transformação do arquivo resposta em json para melhor leitura

# %%
data = req['data']
# filtrando apenas os arquivos do dicionário 'data', que contém as informações relevantes
print(type(data))
# mostrar qual o tipo do arquivo data
df = pd.DataFrame(data)
# tranformação do dicionário 'data' em dataframe

# %%
df.columns
# visualização das colunas do dataframe

# %%
df = df.reindex(columns=['id','name','last_name','email','hiring_date','salary','job','birth_date', 'contact_information', 'social_name', 'department', 'cost_center','address', 'educations', 'experience_period'])
# alteração do nome das colunas

# %%
experience_period = df['experience_period']
# criação da series do período de experiência 

# %%
df_experience_period= pd.DataFrame(experience_period)
# conversão da series para dataframe

# %%
list_experience_period = []
item = df['experience_period']

for i in range(df['id'].count()):
    experience_period_info = (item[i]['id'], item[i]['first_end'], item[i]['second_end'], item[i]['total_days'], item[i]['experience_period_type'])
    list_experience_period.append(experience_period_info)
#loop passando o id para extrair as informações da coluna 'experience_period' e inserir em uma lista

# %%
to_columns = ['id','first_end','second_end','total_days','experience_period_type']
experience_period_df = pd.DataFrame(list_experience_period, columns=to_columns)
# transformação da lista em dataframe

# %%
experience_period_df.rename(columns={'id':'id_experience_period'}, inplace = True)
# renomeação das colunas do dataframe

# %%
df_educations_teste = df[['id', 'educations']]
# criação de dataframe apenas com as colunas 'id' e 'educations'

# %%
df_educationst = df_educations_teste.explode('educations')
# 'explosão' da coluna 'educations'

# %%
df_educationst[20:30]
# visualizar dataframe pelo indice (número entre colchetes)

# %%
duplicados = df_educationst.duplicated(subset='id', keep='last')
# definir valores duplicados mantendo apenas os valores da última inserção

# %%
df_educationsc = df_educationst.drop_duplicates(subset='id', keep = 'last')
# remoção dos valores duplicados mantendo apenas os valores da última inserção em relação ao campo 'educations'

# %%
educations = df_educationsc['educations']
# transofrmação da coluna 'educations' em uma lista

# %%
df_educationsb = pd.DataFrame(educations)
# transformação da lista em um dataframe

# %%
df_educationsb.head(2)
# visualizar cabeçalho do dataframe

# %%
list_educations = []
item = df_educationsb['educations']
# definição de item

# %%
len(item)
# quantidade de itens na lista item

# %%
for i in range(len(item)):
      if 'course' not in item[i].keys():
            item[i]['course'] = 0
      if 'institution' not in item[i].keys():
            item[i]['institution'] = 0
      if 'graduation_year' not in item[i].keys():
            item[i]['graduation_year'] = 0
      if 'education_type' not in item[i].keys():
            item[i]['education_type'] = 0
for i in range(len(item)):
      educations_info = (item[i]['id'], item[i]['course'], item[i]['institution'], item[i]['graduation_year'], item[i]['education_type'])  
      list_educations.append(educations_info)
#loop passando o item para extrair as informações da coluna 'educations' e inserir em uma lista

# %%
to_columns = ["id","course", "institution", "graduation_year","education_type"]
#transformação dos itens em colunas
df_educationsb = pd.DataFrame(list_educations, columns= to_columns)
# criação do dataframe a partir da lista
df_educationsb.rename(columns={'id':'id_educations'}, inplace = True)
# renomeação das colunas


# %%
df_educationsb.head(2)
# visualizar cabeçalho do dataframe

# %% [markdown]
# Realizar o processo acima para tratamento de todas as colunas que tiverem dicionários dentro e que se deseje extrair as informações

# %%
df_da = df.join(job_df, how='outer')
# cria um dataframe juntando dois dataframes de forma 'outer'  -> união total das tabelas (utilizando a matrícula-index como coluna de ligação)

# %% [markdown]
# Repetir o código acima quanta vezes forem necessárias para cada dataframe criado

# %% [markdown]
# Excluir as colunas que foram decompostas - senão não será possível transformar o dataframe em base de dados

# %%
import pymysql
# biblioteca para instalar o driver do mysql no python
from sqlalchemy import create_engine
# bibliotaca para tranformar dataframes em base de dados
engine = create_engine("mysql+pymysql://user:password@localhost/nomedobancodedados")
#substituir valores conforme banco de dados criado em mysql
con = engine.connect()
df_dataset.to_sql(name='colaboradoresativos', con=engine.connect(), if_exists='replace', index=False)
# criação da tabela no banco de dados


