# %%
import requests 
# biblioteca utilizada para fazer requisições na api
import json
# biblioteca utilizada para criar e editar dados em formato json
import pandas as pd
# biblioteca utilizada para tratamento de dados
import requests_ratelimiter
from requests_ratelimiter import LimiterSession
# biblioteca utilizada para limitar tempo de requisição em função de limitação de tempo de requisiões de api
import pymysql
from sqlalchemy import create_engine
# biblioteca utilizada para criação de banco de dados
import xml.etree.ElementTree as ET
# bibliotexa utilizada para tratamento de arquivos no formato de xml
import io
import lxml
import xml.etree
import xmltodict
import os
import numpy as np
# biblioteca de criação de array 

# %%
from lxml.etree import fromstring, tostring

# %%
req = requests.get('https://public-api.convenia.com.br/api/v3/employees',
                                   headers={'Token': ''}, timeout=5)
req.status_code
# código para conexão com o convenia, link extraído da página da documentação da API e o headers é baseado no token gerado pelo convenia, parâmetro de timeout para limitar tempo de resposta de conexão a 5 segundos

# %%
req_json = req.json()
# transformação do arquivo de resposta para json

# %%
data = req_json['data']
# acesso da tabela onde existem os dados

df = pd.DataFrame(data)
# transformação do arquivo para dataframe

# %%
ativos = df['id']
ativos= ativos.to_list()
# transformação de coluna em lista para fazer o loop

# %%
session = LimiterSession(per_minute=40)

# limitação para 40 requisições por minuto em função da característica da api

returning = []

for i in range(len(ativos)):

    id = ativos[i]

    req = session.get('https://public-api.convenia.com.br/api/v3/employees/{}'.format(id),
                                   headers={'Token': ''})
    data_1 = req.json()
    try:
        data_2 = data_1['data']
    
        returning.append(data_2)
    except: 
        pass
# for para pegar os dados de cada funcionário baseado no id gerado pela lista

# %%
df_a = pd.DataFrame(returning)
#transformação de lista em dataframe

# %%
df_a.columns
# visualização das colunas

# %%
ids= df_a['registration']
# pegar informações de matricula dos funcionários


# %%
ids = ids.to_list()
#colocar matrículas em uma lista

# %%
valueToBeRemoved = None
 
try:
    while True:
        ids.remove(valueToBeRemoved)
except ValueError:
    pass
# exclusão dos funcionários que não tem matrícula

# %%

req_headers = {"content-type": "xml"}
payload = """<soapenv:Envelope xmlns:soapenv='http://schemas.xmlsoap.org/soap/envelope/' xmlns:ws='http://www.ahgora.com.br/ws'>
  <soapenv:Header/>
  <soapenv:Body>
     <ws:obterResultados>
        <empresa>{empresa}</empresa>
        <matricula>{matricula}</matricula>
        <datai>{datainicio}</datai>
        <dataf>{datafim}</dataf>
        <opcoes>
           <Opcao>
              <nome>periodo_aberto</nome>
              <valor>false</valor>
           </Opcao>
           <Opcao>
              <nome>apuracao_diaria</nome>
              <valor>false</valor>
           </Opcao>
           <Opcao>
              <nome>troca_matricula_codigo_interno</nome>
              <valor>false</valor>
           </Opcao>
        </opcoes>
     </ws:obterResultados>
  </soapenv:Body>
</soapenv:Envelope>"""
# formato do xml gerado pela plataforma myaghora com os elementos que deverão ser substituídos para obtenção dos resultados necessários para análise do ponto

# %%
import xmljson
# biblioteca de xml para json

# %%
from bs4 import BeautifulSoup
# biblioteca para embelezar arquivos no formato xml
import json
# biblioteca que transforma/lê arquivos em formato json
session = LimiterSession(per_minute=40)
batidas = []
for i in range(len(ids)):
  response = session.post(
  "http://www.ahgora.com.br/ws/pontoweb.php?wsdl",
  data=payload.format(empresa= 'TOKEN', matricula=ids[i], datainicio='DDMMYYYY', datafim='DDMMYYYY'), #substituir token pelo valor gerado no site do myaghora referente a empresa e colocar data de início e fim, comprrendido ao período e abertura e fechamento do mês
  headers=req_headers
)
  data = response.content
  dict_data = xmltodict.parse(data)
  try:
    resultados = dict_data['SOAP-ENV:Envelope']['SOAP-ENV:Body']['ns1:obterResultadosResponse']['Resultados']['Resultado']
    batidas.append(resultados)
  except:
    pass
#passar a lista de matriculas no loop com a data referente ao mês  


# %%
len(batidas)
# medir quantidade de linhas na coluna

# %%
lista = []
for i in range(len(batidas)):
    ponto = batidas[i]
    for i in range(len(ponto)):
        try:
            b = ponto[i]
            lista.append(b)
        except:
            pass
# passar a lista de batidas no loop

# %%
df_ponto= pd.DataFrame(lista)
# transforma a lista de batidas de ponto para dataframe

# %%
df_ponto[50:100]
# visualizar dataframe pelo indice (número entre colchetes)

# %%
df_saldo = (df_ponto.loc[df_ponto['nome'] == 'SALDO'])
# cria um dataframe ao pesquisar a informação do saldo que está no nome

# %%
df_saldo.set_index(df_saldo['matricula'], inplace=True)
# define a coluna matricula como o index do dataframe saldo

# %%
df_saldo.drop('matricula', axis=1, inplace=True)
# retira a coluna matricuLa do dataframe

# %%
df_saldo.rename(columns={'cod_contabil':'cod_contabil_saldo', 'nome':'saldo', 'valor':'tempo_saldo'}, inplace = True)
# renomeia as colunas do dataframe

# %% [markdown]
# Repetir os códigos acima para cada informação que desejar ser extraída

# %%
df_d= df_horas_trabalhadas.join(df_faltas, how='outer')
# cria um dataframe juntando dois dataframes de fomra 'outer'  -> união total das tabelas (utilizando a matrícula-index como coluna de ligação)

# %% [markdown]
# Repetir o código acima quanta vezes forem necessárias para cada dataframe criado

# %%
import pymysql
# biblioteca para instalar o driver do mysql no python
from sqlalchemy import create_engine
engine = create_engine("mysql+pymysql://user:password@localhost/nomedobancodedados")
#substituir valores conforme banco de dados criado em mysql
con = engine.connect()
df_dataset_junhovint.to_sql(name='pontocolaboradoressetembro22', con=engine.connect(), if_exists='replace', index=False)
# criação da tabela no banco de dados


