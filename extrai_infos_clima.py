import os
import pandas as pd
from datetime import datetime, timedelta

# intervalo de datas
data_inicio = datetime.today()
data_fim = data_inicio + timedelta(days=7)

# formatando datas
data_inicio = data_inicio.strftime('%Y-%m-%d')
data_fim = data_fim.strftime('%Y-%m-%d')

city = 'Praia_Grande'
key = '4ZRH2C3BD7MGTYBXBZY46TEAY'

URL = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/%s/%s/%s?unitGroup=metric&include=days&key=%s&contentType=csv' % (city, data_inicio, data_fim, key)

dados = pd.read_csv(URL)

print(dados.head())

fp = f'./semana={data_inicio}/'
os.mkdir(fp)

dados.to_csv(fp + 'dados_brutos.csv')
dados[['datetime','tempmin','temp','tempmax']].to_csv(fp + 'temperatura.csv')
dados[['datetime','description','icon']].to_csv(fp + 'condicoes.csv')