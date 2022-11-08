import time
import numpy as np
import pandas as pd
from pandas import json_normalize
from tqdm import tqdm

from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

import httpx
import asyncio
import nest_asyncio
nest_asyncio.apply()

ano_pop = '2021'
arq_pop = "https://ftp.ibge.gov.br/Estimativas_de_Populacao/Estimativas_{}/estimativa_dou_{}.xls".format(
    ano_pop, ano_pop)

df_population = pd.read_excel(arq_pop, sheet_name="Municípios")
df_population.columns = df_population.loc[0]
df_population = df_population.drop(df_population.index[0])
df_population = df_population[df_population["COD. UF"].notnull()]
df_population.rename(columns={"NOME DO MUNICÍPIO": "City", "POPULAÇÃO ESTIMADA": "Population",
                     "COD. UF": "ID_UF", "COD. MUNIC": "ID_CITY"}, inplace=True,)
df_population["ID_IBGE"] = (df_population["ID_UF"].map(
    str).str[:2] + df_population["ID_CITY"].map(str).str[:5]).astype(str).astype(int)
df_population['Population'] = np.where(df_population['Population'].str.find("(") > 0, df_population['Population'].str.split(
    r"(", expand=True)[0].str.replace(".", "", regex=True), df_population['Population'])
df_population = df_population[["ID_IBGE", "ID_UF",
                               "ID_CITY", "Population"]].reset_index(drop=True)
df_population = df_population.astype(
    {"ID_IBGE": str, "ID_UF": str, "ID_CITY": str, "Population": int})


async def get_async(url):
    async with httpx.AsyncClient() as client:
        return await client.get(url)

urls = ["https://servicodados.ibge.gov.br/api/v1/localidades/estados"]


async def obter_estados():
    resps = await asyncio.gather(*map(get_async, urls))
    data_json = [resp.json() for resp in resps]

    for html in data_json:

        states = json_normalize(data=html, sep="")
        list_urls = ["https://servicodados.ibge.gov.br/api/v1/localidades/estados/{}/municipios".format(
            uf) for uf in states.id.unique()]

    return list_urls


async def launch2(urls):

    resps = await asyncio.gather(*map(get_async, urls))
    data_json = [resp.json() for resp in resps]
    global df_cities
    df_cities = pd.DataFrame()

    for html in data_json:

        states = json_normalize(data=html, sep="")
        df_cities = pd.concat([df_cities, states])

    df_cities = df_cities[["id", "microrregiaomesorregiaoUFregiaonome", "microrregiaomesorregiaoUFsigla",
                           "microrregiaomesorregiaoUFnome", "microrregiaonome", "microrregiaomesorregiaonome",
                           "regiao-imediatanome", "regiao-imediataregiao-intermediarianome", "nome"]]
    df_cities.rename(columns={"id": "ID_IBGE", "nome": "City", "microrregiaonome": "Microregion", "microrregiaomesorregiaonome": "Mesoregion",
                     "microrregiaomesorregiaoUFsigla": "UF", "microrregiaomesorregiaoUFnome": "State", "microrregiaomesorregiaoUFregiaonome": "Region",
                              "regiao-imediatanome": "ImmediateRegion", "regiao-imediataregiao-intermediarianome": "IntermediateRegion"}, inplace=True)
    df_cities['Location'] = df_cities['City'] + ", " + \
        df_cities['UF'] + ", " + df_cities['State'] + ", Brazil"

    df_cities = df_cities.astype({"ID_IBGE": str})

    return df_cities

estados = asyncio.run(obter_estados())
asyncio.run(launch2(estados))

geolocator = Nominatim(user_agent="user_agent")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

location = []
geopy_latitude = []
geopy_longitude = []

list_location = df_cities['Location'].to_list()

for loc in tqdm(list_location):

    time.sleep(1)
    find_location = geocode(loc)
    location.append(loc)
    geopy_latitude.append(find_location.latitude)
    geopy_longitude.append(find_location.longitude)

colunas = ["Location", "Latitude", "Longitude"]
df_location = pd.DataFrame(
    zip(location, geopy_latitude, geopy_longitude), columns=colunas)

df_cities_population = pd.merge(
    df_cities, df_population, on=["ID_IBGE"], how="left")
df_brazil = pd.merge(df_cities_population, df_location,
                     on=["Location"], how="left")

df_brazil = df_brazil[["ID_IBGE", "ID_UF", "ID_CITY", "Region", "UF", "State",
                       "Microregion", "Mesoregion", "ImmediateRegion", "IntermediateRegion",
                       "City", "Location", "Latitude", "Longitude", "Population"]]

df_brazil.to_csv("list_braziliancities.csv")
print("The file was generated!")
