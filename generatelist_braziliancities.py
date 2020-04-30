import requests
import json
import pandas as pd
from pandas import json_normalize
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from tqdm import tqdm

tqdm.pandas()
nom = Nominatim(user_agent="eduhardocesar")
geolocator = RateLimiter(nom.geocode, min_delay_seconds=1)

r_ibge_states = requests.get(
    "https://servicodados.ibge.gov.br/api/v1/localidades/estados"
)
states = json_normalize(data=r_ibge_states.json(), sep="")

base_url_pop = "https://agenciadenoticias.ibge.gov.br/media/com_mediaibge/arquivos/"
arq_pop = base_url_pop + "de3c3890d5e127db41740496aa4ec20f.xls"

df_population = pd.read_excel(arq_pop, sheet_name="Municípios")
df_population.columns = df_population.loc[0]
df_population = df_population.drop(df_population.index[0])
df_population = df_population[df_population["COD. UF"].notnull()]
df_population.rename(
    columns={"NOME DO MUNICÍPIO": "City", "POPULAÇÃO ESTIMADA": "Population"},
    inplace=True,
)
df_population["ID_IBGE"] = (
    df_population["COD. UF"].map(str).str[:2]
    + df_population["COD. MUNIC"].map(str).str[:5]
)
df_population["Population"] = (
    df_population["Population"]
    .str.split(" ")
    .str[0]
    .fillna(df_population["Population"])
)
df_population = df_population[["ID_IBGE", "Population"]].reset_index(drop=True)
df_population = df_population.astype({"ID_IBGE": int, "Population": int})

df_brazil = pd.DataFrame()

for uf in states.id.unique():
    url = (
        "https://servicodados.ibge.gov.br/api/v1/localidades/estados/"
        + str(uf)
        + "/municipios"
    )
    r = requests.get(url)
    states = json_normalize(data=r.json(), sep="")
    df_brazil = df_brazil.append(states, ignore_index=True)

df_brazil = df_brazil[
    [
        "id",
        "microrregiaomesorregiaoUFregiaonome",
        "microrregiaomesorregiaoUFsigla",
        "microrregiaomesorregiaoUFnome",
        "microrregiaonome",
        "microrregiaomesorregiaonome",
        "nome",
    ]
]
df_brazil.rename(
    columns={
        "id": "ID_IBGE",
        "nome": "City",
        "microrregiaonome": "Microregion",
        "microrregiaomesorregiaonome": "Mesoregion",
        "microrregiaomesorregiaoUFsigla": "UF",
        "microrregiaomesorregiaoUFnome": "State",
        "microrregiaomesorregiaoUFregiaonome": "Region",
    },
    inplace=True,
)

df_brazil["find_latlon"] = (
    df_brazil["City"].map(str)
    + ", "
    + df_brazil["UF"].map(str)
    + ", "
    + df_brazil["State"].map(str)
    + ", Brazil"
)

df_brazil = pd.merge(df_brazil, df_population, on=["ID_IBGE"], how="left")

# df_brazil["proc_latlon"] = df_brazil["proc_latlon"].apply(geolocator)
df_brazil["find_latlon"] = df_brazil["find_latlon"].progress_apply(geolocator)
df_brazil["Latitude"] = df_brazil["find_latlon"].apply(
    lambda x: x.latitude if x != None else None
)
df_brazil["Longitude"] = df_brazil["find_latlon"].apply(
    lambda x: x.longitude if x != None else None
)
df_brazil = df_brazil[
    [
        "ID_IBGE",
        "Region",
        "UF",
        "State",
        "Microregion",
        "Mesoregion",
        "City",
        "Latitude",
        "Longitude",
        "Population",
    ]
]
df_brazil = df_brazil.set_index("ID_IBGE")
df_brazil.to_csv("list_braziliancities.csv")
