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
        "microrregiaomesorregiaonome": "Mesorregion",
        "microrregiaomesorregiaoUFsigla": "UF",
        "microrregiaomesorregiaoUFnome": "State",
        "microrregiaomesorregiaoUFregiaonome": "Region",
    },
    inplace=True,
)

df_brazil["find_latlon"] = (
    df_brazil["City"].map(str) + ", " + df_brazil["UF"].map(str) + ", Brazil"
)
# df_brazil['proc_latlon'] = df_brazil['proc_latlon'].apply(geolocator)
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
    ]
]
df_brazil = df_brazil.set_index("ID_IBGE")
df_brazil.to_csv("list_braziliancities.csv")
