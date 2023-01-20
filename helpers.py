import pandas as pd
from bs4 import BeautifulSoup
import requests
import sys


def get_exchange_rate_url(source: str, target: str) -> str:

    if len(source.upper()) != 3:
        print(f'Invalid {source} or {target}')
        sys.exit()

    url = f"https://sdw-wsrest.ecb.europa.eu/service/data/EXR/M.{source}.{target}.SP00.A?detail=dataonly"

    return url


def get_data_identifier_url(identifier: str) -> str:

    url = f"https://sdw-wsrest.ecb.europa.eu/service/data/BP6/{identifier.strip()}?detail=dataonly"
    return url


def _find_values(polievka):
    getter = []
    for x in polievka.find_all('Obs'):
        for value in x.find_all('ObsValue'):
            getter.append(value.attrs['value'])
            # print(value.attrs['value'])
    return getter


def _find_time(polievka):
    getter = []
    for x in polievka.find_all('Obs'):
        for time_time in x.find_all('ObsDimension'):
            # print(value.attrs['value'])
            getter.append(time_time.attrs['value'])
            # df_time.append(time.attrs['value'])
    return getter


def create_df(full_url, ident):
    data_url = full_url
    data_req = requests.get(data_url)
    data_soup = BeautifulSoup(data_req.text, 'xml')

    values = _find_values(data_soup)
    times = _find_time(data_soup)

    skuska = pd.DataFrame(zip(times, values),  columns=['TIME_PERIOD', 'OBS_VALUE'])
    skuska['IDENTIFIER'] = ident
    skuska['OBS_VALUE'] = skuska['OBS_VALUE'].astype(float)

    return skuska



def create_df_2(full_url, ident):
    data_url = full_url
    data_req = requests.get(data_url)
    data_soup = BeautifulSoup(data_req.text, 'xml')

    values = _find_values(data_soup)
    times = _find_time(data_soup)

    ident_col = ident

    skuska = pd.DataFrame(zip(times, values),  columns=['TIME_PERIOD', ident])
    # skuska.rename({'OBS_VALUE': ident}, inplace=True)
    # skuska['IDENTIFIER'] = ident
    # skuska['OBS_VALUE'] = skuska['OBS_VALUE'].astype(float)

    return skuska

