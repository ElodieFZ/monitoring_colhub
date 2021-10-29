#!/usr/bin/env python3


"""

Tools to query a Sentinel datahub

"""


import pathlib
import logging
import sys
import yaml
import datetime as dt
import pandas as pd
import random
from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt

logger = logging.getLogger(__name__)


def pretty_date(mydate):
    """
    Format date for human reading in logs. Example: 20210502 to 2 May 2021.
    :param d: date as string YYYYMMDD
    :return: formatted string
    """
    if isinstance(mydate, str):
        mydate = dt.datetime.strptime(mydate, "%Y%m%d")
    return mydate.strftime("%d %B %Y - %H:%M:%S")


def get_cred(url, file, user=None):
    """
    Read credentials from text file.
    Return [user, pwd]
    """
    tmp = pd.read_csv(file, sep=';', names=['url', 'user', 'password'])
    ok = tmp[tmp['url'] == url]
    if user:
        ok = ok[ok['user'] == user]
    randint = random.randint(0, len(ok)-1)
    return ok.iloc[randint]['user'], ok.iloc[randint]['password']



def connect_hub(cfg):
    """ Connect to sentinel datahub """
    user, pwd = get_cred(cfg['url'], cfg['credentials'], cfg.get('user'))
    return SentinelAPI(user, pwd, cfg['url'], show_progressbars=False)


def call_api(myapi, **myquery):
    return myapi.query(**myquery)


def query_hub(myapi, sensing_start, sensing_end, footprint, myquery):
    """
    Request a Sentinel DataHub
    return: list of products id
    """
    logger.info(f"Searching products for platform {myquery['platformname']}, "
                f" sensing dates {sensing_start} to {sensing_end}")

    if footprint.stem.startswith('global'):
        products = myapi.query(date=(sensing_start, sensing_end), **myquery)
    else:
        zone = geojson_to_wkt(read_geojson(footprint))
        products = myapi.query(zone, area_relation='Intersects', date=(sensing_start, sensing_end), **myquery)

    products_df = myapi.to_dataframe(products)

    if products_df.size > 0:
        logger.info(f'{products_df.shape[0]} products found')
    else:
        logger.info(f'No products found')

    return products_df

