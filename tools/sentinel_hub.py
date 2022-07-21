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
import sentinelsat

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


def get_cred(url, mdpfile):
    """
    Read credentials from text file.
    Return [user, pwd]
    """
    tmp = pd.read_csv(mdpfile, sep=';', names=['url', 'user', 'password'])
    ok = tmp[tmp['url'] == url]
    randint = random.randint(0, len(ok)-1)
    return ok.iloc[randint]['user'], ok.iloc[randint]['password']


def connect_hub(hub_url, mdpfile):
    """ Connect to sentinel datahub """
    user, pwd = get_cred(hub_url, mdpfile)
    return sentinelsat.SentinelAPI(user, pwd, hub_url, show_progressbars=False)


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
        zone = sentinelsat.geojson_to_wkt(sentinelsat.read_geojson(footprint))
        products = myapi.query(zone, area_relation='Intersects', date=(sensing_start, sensing_end), **myquery)

    products_df = myapi.to_dataframe(products)

    if products_df.size > 0:
        logger.info(f'{products_df.shape[0]} products found')
    else:
        logger.info(f'No products found')

    return products_df


def download(myapi, myuuid, outdir):
    """
    Download a product knowing its uuid.
    If product is not online, triggers the retrieval
    """
    logger.info(f'Trying to download product {myuuid}')
    try:
        product_info = myapi.get_product_odata(myuuid)
    except sentinelsat.exceptions.SentinelAPIError as e:
        logger.error(e)
        return False
    is_online = product_info['Online']

    download_ok = False
    asked_offline = False

    if is_online:
        logger.info(f'Product {myuuid} is online. Starting download.')
        try:
            myapi.download(myuuid, directory_path=outdir)
            download_ok = True
            logger.info(f'Product downloaded successfully.')
        except (sentinelsat.sentinel.SentinelAPIError, sentinelsat.sentinel.InvalidChecksumError, AttributeError) as e:
            logger.error('Download failed')
            logger.error(e)
    else:
        logger.info(f'Product {myuuid} is not online, so trigger retrieval.')
        try:
            myapi.trigger_offline_retrieval(myuuid)
            asked_offline = True
            logger.info(f'Product retrieval triggered successfully.')
        except:
            logger.error('Some exception')
    return download_ok
