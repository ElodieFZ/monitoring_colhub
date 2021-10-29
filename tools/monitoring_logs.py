#!/usr/bin/python3

"""

Check logs from a datahub dhus instance

elodief - Oct 2021

"""

import pathlib
import logging
import pandas as pd
import datetime as dt
import numpy as np

logger = logging.getLogger(__name__)


def get_sensing_time(product_name):

    if 'DTERRENG' in product_name:
        pattern = product_name.split('_')[-6]
    elif product_name[0:2] == 'S3':
        pattern = product_name[16:31]
    elif product_name[0:3] == 'S5p':
        pattern = product_name[20:35]
    else:
        pattern = product_name.split('_')[-5]

    try:
        return dt.datetime.strptime(pattern, '%Y%m%dT%H%M%S')
    except ValueError:
        logger.error(f'Problem trying to get sensing information from product name ({product_name}).')
        return None


def check_downloaded(mylist):

    if len(mylist) == 0:
        return None

    log_df = pd.DataFrame(mylist, columns=['all'])

    # Get download date
    # TODO: create function that includes the exception,
    # so that we only loose the timeliness information for one or several products instead of the whole df
    log_df['download_time'] = log_df['all'].apply(
            lambda x: dt.datetime.strptime(x.split('[')[2].split(']')[0], '%Y-%m-%d %H:%M:%S,%f'))

    # Get user name
    log_df['user'] = log_df['all'].apply(lambda x: x.split('\'')[3])

    # Get product name
    log_df['product'] = log_df['all'].apply(lambda x: x.split('(')[1].split(')')[0])

    # Get product size
    log_df['size'] = log_df['all'].apply(lambda x: x.split('-> ')[1].split()[0])

    # Get download time
    log_df['download_duration'] = log_df['all'].apply(lambda x: x.split('completed in ')[1].split('ms')[0])
    logger.info(f'Number of products downloaded: {log_df.shape[0]}')

    return log_df


def check_logfile(myfile):
    synchronized = []
    downloaded = []
    ingested = []
    removed = 0
    sat = myfile.stem.split('-')[0]
    if sat == 'S5p':
        pattern = '.nc'
    else:
        pattern = '.zip'
    with open(myfile) as f:
        for line in f:
            if all(x in line for x in ["successfully downloaded", pattern]):
                synchronized.append(line)
            elif all(x in line for x in ["download by", "completed"]):
                downloaded.append(line)
            elif 'Ingestion processing complete for product file' in line:
                ingested.append(line)
            elif 'deleted globally' in line:
                removed += 1
    return synchronized, ingested, downloaded, removed


def check_synchronized(mylist):

    if len(mylist) == 0:
        return len(mylist), np.nan, np.nan, np.nan

    log_df = pd.DataFrame(mylist, columns=['all'])

    # Get ingestion date
    # TODO: create function that includes the exception,
    # so that we only loose the timeliness information for one or several products instead of the whole df
    try:
        log_df['ingestion_date'] = log_df['all'].apply(
            lambda x: dt.datetime.strptime(x.split('[')[2].split(']')[0], '%Y-%m-%d %H:%M:%S,%f'))
    except IndexError:
        logger.error("Problem trying to read timeliness information, so returning NaNs.")
        return len(mydf), np.nan, np.nan, np.nan

    # Get product name
    log_df['product_name'] = log_df['all'].apply(
        lambda x: x.split('[INFO ] Product \'')[1].split('.')[0])

    ### Check that product names match the sat name
    ### If not, drop the rows with non-matching product name
    ##log_df["sat_name_ok"] = log_df['product_name'].apply(lambda x: x[0:2] == sat[0:2])
    ##if not log_df["sat_name_ok"].all():
    ##    logger.error('Problem with products, some product_names do not match the satellite name.')
    ##    logger.error('Products that have been ingested: ')
    ##    logger.error(log_df[~log_df['sat_name_ok']]['product_name'])
    ##    log_df = log_df.loc[log_df["sat_name_ok"], :]

    # Get sensing date
    log_df['sensing_date'] = log_df['product_name'].apply(lambda x: get_sensing_time(x))

    # Timeliness = ingestion_date - sensing_date
    log_df['timeliness'] = log_df['ingestion_date'] - log_df['sensing_date']

    #log_df.loc[log_df['timeliness'][::-1].idxmax()][['product_name', 'timeliness']]

    logger.info(f'Min timeliness: {log_df["timeliness"].min()}')
    logger.info(f'Max timeliness: {log_df["timeliness"].max()}')
    logger.info(f'Median timeliness: {log_df["timeliness"].median()}')

    return log_df.shape[0], log_df["timeliness"].min(), log_df["timeliness"].max(), log_df["timeliness"].median()


def read_logs_dhus(log_day):
    """
    Check a dhus logfile
    """

    logger.debug(f'Checking logfile {log_day}')

    # Parse logfile
    synch_list, ingested_list, down_list, deleted = check_logfile(log_day)

    # Check products synchronized (with odata synchronizer)
    synchronized, timeliness_min, timeliness_max, timeliness_median = check_synchronized(synch_list)
    logger.info(f'{synchronized} products successfully synchronized (from odata synchronizer).')

    logger.info(f'{deleted} products successfully evicted.')
    logger.info(f'{len(ingested_list)} products successfully ingested (from file scanner) - timeliness not checked for these products.')

    # Check products downloaded
    downloaded = check_downloaded(down_list)
    if downloaded is not None:
        logger.info(f'{downloaded.shape[0]} products successfully downloaded by users.')
    else:
        logger.info('No products downloaded.')

    return synchronized, timeliness_min, timeliness_max, timeliness_median, deleted, len(ingested_list), downloaded
