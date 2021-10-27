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


def get_sensing_time(product_name, sat):

    if sat == 'S2DEM':
        pattern = product_name.split('_')[-6]
    elif sat == 'S3':
        pattern = product_name[16:31]
    elif sat == 'S5p':
        pattern = product_name[20:35]
    else:
        pattern = product_name.split('_')[-5]

    try:
        return dt.datetime.strptime(pattern, '%Y%m%dT%H%M%S')
    except ValueError:
        logger.error('Problem trying to get sensing information from product name.')
        logger.error(f'Product name: {product_name}, sat: {sat}')
        return None


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


def check_synchronized(mylist, sat):

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

    # Check that product names match the sat name
    # If not, drop the rows with non-matching product name
    log_df["sat_name_ok"] = log_df['product_name'].apply(lambda x: x[0:2] == sat[0:2])
    if not log_df["sat_name_ok"].all():
        logger.error('Problem with products, some product_names do not match the satellite name.')
        logger.error('Products that have been ingested: ')
        logger.error(log_df[~log_df['sat_name_ok']]['product_name'])
        log_df = log_df.loc[log_df["sat_name_ok"], :]

    # Get sensing date
    log_df['sensing_date'] = log_df['product_name'].apply(lambda x: get_sensing_time(x, sat))

    # Timeliness = ingestion_date - sensing_date
    log_df['timeliness'] = log_df['ingestion_date'] - log_df['sensing_date']

    log_df.loc[log_df['timeliness'][::-1].idxmax()][['product_name', 'timeliness']]

    logger.info(f'Min timeliness: {log_df["timeliness"].min()}')
    logger.info(f'Max timeliness: {log_df["timeliness"].max()}')
    logger.info(f'Median timeliness: {log_df["timeliness"].median()}')

    return log_df.shape[0], log_df["timeliness"].min(), log_df["timeliness"].max(), log_df["timeliness"].median()


def read_logs_dhus(type, sat, area, day, logdir, outfile=None):
    """
    Check dhus logs for one instance / one day
    :param sat: instance name (1 instance = 1 type of sentinel product)
    :param day:
    :return:
    """

# todo:
#  check FE logs for number of new users for a date
#  check FE logs for number of new users in a period
#  check FE logs for users downloading: nb of differnt users that downloaded one day
#                                       nb of products downloaded by each user on one day

    logger.info(f'\n\nChecking BE of product {sat} for date {day.strftime("%d/%m/%Y")}\n')
    if day == pd.to_datetime('today').date():
        log_day = list((logdir / sat / 'logs').rglob(f'{sat}-backend-{area}.log'))
    else:
        log_day = list((pathlib.Path(logdir) / sat / 'logs').rglob(
            f'{sat}-backend-{area}--{day.strftime("%Y-%m-%d")}-*.log'))
    logger.info(log_day)
    if len(log_day) != 1:
        logger.error('Something strange happened, more than one log file found for one day or no log file found.')
        logger.error(log_day)
        return 0, np.nan, np.nan, np.nan, 0
    log_day = log_day[0]

    logger.debug(f'Checking logfile {log_day}')
    synch_list, ingested_list, down_list, deleted = check_logfile(log_day)

    if 'BE' in type:
        logger.info(f'{deleted} products successfully evicted.')

        # Check products synchronized (with odata synchronizer)
        synchronized, timeliness_min, timeliness_max, timeliness_median = check_synchronized(synch_list, sat)
        logger.info(f'{synchronized} products successfully synchronized (from odata synchronizer).')

        # Check products ingested (with file scanner)
        logger.info(f'{len(ingested_list)} products successfully ingested (from file scanner) - timeliness not checked.')

    if 'FE' in type:
        # Check products downloaded
        downloaded = check_downloaded(down_list, outfile)
        logger.info(f'{downloaded} products successfully downloaded.')

    if 'BE' in type:
        return synchronized, timeliness_min, timeliness_max, timeliness_median, deleted, len(ingested_list)
    else:
        return None
