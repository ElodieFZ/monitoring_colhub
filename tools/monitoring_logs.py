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


def get_success_lines(myfile, pattern='.zip'):
    ok = []
    with open(myfile) as f:
        for line in f:
            if all(x in line for x in ["successfully downloaded", pattern]):
                ok.append(line)
    return ok


def products_evicted(myfile):
    out = 0
    with open(myfile) as f:
        for line in f:
            if 'deleted globally' in line:
                out += 1
    return out


def read_logs_dhus(sat, day, logdir):
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
        log_day = list((logdir / sat / 'logs').rglob(f'{sat}-backend-*.log'))
    else:
        log_day = list((pathlib.Path(logdir) / sat / 'logs').rglob(
            f'{sat}-backend-*--{day.strftime("%Y-%m-%d")}-*.log'))
    logger.info(log_day)
    if len(log_day) != 1:
        logger.error('Something strange happened, more than one log file found for one day or no log file found.')
        logger.error(log_day)
        return 0, np.nan, np.nan, np.nan, 0
    log_day = log_day[0]

    logger.debug(f'Checking logfile {log_day}')
    if sat == 'S5p':
        successes = get_success_lines(log_day, '.nc')
    else:
        successes = get_success_lines(log_day)
    logger.info(f'{len(successes)} products successfully downloaded.')

    deleted = products_evicted(log_day)
    logger.info(f'{deleted} products successfully evicted.')

    if len(successes) == 0:
        return len(successes), np.nan, np.nan, np.nan, deleted

    log_df = pd.DataFrame(successes, columns=['all'])

    # Get ingestion date
    # TODO: create function that includes the exception, 
    # so that we only loose the timeliness information for one or several products instead of the whole df
    try:
        log_df['ingestion_date'] = log_df['all'].apply(
            lambda x: dt.datetime.strptime(x.split('[')[2].split(']')[0], '%Y-%m-%d %H:%M:%S,%f'))
    except IndexError:
        logger.error("Problem trying to read timeliness information, so returning NaNs.")
        return len(successes), np.nan, np.nan, np.nan, deleted

    # Get product name, and sensing date from it
    log_df['product_name'] = log_df['all'].apply(
        lambda x: sat[0:2] + x.rpartition(sat[0:2])[2].split('.zip')[0])
    if sat == 'S2DEM':
        log_df['sensing_date'] = log_df['product_name'].apply(
            lambda x: dt.datetime.strptime(x.split('_')[-6], '%Y%m%dT%H%M%S'))
    elif sat == 'S3':
        log_df['sensing_date'] = log_df['product_name'].apply(
            lambda x: dt.datetime.strptime(x[16:31], '%Y%m%dT%H%M%S'))
    elif sat == 'S5p':
        log_df['sensing_date'] = log_df['product_name'].apply(
            lambda x: dt.datetime.strptime(x[20:35], '%Y%m%dT%H%M%S'))
    else:
        log_df['sensing_date'] = log_df['product_name'].apply(
            lambda x: dt.datetime.strptime(x.split('_')[-5], '%Y%m%dT%H%M%S'))

    # Timeliness = ingestion_date - sensing_date
    log_df['timeliness'] = log_df['ingestion_date'] - log_df['sensing_date']

    log_df.loc[log_df['timeliness'][::-1].idxmax()][['product_name', 'timeliness']]

    logger.info(f'Min timeliness: {log_df["timeliness"].min()}')
    logger.info(f'Max timeliness: {log_df["timeliness"].max()}')
    logger.info(f'Median timeliness: {log_df["timeliness"].median()}')

    return len(successes), log_df["timeliness"].min(), log_df["timeliness"].max(), log_df["timeliness"].median(), deleted


