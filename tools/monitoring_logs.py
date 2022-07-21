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
import utils

logger = logging.getLogger(__name__)


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

    # Product type
    log_df['product_type'] = log_df['product'].apply(lambda x: utils.get_product_type(x))

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
    deleted = []
    users_new = 0
    users_deleted = 0
    sat = myfile.stem.split('-')[0]
    if sat == 'S5p':
        pattern = '.nc'
    else:
        pattern = '.zip'
    okish = False
    with open(myfile) as f:
        for line in f:
            if all(x in line for x in ["successfully downloaded", pattern]):
                synchronized.append(line)
            elif all(x in line for x in ["download by", "completed"]):
                downloaded.append(line)
            elif 'Ingestion processing complete for product file' in line:
                ingested.append(line)
            elif 'deleted globally' in line:
                deleted.append(line)
            # For user creation, need to check if it's successfull with the next line in the log file
            elif all(x in line for x in ["Create/save User(", "http-nio-"]):
                okish = True
            elif okish and all(x in line for x in ['SUCCESS']):
                users_new += 1
                okish = False
            elif 'Delete User' in line:
                users_deleted += 1

    return synchronized, ingested, downloaded, deleted, users_new, users_deleted


def check_synchronized(list_synch, list_ing, list_del):

    synch_df = pd.DataFrame(list_synch, columns=['all'])
    ing_df = pd.DataFrame(list_ing, columns=['all'])
    del_df = pd.DataFrame(list_del, columns=['all'])

    # -- Get info on synchronized products (ie with odata synchronizer)

    # Ingestion date
    if synch_df.shape[0] != 0:
        ingestion_date = synch_df['all'].apply(lambda x: utils.get_ingestion_time(x))
        # Get product name
        product_name_sync = synch_df['all'].apply(lambda x: x.split('[INFO ] Product \'')[1].split('.')[0])
        # Product type
        synch_df['product_type'] = product_name_sync.apply(lambda x: utils.get_product_type(x))
        # Timeliness = ingestion_date - sensing_date (in hours)
        synch_df['timeliness'] = (ingestion_date - product_name_sync.apply(lambda x: utils.get_sensing_time(x)))\
            .apply(lambda x: pd.to_timedelta(x).total_seconds() / 3600)
        # Product size (in Gb)
        synch_df['size'] = synch_df['all'].apply(lambda x: int(x.split('(')[1].split(' bytes')[0])/1024/1024/1024)
        # Cleaning
        synch_df['action'] = 'synchronized'
        synch_df.drop(columns=['all'], inplace=True)

    # -- Get info on deleted products
    if del_df.shape[0] != 0:
        del_df['timeliness'] = 0
        del_df['size'] = 0
        del_df['product_type'] = 'Unknown'
        del_df['action'] = 'deleted'
        del_df.drop(columns=['all'], inplace=True)

    # -- Get info on ingested products (ie with file scanner)
    if ing_df.shape[0] != 0:
        ingestion_date = ing_df['all'].apply(lambda x: utils.get_ingestion_time(x))
        # Get product name
        product_name_ing = ing_df['all'].apply(lambda x: x.split('file:')[1].split('.')[0].split('/')[-1])
        # Product type
        ing_df['product_type'] = product_name_ing.apply(lambda x: utils.get_product_type(x))
        # Timeliness = ingestion_date - sensing_date
        ing_df['timeliness'] = (ingestion_date - product_name_ing.apply(lambda x: utils.get_sensing_time(x))) \
            .apply(lambda x: pd.to_timedelta(x).total_seconds() / 3600)
        # Product size (in Gb)
        ing_df['size'] = ing_df['all'].apply(lambda x: int(x.split('(')[1].split(' bytes')[0])/1024/1024/1024)
        # Cleaning
        ing_df['action'] = 'fscanner'
        ing_df.drop(columns=['all'], inplace=True)

    return synch_df.append(ing_df).append(del_df)


def read_logs_dhus(log_day, day):
    """
    Check a dhus logfile
    """

    logger.debug(f'Checking logfile {log_day}')

    # Parse logfile
    synch_list, ingested_list, down_list, deleted_list, new_users, deleted_users = check_logfile(log_day)

    # Check products downloaded
    download_df = check_downloaded(down_list)

    # Check products input
    input_df = check_synchronized(synch_list, ingested_list, deleted_list)

    if input_df.shape[0] == 0:
        return None, download_df, 0, 0

    # Extra information needed for output csv file
    input_df['day'] = day

    # Compute the statistics of interest:
    group = input_df.groupby(['day', 'product_type', 'action'])
    # - nb of products
    stats_nb = group['action'].count()
    # - total volume of products
    stats_sum = group['size'].sum()
    # - median timeliness
    try:
        stats_median = group['timeliness'].median()
        input_stats_tmp = pd.concat([stats_sum, stats_nb, stats_median], axis=1)
    except pd.core.base.DataError:
        input_stats_tmp = pd.concat([stats_sum, stats_nb], axis=1)

    input_stats = input_stats_tmp.rename(columns={'action': 'nb_products'}).reset_index()

    return input_stats, download_df, new_users, deleted_users
