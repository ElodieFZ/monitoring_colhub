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


def get_ingestion_time(string):

    try:
        return dt.datetime.strptime(string.split('[')[2].split(']')[0], '%Y-%m-%d %H:%M:%S,%f')
    except IndexError:
        logger.error(f'Problem trying to get ingestion information from product name ({product_name}).')
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
    synch_df['date'] = synch_df['all'].apply(lambda x: get_ingestion_time(x))
    # Get product name
    synch_df['product_name'] = synch_df['all'].apply(lambda x: x.split('[INFO ] Product \'')[1].split('.')[0])
    # Sensing date
    #synch_df['sensing_date'] = synch_df['product_name'].apply(lambda x: get_sensing_time(x))
    # Timeliness = ingestion_date - sensing_date
    synch_df['timeliness'] = synch_df['date'] - synch_df['product_name'].apply(lambda x: get_sensing_time(x))
    # Product size
    synch_df['size'] = synch_df['all'].apply(lambda x: x.split('(')[1].split(' bytes')[0])
    # Cleaning
    synch_df['type'] = 'synchronized'
    synch_df.drop(columns=['all'], inplace=True)

    # -- Get info on deleted products
    # Ingestion date
    del_df['date'] = del_df['all'].apply(lambda x: get_ingestion_time(x))
    # Get product name
    del_df['product_name'] = None
    # Timeliness = ingestion_date - sensing_date
    del_df['timeliness'] = None
    # Product size
    del_df['size'] = None
    # Cleaning
    del_df['type'] = 'deleted'
    del_df.drop(columns=['all'], inplace=True)

    # -- Get info on ingested products (ie with file scanner)
    # Ingestion date
    ing_df['date'] = ing_df['all'].apply(lambda x: get_ingestion_time(x))
    # Get product name
    ing_df['product_name'] = ing_df['all'].apply(lambda x: x.split('file:')[1].split('.SAFE')[0].split('/')[-1])
    # Timeliness = ingestion_date - sensing_date
    ing_df['timeliness'] = None
    # Product size
    ing_df['size'] = ing_df['all'].apply(lambda x: x.split('(')[1].split(' bytes')[0])
    # Cleaning
    ing_df['type'] = 'fscanner'
    ing_df.drop(columns=['all'], inplace=True)

    return synch_df.append(ing_df).append(del_df)


def read_logs_dhus(log_day):
    """
    Check a dhus logfile
    """

    logger.debug(f'Checking logfile {log_day}')

    # Parse logfile
    synch_list, ingested_list, down_list, deleted_list, new_users, deleted_users = check_logfile(log_day)

    # Check products input
    input_df = check_synchronized(synch_list, ingested_list, deleted_list)

    # Check products downloaded
    download_df = check_downloaded(down_list)

    return input_df, download_df, deleted_users, new_users
