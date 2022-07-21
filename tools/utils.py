#!/usr/bin/env python3


"""

Tools

"""


import random
import logging
import sys
import datetime as dt
import pathlib

logger = logging.getLogger(__name__)


def setup_log(myname=None):
    """ Set up logging """
    # If a log dir is specified, log to file,
    # Otherwise, log to stdout
    if myname:
        myname.parent.mkdir(exist_ok=True)
        # If a log file with same name already exists, rename it
        if myname.is_file():
            myname.rename(myname.with_suffix(f'.log.old_{dt.datetime.now().strftime("%Y%m%d_%H%M%S")}'))
        log_info = logging.FileHandler(myname, mode='w')
        log_info.setLevel(logging.DEBUG)
    else:
        log_info = logging.StreamHandler(sys.stdout)
    log_info.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    return log_info


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


def get_product_type(product):
    """ From a product title, get it's type """
    type = 'Unknown'
    try:
        if product[0:2] == 'S1':
            type = product.split('_')[2]
        elif product[0:2] == 'S2':
            type = product.split('_')[1]
            if not type.startswith('M'):
                type = 'Unknown'
        elif product[0:2] == 'S3':
            tmp = product.split('_')
            if tmp[1] == 'SL':
                type = 'SLSTR_L' + tmp[2]
            elif tmp[1] == 'SR':
                type = 'SRAL_L' + tmp[2]
            elif tmp[1] == 'OL':
                type = 'OLCI_L' + tmp[2]
            elif tmp[1] == 'SY':
                type = 'SYN_L' + tmp[2]
        elif product[0:2] == 'S5':
            tmp = product.split('_')
            if tmp[1] == 'OFFL':
                type = 'OFFL_' + tmp[2]
            elif tmp[1] == 'NRTI':
                type = 'NRTI_' + tmp[2]
        if 'DTERRENG' in product:
            type = type + '_DTERRENG'
    except TypeError:
        type = 'Unknown'
    if type == 'Unknown':
        logger.info(f'Type not found for product {product}')
    return type

