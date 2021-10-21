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




