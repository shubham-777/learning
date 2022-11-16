from datetime import datetime, timedelta
from multiprocessing import Pool
from functools import partial
import numpy as np
import pandas as pd
import yfinance as yf
from functools import lru_cache
from dateutil.relativedelta import relativedelta
from time import time
from apps.main.helpers.constants import SniperTrend, COMMON_DATE_FORMATE, SniperName, EWMValues

from pandas.tseries.offsets import BDay


def parallelize(data, func, num_of_processes=1):
    data_split = np.array_split(data, num_of_processes)
    pool = Pool(num_of_processes)
    data = pd.concat(pool.map(func, data_split))
    pool.close()
    pool.join()
    return data


def run_on_subset(func, data_subset):
    return data_subset.apply(func, axis=1)


def parallelize_on_rows(data, func, num_of_processes=8):
    return parallelize(data, partial(run_on_subset, func), num_of_processes)


