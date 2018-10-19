import pandas as pd
from utils import persistence as ps

def clean_cabs(row: pd.Series) -> pd.Series:


    return row

def clean_cabs_dt(x):
    return pd.to_datetime(x,
                     format="%Y-%m-%d %H:%M:%S",
                     errors='coerce')

def clean_num(x):
    return pd.to_numeric(x, errors='coerce')

def clean_transit_date(date,time):
    return pd.to_datetime(str(date)+str(time),format="%m/%d/%Y%H:%M:%S", errors='coerce')

def clean_traffic_date(x):
    return pd.to_datetime(x, format="%m/%d/%Y %H:%M:%S", errors='coerce')

def clean_transit(row: pd.Series) -> pd.Series:
    row['DATETIME'] = pd.to_datetime(str(row['DATE'])+str(row['TIME']),
                             format="%m/%d/%Y%H:%M:%S",
                             errors='coerce')
    row['EXITS'] = pd.to_numeric(row['EXITS'], errors='coerce')
    return row

def parse_rg_dt(x):
    return pd.to_datetime(x,
                     format="%Y-%m-%d",
                     errors='coerce')
