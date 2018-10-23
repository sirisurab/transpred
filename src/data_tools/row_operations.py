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

def clean_tsfare_date(date):
    return pd.to_datetime(str(date),format="%m/%d/%Y", errors='coerce')

def parse_rg_dt(x):
    return pd.to_datetime(x,
                     format="%Y-%m-%d",
                     errors='coerce')


def drop_outliers(df: pd.DataFrame, col: str):
    # drop outliers
    l_q = df[col].quantile(.25)
    h_q = df[col].quantile(.75)
    iqr1_5 = (h_q - l_q) * 1.5
    return df.loc[(df[col] > l_q - iqr1_5) & (df[col] < h_q + iqr1_5)]
