import pandas as pd

def clean_cabs(row: pd.Series) -> pd.Series:
    print(' row index is %s' % str(row.index))
    print(' row passenger_count with loc index is %s' % str(row.loc['passenger_count']))
    print(' row index is %s' % str(row.index))
    print(' row dodatetime with loc index is %s' % str(row.loc['dodatetime']))
    row['dodatetime'] = pd.to_datetime(row['dodatetime'],
                                             format="%Y-%m-%d %H:%M:%S",
                                            errors='coerce')
    row['passengers'] = pd.to_numeric(row['passengers'], errors='coerce')
    #row['longitude'] = pd.to_numeric(row['longitude'], errors='coerce')
    #row['latitude'] = pd.to_numeric(row['latitude'], errors='coerce')
    return row

def clean_cabs_dt(x):
    return pd.to_datetime(x,
                     format="%Y-%m-%d %H:%M:%S",
                     errors='coerce')

def clean_num(x):
    return pd.to_numeric(x, errors='coerce')



def clean_transit(row: pd.Series) -> pd.Series:
    row['DATETIME'] = pd.to_datetime(str(row['DATE'])+str(row['TIME']),
                             format="%m/%d/%Y%H:%M:%S",
                             errors='coerce')
    row['EXITS'] = pd.to_numeric(row['EXITS'], errors='coerce')
    return row

#TODO
def clean_traffic():
    return
