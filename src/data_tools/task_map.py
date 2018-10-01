# coding=utf-8
from typing import Dict
from data_tools import row_operations as row_ops
import pandas as pd

task_type_map: Dict = {
                  'cl-gcabs': {
                                'in': 'gcabs',
                                'out': 'cl-gcabs',
                                'cols': {
                                        'lpep_dropoff_datetime':'dodatetime',
                                        'passenger_count':'passengers',
                                        'dolocationid':'dolocationid',
                                        'dropoff_longitude':'dolongitude',
                                        'dropoff_latitude':'dolatitude'
                                        },
                                'row_op': row_ops.clean_cabs,
                                'dates': {
                                        'parse': False
                                        },
                                'converters': {
                                        'dodatetime': row_ops.clean_cabs_dt,
                                        'passengers': row_ops.clean_num,
                                        'dolongitude': row_ops.clean_num,
                                        'dolatitude': row_ops.clean_num
                                        },
                                'dtypes': {
                                        'dodatetime': 'datetime64[ns]',
                                        'passengers': 'int64',
                                        'dolocationid': 'object',
                                        'dolongitude' : 'float64',
                                        'dolatitude' : 'float64'
                                        },
                                'index': {
                                        'col': 'dodatetime',
                                        'sorted': False
                                        }
                                },
                  'cl-ycabs': {
                                'in': 'ycabs',
                                'out': 'cl-ycabs',
                                'cols': {
                                        'tpep_dropoff_datetime':'dropoff_datetime',
                                        'passenger_count':'passenger_count',
                                        'dolocationid':'dolocationid'
                                        },
                                'row_op': row_ops.clean_cabs,
                                'dates': {
                                        'parse': False
                                        },
                                'converters': {
                                        'dropoff_datetime': row_ops.clean_cabs_dt,
                                        'passenger_count': row_ops.clean_num
                                        },
                                'dtypes': {
                                        'dropoff_datetime': 'datetime64[ns]',
                                        'passenger_count': 'int64',
                                        'dolocationid': 'object'
                                        },
                                'index': {
                                        'col': 'dropoff_datetime',
                                        'sorted': False
                                        }
                                },
                  'cl-transit': {
                                'in': 'transit',
                                'out': 'cl-transit',
                                'cols': {
                                        'station': 'station',
                                        'date': 'date',
                                        'time': 'time',
                                        'entries': 'entries',
                                        'exits': 'exits'
                                        },
                                'dates': {
                                        'parse': True,
                                        'in_cols': [1, 2],
                                        'out_col': 'datetime',
                                        'parser': row_ops.clean_transit_date
                                        },
                                'converters': {
                                        'entries': row_ops.clean_num,
                                        'exits': row_ops.clean_num
                                        },
                                'dtypes': {
                                        'station': 'object',
                                        'datetime': 'datetime64[ns]',
                                        'entries': 'int64',
                                        'exits': 'int64'
                                        },
                                'index': {
                                        'col': 'datetime',
                                        'sorted': True
                                        }
                                },
                  'cl-traffic': {
                                'in': 'gcabs',
                                'out': 'cl-gcabs',
                                'row_op': row_ops.clean_traffic,
                                'aggr_func': ''
                                },
                  'rs-gcabs': {
                                'in': 'cl-gcabs',
                                'out': 'rs-gcabs',
                                'dtypes': {
                                        'dodatetime': 'datetime64[ns]',
                                        'passengers': 'int64',
                                        'dolocationid': 'object',
                                        'dolongitude' : 'float64',
                                        'dolatitude' : 'float64'
                                        },
                                'index': {
                                        'col': 'dodatetime',
                                        'sorted': True
                                        },
                                'diff': {
                                        'compute': False
                                        },
                                'group': {
                                        'compute': True,
                                        'by_cols': ['dolocationid']
                                },
                                'aggr_func': sum
                                },
                  'rs-transit': {
                                'in': 'cl-transit',
                                'out': 'rs-transit',
                                'dtypes': {
                                        'station': 'object',
                                        'datetime': 'datetime64[ns]',
                                        'entries': 'int64',
                                        'exits': 'int64'
                                        },
                                'date_cols': ['datetime'],
                                'index': {
                                        'col': 'datetime',
                                        'sorted': True
                                        },
                                'diff': {
                                        'compute': True,
                                        'cols': ['exits', 'entries'],
                                        'new_cols': ['delex', 'delent']
                                        },
                                'group': {
                                        'compute': True,
                                        'by_cols': ['station']
                                        },
                                'aggr_func': sum
                                }
                  }