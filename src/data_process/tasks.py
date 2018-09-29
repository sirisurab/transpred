# read from task_type from sys argument
from typing import Dict
from data_process import row_operations as row_ops
task_type_map: Dict = {
                  'cl-gcabs': {
                                'in': 'gcabs',
                                'out': 'cl-gcabs',
                                'cols': {
                                        'lpep_dropoff_datetime':'dropoff_datetime',
                                        'passenger_count':'passenger_count',
                                        'dolocationid':'dolocationid'
                                        },
                                'row_op': row_ops.clean_cabs,
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
                                        },
                                'diff': {
                                        'compute': False
                                        },
                                'aggr_func': ''
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
                                        },
                                'diff': {
                                        'compute': False
                                        },
                                'aggr_func': ''
                                },
                  'cl-transit': {
                                'in': 'transit',
                                'out': 'cl-transit',
                                'cols': {
                                        'station':'station',
                                        'passenger_count':'passenger_count',
                                        'dropoff_longitude':'longitude',
                                        'dropoff_latitude':'latitude'
                                        },
                                'dtypes': {
                                        'dropoff_datetime': 'datetime64[ns]',
                                        'passenger_count': 'int64',
                                        'longitude': 'float64',
                                        'latitude': 'float64'
                                        },
                                'index': {
                                        'col': 'dropoff_datetime',
                                        'sorted': False
                                        },
                                'row_op': row_ops.clean_transit,
                                'diff': {
                                        'compute': True,
                                        'col': 'EXITS',
                                        'new_col': 'DELEXITS'
                                        },
                                'aggr_func': ''
                                },
                  'cl-traffic': {
                                'in': 'gcabs',
                                'out': 'cl-gcabs',
                                'row_op': row_ops.clean_traffic,
                                'aggr_func': ''
                                },
                  }