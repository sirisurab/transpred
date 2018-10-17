# coding=utf-8
from typing import Dict
from data_tools import row_operations as row_ops
from numpy import int64, float64, mean

task_type_map: Dict = {
                  'dl-transit': {
                                'in': 'transit',
                                'out': 'dl-transit'
                                },
                  'dl-traffic': {
                                'in': 'traffic',
                                'out': 'dl-traffic'
                                },
                  'dl-gcabs': {
                                'out': 'dl-gcabs',
                                'cols': {
                                        'lpep_dropoff_datetime': 'dodatetime',
                                        'lpep_pickup_datetime': 'pudatetime',
                                        'passenger_count': 'passengers',
                                        'trip_distance': 'distance',
                                        'dolocationid': 'dolocationid',
                                        'pulocationid': 'pulocationid',
                                        'dropoff_longitude': 'dolongitude',
                                        'dropoff_latitude': 'dolatitude',
                                        'pickup_longitude': 'pulongitude',
                                        'pickup_latitude': 'pulatitude'
                                        },
                                'dates': {
                                        'parse': True,
                                        'parser': row_ops.clean_cabs_dt
                                        },
                                'converters': {
                                        'passenger_count': row_ops.clean_num,
                                        'dolocationid': row_ops.clean_num,
                                        'pulocationid': row_ops.clean_num,
                                        'dropoff_longitude': row_ops.clean_num,
                                        'dropoff_latitude': row_ops.clean_num,
                                        'pickup_longitude': row_ops.clean_num,
                                        'pickup_latitude': row_ops.clean_num,
                                        'trip_distance': row_ops.clean_num
                                        }
                                },
                  'dl-ycabs': {
                                'out': 'dl-ycabs',
                                'cols': {
                                        'tpep_dropoff_datetime': 'dodatetime',
                                        'tpep_pickup_datetime': 'pudatetime',
                                        'passenger_count': 'passengers',
                                        'dolocationid': 'dolocationid',
                                        'pulocationid': 'pulocationid',
                                        'dropoff_longitude': 'dolongitude',
                                        'dropoff_latitude': 'dolatitude',
                                        'pickup_longitude': 'pulongitude',
                                        'pickup_latitude': 'pulatitude',
                                        'trip_distance': 'distance'
                                        },
                                'dates': {
                                        'parse': True,
                                        'parser': row_ops.clean_cabs_dt
                                        },
                                'converters': {
                                        'passenger_count': row_ops.clean_num,
                                        'dropoff_longitude': row_ops.clean_num,
                                        'dropoff_latitude': row_ops.clean_num,
                                        'pickup_longitude': row_ops.clean_num,
                                        'pickup_latitude': row_ops.clean_num,
                                        'trip_distance': row_ops.clean_num
                                        }
                                },
                  'cl-gcabs': {
                                'in': 'dl-gcabs',
                                'out': 'cl-gcabs',
                                'cols': {
                                        'lpep_dropoff_datetime':'dodatetime',
                                        'passenger_count':'passengers',
                                        'dolocationid':'dolocationid',
                                        'dropoff_longitude':'dolongitude',
                                        'dropoff_latitude':'dolatitude'
                                        },
                                'row_op': {
                                        'compute': True,
                                        'func': row_ops.clean_cabs
                                        },
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
                                'in': 'dl-ycabs',
                                'out': 'cl-ycabs',
                                'cols': {
                                        'tpep_dropoff_datetime':'dodatetime',
                                        'passenger_count':'passengers',
                                        'dolocationid':'dolocationid',
                                        'dropoff_longitude':'dolongitude',
                                        'dropoff_latitude':'dolatitude'
                                        },
                                'row_op': {
                                        'compute': True,
                                        'func': row_ops.clean_cabs
                                        },
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
                  'cl-transit': {
                                'in': 'dl-transit',
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
                                'row_op': {
                                        'compute': False
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
                                'in': 'dl-traffic',
                                'out': 'cl-traffic',
                                'row_op': row_ops.clean_traffic,
                                'aggr_func': ''
                                },
                  'rs-gcabs': {
                                'in': 'cl-gcabs',
                                'out': 'rs-gcabs',
                                'index': {
                                        'col': 'dodatetime',
                                        'sorted': False
                                        },
                                'group': {
                                        'compute': True,
                                        'by_cols': ['dolocationid', 'pulocationid'],
                                        'aggr_func': sum,
                                        'meta': {
                                            'passengers': int64,
                                            'distance': float64
                                            }
                                        }
                                },
                  'rs-ycabs': {
                                'in': 'cl-ycabs',
                                'out': 'rs-ycabs',
                                'index': {
                                        'col': 'dodatetime',
                                        'sorted': False
                                        },
                                'group': {
                                        'compute': True,
                                        'by_cols': ['dolocationid', 'pulocationid'],
                                        'aggr_func': sum,
                                        'meta': {
                                            'passengers': int64,
                                            'distance': float64
                                            }
                                        }
                                },
                  'rs-transit': {
                                'in': 'cl-transit',
                                'out': 'rs-transit',
                                'index': {
                                        'col': 'datetime',
                                        'sorted': True
                                        },
                                'group': {
                                        'compute': True,
                                        'by_cols': ['station'],
                                        'aggr_func': sum,
                                        'meta': {
                                            'datetime': 'datetime64[ns]',
                                            'station': object,
                                            'delex': int64,
                                            'delent': int64
                                            }
                                        }
                                },
                  'rs-traffic': {
                                'in': 'dl-traffic',
                                'out': 'rs-traffic',
                                'index': {
                                        'col': 'datetime',
                                        'sorted': False
                                        },
                                'group': {
                                        'compute': True,
                                        'by_cols': ['linkid'],
                                        'aggr_func': mean,
                                        'meta': {
                                            'speed': float64,
                                            'traveltime': float64
                                            }
                                        }
                                },
                  'rg-transit': {
                                'in': 'rs-transit',
                                'out': 'rg-transit',
                                'split_by': ['station'],
                                'date_cols': ['datetime'],
                                'dtypes': {
                                        'station': object,
                                        'delex': int64,
                                        'delent': int64
                                        }
                                },
                  'rg-gcabs': {
                                'in': 'rs-gcabs',
                                'out': 'rg-gcabs',
                                'split_by': ['dolocationid'],
                                'date_cols': ['dodatetime'],
                                'dtypes': {
                                        'passengers': int64,
                                        'dolocationid': int64,
                                        'pulocationid': int64,
                                        'distance': float64
                                        }
                                },
                  'rg-ycabs': {
                                'in': 'rs-ycabs',
                                'out': 'rg-ycabs',
                                'split_by': ['dolocationid'],
                                'date_cols': ['dodatetime'],
                                'dtypes': {
                                        'passengers': int64,
                                        'dolocationid': int64,
                                        'pulocationid': int64,
                                        'distance': float64
                                        }
                                },
                  'rg-traffic': {
                                'in': 'rs-traffic',
                                'out': 'rg-traffic',
                                'split_by': ['linkid'],
                                'date_cols': ['datetime'],
                                'dtypes': {
                                        'speed': float64,
                                        'traveltime': float64,
                                        'linkid': int64
                                        }
                                },
                  'pl-1M-16-17':      {
                                'in': ['rg-gcabs'],
                                'freq': '1M',
                                'range': ['2016', '2017']
                                },
                  'pl-1W-16-17':      {
                                'in': ['rg-gcabs', 'rg-traffic', 'rg-gcabs', 'rg-ycabs'],
                                'freq': '1W',
                                'range': ['2016', '2017']
                                }
                  }