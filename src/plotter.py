import sys
from typing import List, Dict
from data_tools import task_map
from utils import persistence as ps
from urllib3.response import HTTPResponse
from pandas import DataFrame, read_csv, concat
from bokeh.plotting import figure, output_file, show

RGTRANSIT_BUCKET: str = 'rg-transit'
REFBASE_BUCKET: str = 'ref-base'
GEOMERGED_PATH: str = 'geo-merged/'
PLOTS_BUCKET: str = 'plots'

def plot(*args) -> bool:
    inputs: List[str] = list(*args)
    task: str = inputs[0]
    buffer: float = float(inputs[1])
    print('plotting task %(task)s and buffer-radius %(buffer)s for stations %(stations)s'
          % {'task': task, 'buffer': buffer, 'stations': inputs[2:]})
    # read in and out buckets, freq and range for task from task_map
    in_buckets: List[str] = task_map.task_type_map[task]['in']
    #out_bucket: str = task_map.task_type_map[task]['out']
    freq: str = task_map.task_type_map[task]['freq']
    range: List[str] = task_map.task_type_map[task]['range']
    geomerged_filename: str = GEOMERGED_PATH+str(buffer)+'.csv'

    # load ref-base geomerged file
    filestream: HTTPResponse = ps.get_file_stream(bucket=REFBASE_BUCKET, filename=geomerged_filename)
    dtypes: Dict[str, str] = {
        #'stationid': 'int64',
        'stop_name': 'object',
        'tsstation': 'object',
        'dolocationid': 'int64'
    }
    geomerged_df: DataFrame = read_csv(filestream, usecols=dtypes.keys(), encoding='utf-8', dtype=dtypes)

    for station in inputs[2:]:
        try:
            # determine filename of transit data for
            # the current station in the rg-transit bucket
            # replace '/' in station with ' '
            ts_filename: str = station.replace('/', ' ').upper()

            # read transit data for station (rg-transit bucket)
            filestream = ps.get_file_stream(bucket=RGTRANSIT_BUCKET, filename=ts_filename)
            ts_datecols = ['datetime']
            dtypes = {
                                     #'datetime': 'datetime64',
                                     #'station': 'object',
                                     'delex': 'int64',
                                     'delent': 'int64'
                                    }
            transit_df: DataFrame = read_csv(filestream, usecols=ts_datecols + list(dtypes.keys()),
                                             parse_dates=ts_datecols,
                                             encoding='utf-8', dtype=dtypes)

            #transit_df = transit_df.set_index('datetime').sort_index().reset_index()

            # read data from other in buckets
            gcabs_df: DataFrame
            cabs_datecols = ['dodatetime']
            for bucket in in_buckets:
                if bucket == 'rg-gcabs':
                    # determine relevant cabs files
                    # by finding dolocationids corresponding
                    # to current station from ref-base geomerged df
                    dolocationids = geomerged_df.loc[geomerged_df.tsstation == station]['dolocationid']

                    #for zone in dolocationids:
                    #filestream = ps.get_file_stream(bucket=bucket, filename=zone)
                    dtypes = {
                        #'dodatetime': 'datetime64',
                        #'dolocationid': 'int64',
                        'passengers': 'int64'
                    }
                    gcabs_df = concat([read_csv(ps.get_file_stream(bucket=bucket, filename=str(locationid)),
                                                           usecols=cabs_datecols + list(dtypes.keys()),
                                                           parse_dates=cabs_datecols,
                                                           encoding='utf-8', dtype=dtypes)
                                                  for locationid in dolocationids],
                                                 ignore_index=True)

                    gcabs_df = gcabs_df.groupby(cabs_datecols).apply('sum').\
                        set_index(cabs_datecols).sort_index().reset_index()

            # create plots
            plot_filepath: str = task+'/'+str(buffer)+'/'
            plot_filename: str = station+'.html'
            tmp_filepath: str = '/tmp/'+plot_filename
            output_file(tmp_filepath)
            p = figure(title='plot for station '+station, x_axis_label='datetime', y_axis_label='')

            p.line(transit_df[ts_datecols[0]], transit_df['delex'], label='transit exits', line_width=2)
            p.line(transit_df[ts_datecols[0]], transit_df['delent'], label='transit entries', line_width=2)
            p.line(gcabs_df[cabs_datecols[0]], gcabs_df['passengers'], label='cab droppoffs', line_width=2)

            show(p)

            # save plots in out bucket
            ps.copy_file(dest_bucket=PLOTS_BUCKET, file=plot_filepath+plot_filename, source=tmp_filepath)

        except Exception as err:
            print('Error in plotting task %(task)s for station %(station)s'
                  % {'task': task, 'station': station})
            raise err

    return True


if __name__ == '__main__':
    status: bool = plot(sys.argv[1:])
