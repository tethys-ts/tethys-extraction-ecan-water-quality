"""
Created on 2020-06-15

@author: MichaelK

"""
import os
import pandas as pd
import numpy as np
from datetime import datetime
from hilltoppy import web_service as ws
import json
import io
import zstandard as zstd
import boto3
import pickle
import yaml
from utils import list_parse_s3, write_pkl_zstd
import logging
from time import sleep

pd.options.display.max_columns = 10

run_time_start = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

logging.basicConfig(filename='water-quality.log', format='%(asctime)s: %(levelname)s: %(message)s', level=logging.INFO, filemode='w')

########################################
### Read in parameters
logging.info('--Read in parameters')

# mod_local_tz = 'Pacific/Auckland'
ts_local_tz = 'Etc/GMT-12'

base_dir = os.path.realpath(os.path.dirname(__file__))

with open(os.path.join(base_dir, 'parameters-b2.yml')) as param:
    param = yaml.safe_load(param)

# parser = argparse.ArgumentParser()
# parser.add_argument('yaml_path')
# args = parser.parse_args()
#
# with open(args.yaml_path) as param:
#     param = yaml.safe_load(param)

ts_summ_key_pattern = param['remote']['ts_summ_key_pattern']
ts_t_key_pattern = param['remote']['ts_t_key_pattern']

##########################################
### Backup

s3 = boto3.client(**param['remote']['connection_config'])

## Time series

try:
    logging.info('--Determining last mod dates')

    run_date = pd.Timestamp.today(tz='utc')
    run_date_local = run_date.tz_convert(ts_local_tz).tz_localize(None).strftime('%Y-%m-%d %H:%M:%S')
    run_date_key = run_date.strftime('%Y%m%dT%H%M%SZ')

    for hts in hts_dict:

        skp1 = ts_summ_key_pattern.split('{date}')[0].format(hts=hts)
        df1, last_mod_date = list_parse_s3(s3, param['remote']['bucket'], skp1, delimiter=param['remote']['delimiter'])

        # last_mod_date2 = (last_mod_date + pd.Timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')

        sites1 = ws.site_list(base_url, hts_dict[hts], location=True)
        sites2 = sites1[(sites1.Easting > 1000000) & (sites1.Northing > 5000000)].dropna().copy()

        mtypes_list = []
        for s in sites2.SiteName:
            m1 = ws.measurement_list(base_url, hts_dict[hts], s)
            mtypes_list.append(m1)
        mtypes_df = pd.concat(mtypes_list).reset_index()
        mtypes_df1 = mtypes_df.loc[mtypes_df.DataType == 'WQData', ['Site', 'Measurement', 'From', 'To']].copy()

        mtypes_df2 = mtypes_df1.copy()
        mtypes_df2['To'] = mtypes_df2['To'].dt.tz_localize(ts_local_tz).dt.tz_convert('utc')

        p_mtypes = write_pkl_zstd(mtypes_df2)

        if not df1.empty:
            d
        else:
            mtypes_df1['From'] = (last_mod_date + pd.Timedelta(seconds=1))

        ts_list = []
        for i, row in mtypes_df1.iterrows():
            print(i)
            try:
                ts_data = ws.get_data(base_url, hts_dict[hts], row.Site, row.Measurement, row.From, row.To)
            except ValueError as err:
                print(str(err))
            except ConnectionError as err:
                print(str(err))
                timer = 5
                while timer > 0:
                    sleep(2)
                    try:
                        ts_data = ws.get_data(base_url, hts_dict[hts], row.Site, row.Measurement, row.From, row.To)
                        break
                    except ConnectionError as err:
                        print(str(err))
                        timer = timer - 1


            ts_list.append(ts_data)
        ts_df = pd.concat(ts_list)





        # if last_mod_date == pd.Timestamp('1900-01-01 00:00:00'):










    last_mod_date2 = (last_mod_date + pd.Timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')

    ts_data = rd_sql(param['source']['server'], param['source']['database'], 'DTW_READINGS', ['WELL_NO', 'DATE_READ', 'DEPTH_TO_WATER', 'LAST_UPDATE'], where_in={'TIDEDA_FLAG': ['N']}, date_col='LAST_UPDATE', from_date=last_mod_date2, to_date=run_date_local)

    if not ts_data.empty:
        ts_data['DATE_READ'] = ts_data['DATE_READ'].dt.tz_localize(ts_local_tz).dt.tz_convert('utc')
        ts_data['LAST_UPDATE'] = ts_data['LAST_UPDATE'].dt.tz_localize(ts_local_tz).dt.tz_convert('utc')

        ## adjust water level to be below ground
        well_details = rd_sql(param['source']['server'], param['source']['database'], 'well_details', ['WELL_NO', 'ground_rl']).drop_duplicates('WELL_NO')
        well_details.loc[well_details.ground_rl.isnull(), 'ground_rl'] = 0

        ts_data1 = pd.merge(ts_data, well_details, on='WELL_NO', how='left')
        ts_data1['DEPTH_TO_WATER'] = ts_data1['DEPTH_TO_WATER'] - ts_data1['ground_rl']
        ts_data1.drop('ground_rl', axis=1, inplace=True)

        ## Add quality codes
        ts_data1['DEPTH_TO_WATER'] = ts_data1['DEPTH_TO_WATER'].astype('float')
        ts_data1['quality_code'] = 200
        ts_data1.loc[(ts_data1['DEPTH_TO_WATER'] > 999), 'quality_code'] = 201
        ts_data1.loc[(ts_data1['DEPTH_TO_WATER'] < -999), 'quality_code'] = 202
        ts_data1.loc[(ts_data1['DEPTH_TO_WATER'] > 999) | (ts_data1['DEPTH_TO_WATER'] < -999), 'DEPTH_TO_WATER'] = np.nan
        ts_data1['WELL_NO'] = ts_data1['WELL_NO'].str.upper().str.strip()

        ## Aggregate to ensure unique index
        grp1 = ts_data1.groupby(['WELL_NO', 'DATE_READ'])
        value1 = grp1['DEPTH_TO_WATER'].mean()
        qcode1 = grp1[['quality_code', 'LAST_UPDATE']].max()

        ts_data2 = pd.concat([value1, qcode1], axis=1).reset_index()

        ## Save data
        p_ts1 = write_pkl_zstd(ts_data2)

        skp4 = ts_t_key_pattern.format(date=run_date_key)

        p_ts2 = io.BytesIO(p_ts1)
        s3.upload_fileobj(Fileobj=p_ts2, Bucket=param['remote']['bucket'], Key=skp4, ExtraArgs={'Metadata': {'run_date': run_date_key}})
        logging.info(skp4)

    else:
        logging.info('No new data to update')

except Exception as err:
    print(err)
    logging.error('**TS backup failed: ' + str(err))

logging.info('--Success!')

