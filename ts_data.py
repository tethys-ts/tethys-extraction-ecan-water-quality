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
from utils import list_parse_s3, write_pkl_zstd, compare_dfs
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
hts_dict = param['source']['hts']
base_url = param['source']['base_url']

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

        logging.info('--hts: ' + hts)

        skp1 = ts_summ_key_pattern.split('{date}')[0].format(hts=hts)
        df1, last_mod_date = list_parse_s3(s3, param['remote']['bucket'], skp1, delimiter=param['remote']['delimiter'])

        # last_mod_date2 = (last_mod_date + pd.Timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')

        sites1 = ws.site_list(base_url, hts_dict[hts], location=True)
        sites2 = sites1[(sites1.Easting > 1000000) & (sites1.Northing > 5000000)].dropna().copy()

        logging.info('-Running through site/measurement combos')

        mtypes_list = []
        for s in sites2.SiteName:
            m1 = ws.measurement_list(base_url, hts_dict[hts], s)
            mtypes_list.append(m1)
        mtypes_df = pd.concat(mtypes_list).reset_index()
        mtypes_df1 = mtypes_df.loc[mtypes_df.DataType == 'WQData', ['Site', 'Measurement', 'From', 'To']].copy()

        if not df1.empty:
            last_key = df1[df1.run_date == last_mod_date.tz_localize('utc')].iloc[0].Key
            obj1 = io.BytesIO()
            s3.download_fileobj(Bucket=param['remote']['bucket'], Fileobj=obj1, Key=last_key)
            obj1.seek(0)
            dctx = zstd.ZstdDecompressor()
            sdf1 = pickle.loads(dctx.decompress(obj1.read()))
            sdf1['To'] = sdf1['To'].dt.tz_convert(ts_local_tz).dt.tz_localize(None)
            sdf2 = sdf1.copy()
            sdf2['From'] = sdf1['To']
            sdf2.drop('To', axis=1, inplace=True)

            mtypes_df2 = pd.merge(sdf2, mtypes_df1.drop('From', axis=1), on=['Site', 'Measurement'], how='right')
            mtypes_df2 = mtypes_df2[mtypes_df2.To > mtypes_df2.From].copy()

        else:
            mtypes_df2 = mtypes_df1.copy()
            mtypes_df2['From'] = (last_mod_date + pd.Timedelta(seconds=1))

        if not mtypes_df2.empty:
            ts_list = []
            for i, row in mtypes_df2.iterrows():
                # print(i)
                try:
                    ts_data = ws.get_data(base_url, hts_dict[hts], row.Site, row.Measurement, row.From, row.To)
                except ValueError as err:
                    logging.warning(row.Site + ' and ' + row.Measurement + ' error: ' + str(err))
                except ConnectionError as err:
                    logging.warning(row.Site + ' and ' + row.Measurement + ' error: ' + str(err))
                    timer = 5
                    while timer > 0:
                        sleep(2)
                        try:
                            ts_data = ws.get_data(base_url, hts_dict[hts], row.Site, row.Measurement, row.From, row.To)
                            break
                        except ConnectionError as err:
                            logging.warning(row.Site + ' and ' + row.Measurement + ' error: ' + str(err))
                            timer = timer - 1
                except:
                    continue

                ts_list.append(ts_data)
            ts_df = pd.concat(ts_list)

            ## Save data
            p_ts1 = write_pkl_zstd(ts_df)

            skp4 = ts_t_key_pattern.format(hts=hts, date=run_date_key)

            p_ts2 = io.BytesIO(p_ts1)
            s3.upload_fileobj(Fileobj=p_ts2, Bucket=param['remote']['bucket'], Key=skp4, ExtraArgs={'Metadata': {'run_date': run_date_key}})

            mtypes_df2['From'] = mtypes_df2['From'].dt.tz_localize(ts_local_tz).dt.tz_convert('utc')
            mtypes_df2['To'] = mtypes_df2['To'].dt.tz_localize(ts_local_tz).dt.tz_convert('utc')

            p_mtypes = write_pkl_zstd(mtypes_df2)

            skp4 = ts_summ_key_pattern.format(hts=hts, date=run_date_key)

            p_mtypes2 = io.BytesIO(p_mtypes)
            s3.upload_fileobj(Fileobj=p_mtypes2, Bucket=param['remote']['bucket'], Key=skp4, ExtraArgs={'Metadata': {'run_date': run_date_key}})

        else:
            logging.info('No new data to update')

    logging.info('--Success!')

except Exception as err:
    print(err)
    logging.error('**TS backup failed: ' + str(err))



