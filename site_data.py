"""

"""
import os
import pandas as pd
from hilltoppy import web_service as ws
import json
import io
import zstandard as zstd
import boto3
import pickle
from utils import list_parse_s3, write_pkl_zstd
import logging

pd.options.display.max_columns = 10


def get_site_data(param):
    """

    """
    ########################################
    ### Read in parameters
    logging.info('--Read in parameters')

    ts_local_tz = 'Etc/GMT-12'

    # base_dir = os.path.realpath(os.path.dirname(__file__))
    #
    # with open(os.path.join(base_dir, 'parameters-b2.yml')) as param:
    #     param = yaml.safe_load(param)

    # parser = argparse.ArgumentParser()
    # parser.add_argument('yaml_path')
    # args = parser.parse_args()
    #
    # with open(args.yaml_path) as param:
    #     param = yaml.safe_load(param)

    hts_dict = param['source']['hts']
    base_url = param['source']['base_url']

    ##########################################
    ### Sites Backup

    s3 = boto3.client(**param['remote']['connection_config'])

    logging.info('-- Run through the sites table')
    try:
        run_date = pd.Timestamp.today(tz='utc')
        run_date_local = run_date.tz_convert(ts_local_tz).tz_localize(None).strftime('%Y-%m-%d %H:%M:%S')
        run_date_key = run_date.strftime('%Y%m%dT%H%M%SZ')

        for hts in hts_dict:
            logging.info('hts category: ' + hts)

            sites1 = ws.site_list(base_url, hts_dict[hts], location=True)
            sites2 = sites1[(sites1.Easting > 1000000) & (sites1.Northing > 5000000)].dropna().copy()
            sites2['Easting'] = sites2['Easting'].round().astype(int)
            sites2['Northing'] = sites2['Northing'].round().astype(int)

            if not sites2.empty:
                logging.info('Sites will be updated')

                skp2 = param['remote']['site_key_pattern'].format(hts=hts)

                p_sites = write_pkl_zstd(sites2)
                p_sites2 = io.BytesIO(p_sites)
                s3.upload_fileobj(Fileobj=p_sites2, Bucket=param['remote']['bucket'], Key=skp2, ExtraArgs={'Metadata': {'run_date': run_date_key}})
                logging.info(skp2)
            else:
                print('- No new sites to update.')
                logging.info('- No new sites to update.')
    except Exception as err:
        logging.error('-- Sites backup failed: ' + str(err))

    print('-- Success!')
    logging.info('-- Success!')

    return sites2
