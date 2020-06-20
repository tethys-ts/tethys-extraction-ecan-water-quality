# -*- coding: utf-8 -*-
"""
Created on Mon Feb  3 13:56:21 2020

@author: MichaelEK
"""
import pickle
import os
import argparse
import pandas as pd
import numpy as np
import yaml
from site_data import get_site_data
from ts_data import get_ts_data
import logging

#########################################
### Get todays date-time

pd.options.display.max_columns = 10
#run_time_start = pd.Timestamp.today().strftime('%Y-%m-%d %H:%M:%S')
#print(run_time_start)

logging.basicConfig(filename='water-quality.log', format='%(asctime)s: %(levelname)s: %(message)s', level=logging.INFO, filemode='w')

########################################
### Read in parameters
print('---Read in parameters')

base_dir = os.path.realpath(os.path.dirname(__file__))

with open(os.path.join(base_dir, 'parameters-b2.yml')) as param:
   param = yaml.safe_load(param)
#d
# parser = argparse.ArgumentParser()
# parser.add_argument('yaml_path')
# args = parser.parse_args()
#
# with open(args.yaml_path) as param:
#     param = yaml.safe_load(param)

########################################
### Run the process

print('---Process the sites')
new_sites = get_site_data(param)

print('---Process the time series')
get_ts_data(param)
