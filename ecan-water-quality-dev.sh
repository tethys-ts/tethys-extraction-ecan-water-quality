#!/bin/bash
docker pull dtok/water-data-allocation:dev
docker rm wdc_dev
docker run --name wdc_dev -v /home/mike/git/WaterDataConsents/parameters-dev.yml:/parameters.yml dtok/water-data-allocation:dev
echo "Success!"