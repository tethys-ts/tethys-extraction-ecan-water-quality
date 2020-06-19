#!/bin/bash
docker pull tethysts/tethys-extraction-ecan-water-quality:dev
docker rm wq_dev
docker run --name wq_dev -v /home/mike/git/tethys/tethys-extraction-ecan-water-quality/parameters-b2.yml:/parameters.yml tethysts/tethys-extraction-ecan-water-quality:dev
echo "Success!"
