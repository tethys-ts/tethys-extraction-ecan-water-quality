#!/bin/bash
eval "$(conda shell.bash hook)"
conda activate ht_wq
python $PWD/main.py
echo "Success!"
