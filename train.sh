#!/bin/bash
jupyter nbconvert --to script ray_train.ipynb
python ray_train.py