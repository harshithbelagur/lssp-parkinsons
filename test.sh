#!/bin/bash

# Upgrade Python to the latest version
sudo apt update
sudo apt install python3 python3-pip
sudo apt upgrade python3

# Install or upgrade pip
python3 -m pip install --upgrade pip

# Upgrade scikit-learn to the latest version
pip install --upgrade scikit-learn

pip install xgboost lightgbm