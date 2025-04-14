# processing/data_loader.py

import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "Documents")

def load_city_data():
    return pd.read_csv(os.path.join(DATA_DIR, "city_list.csv"))

def load_pincode_data():
    return pd.read_excel(os.path.join(DATA_DIR, "state_pincode_list.xlsx"))
