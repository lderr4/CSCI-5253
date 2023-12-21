import pandas as pd
from utils.constants import OUTPUT_PATH
import os

def load_tables_to_csv(tables, folder_postfix):

    folder_path = f"{OUTPUT_PATH}/{folder_postfix}"
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    
    for table_name, table in tables:
        file_path = f"{folder_path}/{table_name}.csv"
        table.to_csv(file_path, index=False)

    return folder_path