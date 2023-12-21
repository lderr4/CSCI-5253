import pandas as pd
from etls.extract import extract_open_weather_map_forecast_df
from etls.transform import transform_ski_weather_df
from etls.load import load_tables_to_csv
from utils.constants import OPEN_WEATHER_MAP_KEY, SKI_RESORT_INFO, OPEN_WEATHER_MAP_DATA_COLUMNS


def ski_resort_weather_pipeline(folder_postfix):
    
    # EXTRACT
    ski_weather_df = extract_open_weather_map_forecast_df(
        OPEN_WEATHER_MAP_KEY, SKI_RESORT_INFO, OPEN_WEATHER_MAP_DATA_COLUMNS)
    
    # TRANSFORM
    tables = transform_ski_weather_df(ski_weather_df)
    
    # LOAD 
    data_folder = load_tables_to_csv(tables, folder_postfix)

    return data_folder