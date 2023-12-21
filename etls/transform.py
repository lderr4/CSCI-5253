import pandas as pd

def transform_ski_weather_df(ski_df):
    ski_pass_df, ski_pass_map = prep_ski_pass_df(ski_df)
    ski_resort_df, ski_resort_map = prep_ski_resort_df(ski_df)
    weather_measurements_df = prep_weather_measurement_df(ski_df)
    weather_description_df, weather_map = prep_weather_description_df(ski_df)
    date_df, date_map = prep_date_df(ski_df)
    fact_df = prep_fact_df(ski_df, date_map, ski_pass_map, ski_resort_map, weather_map)
    
    tables = [("fact", fact_df), 
              ("ski_pass" , ski_pass_df), 
              ("ski_resorts", ski_resort_df), 
              ("weather_measures", weather_measurements_df), 
              ("weather_description", weather_description_df), 
              ("date", date_df)]
    return tables

def prep_date_df(df):
    date_df = pd.DataFrame()
    date_df['date'] = pd.to_datetime(df['date'].drop_duplicates())
    date_df['year'] = date_df['date'].dt.year
    date_df['month'] = date_df['date'].dt.month
    date_df['day'] = date_df['date'].dt.day
    date_df['date_id'] = date_df.index
    pk_map = {str(date): date_id for date, date_id in zip(date_df['date'], date_df['date_id'])}
    return date_df , pk_map

def prep_weather_description_df(df):
    weather_df = df[["weather", "weather_description"]].drop_duplicates().reset_index(drop=True)
    weather_df["weather_description_id"] = weather_df.index
    pk_map = {f"{w}, {wd}": pk for w, wd, pk in zip(weather_df['weather'], weather_df['weather_description'], weather_df['weather_description_id'])}
    return weather_df, pk_map
    
def prep_weather_measurement_df(df):
    
    weather_measurement_df = df[['vis', 'temp', 'feels_like', 'temp_min', 'temp_max', 'cloud_percent', 'wind_speed']].reset_index(drop=True)
    weather_measurement_df['weather_measurement_pk'] = weather_measurement_df.index
    # dont need a pk map because this table is always 1 to 1
    return weather_measurement_df


def prep_ski_pass_df(df):
    ski_pass_df = pd.DataFrame(df["pass"].drop_duplicates().reset_index(drop=True))
    ski_pass_df['ski_pass_id'] = ski_pass_df.index
    pk_map = {p: pk for p, pk in zip(ski_pass_df['pass'], ski_pass_df['ski_pass_id'])}
    return ski_pass_df, pk_map

def prep_ski_resort_df(df):
    ski_resort_df = df[['resort', 'latitude', 'longitude']].drop_duplicates().reset_index(drop=True)
    ski_resort_df['ski_resort_id'] = ski_resort_df.index
    
    pk_map = {p: pk for p, pk in zip(ski_resort_df['resort'], ski_resort_df['ski_resort_id'])}
    return ski_resort_df, pk_map


def prep_fact_df(ski_df, date_map, ski_pass_map, ski_resort_map, weather_map):
    fact = {
        "date_fk": [],
        "ski_pass_fk": [],
        "ski_resort_fk": [],
        "weather_description_fk":[],
        "weather_measurements_fk": []
    }
    for index, row in ski_df.iterrows():

        date = date_map[str(row['date'])]
        ski_pass = ski_pass_map[row['pass']]
        resort = ski_resort_map[row['resort']]
        weather_des = weather_map[f"{row['weather']}, {row['weather_description']}"]
        weather_measure = index

        fact['date_fk'].append(date)
        fact['ski_pass_fk'].append(ski_pass)
        fact['ski_resort_fk'].append(resort)
        fact['weather_description_fk'].append(weather_des)
        fact["weather_measurements_fk"].append(weather_measure)
    fact=pd.DataFrame(fact)
    return fact
