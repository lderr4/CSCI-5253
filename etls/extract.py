import requests
import pandas as pd


def extract_open_weather_map_forecast_df(api_key, resorts, cols):
    '''
    api_key: openweathermap.org api key
    cities: list of structure: [resort_name, (latitude, long)]
    '''

    weather_data = {col: [] for col in cols}

    for pass_name, name, (lat, lon) in resorts:

        url = f"http://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={api_key}"
        
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
        
        else:
            print(f'Error fetching weather data: \nresponse: {response}\nurl: {url}')
            continue

        weather_data = process_response(data, weather_data, (pass_name, name, lat, lon))
        
        
    return pd.DataFrame(weather_data)


def process_response(response_data, weather_data, resort_info):
    pass_name, name, lat, lon = resort_info
    for x in response_data['list']:

        # date
        date = x['dt_txt']
        
        # visibility
        visibility = x['visibility']
        
        # temperature
        temp = x['main']['temp']
        feels_like = x['main']['feels_like']
        temp_min = x['main']['temp_min']
        temp_max = x['main']['temp_max']
        
        # weather
        weather = x['weather'][0]['main']
        desc = x['weather'][0]['description']
        cloud_percent = x['clouds']['all']
        wind_speed = x['wind']['speed']
        
        weather_data['pass'].append(pass_name)
        weather_data['resort'].append(name)
        weather_data['latitude'].append(lat)
        weather_data['longitude'].append(lon)
        weather_data['date'].append(date)
        weather_data['vis'].append(visibility)
        weather_data['temp'].append(temp)
        weather_data['feels_like'].append(feels_like) 
        weather_data['temp_min'].append(temp_min) 
        weather_data['temp_max'].append(temp_max) 
        weather_data['weather'].append(weather) 
        weather_data['weather_description'].append(desc) 
        weather_data['cloud_percent'].append(cloud_percent) 
        weather_data['wind_speed'].append(wind_speed) 
    
    return weather_data
    