from configparser import ConfigParser
import os

parser = ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), '../config/config.conf'))

OPEN_WEATHER_MAP_KEY = parser.get( "api_keys","open_weather_path_key")

DATABASE_HOST = parser.get("database","database_host")
DATABASE_NAME = parser.get("database","database_name")
DATABASE_PORT = parser.get("database","database_port")
DATABASE_USER = parser.get("database","database_username")
DATABASE_PASSWORD = parser.get("database","database_password")

INPUT_PATH = parser.get("file_paths", "input_path")
OUTPUT_PATH = parser.get("file_paths", "output_path")


# AWS
AWS_ACCESS_KEY_ID = parser.get("aws", "aws_access_key_id")
AWS_SECRET_KEY = parser.get("aws", "aws_secret_access_key")
AWS_REGION = parser.get("aws", "aws_region")
AWS_BUCKET_NAME = parser.get("aws", "aws_bucket_name")



SKI_RESORT_INFO = [
    ("Ikon", "Winter Park", (39.87652886804281, -105.76577005628859)),
    ("Ikon", "Copper Mountain", (39.49367530426312, -106.15941614917506)),
    ("Ikon", "Eldora", (39.93747528686545, -105.58400897925249)),
    ("Ikon", "Steamboat Springs", (40.466202977197106, -106.78094073252413)),
    ("Ikon", "Palisades Tahoe", (39.19114916529031, -120.2557470349228)),
    ("Ikon", "Big Sky", (45.293131560098956, -111.36294562989895)),
    ("Epic", "Northstar", (39.25671866393303, -120.13341133625593)),
    ("Ikon", "Crystal", (46.934754314303206, -121.48384975195034)),
    ("Ikon", "Jackson", (43.594634283058724, -110.84610943388475)),
    ("Epic", "Breckenridge", (39.478861079038985, -106.07839392176015)),
    ("Epic", "Vail", (39.619779887440146, -106.36957399501661)),
    ("Ikon", "Stratton", (43.110007073773104, -72.91026718465827)),
    ("Epic", "Whistler", (50.073500220248334, -122.95937601933981)),
    ("Ikon", "Charmonix", (45.96980068114292, 6.879032518531963))
]

OPEN_WEATHER_MAP_DATA_COLUMNS = [
        "pass",
        "resort",
        "date",
        "vis",
       "temp",
       "feels_like",
       "temp_min",
       "temp_max",
       "weather",
       "weather_description",
       "cloud_percent",
       "wind_speed",
       "latitude",
       "longitude"
       ]