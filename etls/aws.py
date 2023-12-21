from utils.constants import AWS_ACCESS_KEY_ID, AWS_SECRET_KEY
import s3fs
import os

def connect_to_s3():
    try:
        print(AWS_ACCESS_KEY_ID, AWS_SECRET_KEY)
        s3 = s3fs.S3FileSystem(anon=False,key=AWS_ACCESS_KEY_ID,secret=AWS_SECRET_KEY)
    
        return s3
    except Exception as e:
        print(e)
        return None

def create_bucket_if_not_exists(s3, bucket_name):
    try:
        if not s3.exists(bucket_name):
            s3.mkdir(bucket_name)
            print(f"Bucket '{bucket_name}' created.")

        else:
            print(f"Bucket '{bucket_name}' already exists.")
    except Exception as e:
        print(e)

def upload_to_s3(s3, folder_path, bucket_name):
    try:
        files = os.listdir(folder_path)
        for file_name in files:

            if ".csv" not in file_name:
                continue

            file_path = f"{folder_path}/{file_name}"
            s3.put(file_path, f"{bucket_name}/raw/{file_name}")
            print("{file_name} uploaded to {bucket_name}")


    except FileNotFoundEError:
        print("File was not found.")