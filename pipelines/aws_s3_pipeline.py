from etls.aws import connect_to_s3, create_bucket_if_not_exists, upload_to_s3
from utils.constants import AWS_BUCKET_NAME


def upload_s3_pipeline(ti):
    folder_path = ti.xcom_pull(
        task_ids="weather_extract_transform", key="return_value")
    
    s3 = connect_to_s3()

    create_bucket_if_not_exists(s3, AWS_BUCKET_NAME)

    upload_to_s3(s3, folder_path, AWS_BUCKET_NAME)
