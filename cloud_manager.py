import boto3
import logging
import os
import oss2


def upload_file(relative_path: str, *, content_type: str = None, bucket_path: str = None, delete: bool = True,
                new_filename: str = None, config: dict = None) -> str:
    """
    Upload a file to a bucket
    <call method> upload_file('gelm2uat', 'testing/test.html', 'temporary/data')

    :param config: credentials config
    :param new_filename: custom rename file when uploading to bucket
    :param relative_path: Filepath to upload 'files/excel/excel.xlsx'
    :param content_type: etc. "image/jpg"
    :param bucket_path: Location you want to put your files, on the cloud etc. 'temporary/data'
    :param delete: to delete after upload succeed
    :return:
    """
    directory, filename = os.path.split(relative_path)

    filename = new_filename if new_filename else filename
    # If bucket_path was not specified, use file_path
    bucket_path = relative_path if bucket_path is None else f"{bucket_path}/{filename}"
    url = ""

    cloud_provider = config["cloud_provider"]
    cloud_url = config["cloud_storage_url"]
    cloud_bucket = config["cloud_bucket"]

    if cloud_provider == "aws":
        if upload_file_aws(cloud_bucket, relative_path, bucket_path, config=config):
            url = f'{cloud_url.format(cloud_bucket)}{bucket_path}'
    elif cloud_provider == "alibaba":
        if upload_file_alibaba(cloud_bucket, relative_path, bucket_path, content_type=content_type, config=config):
            url = f"{cloud_url.format(cloud_bucket)}{bucket_path}"
    else:
        logging.error(f"Invalid 'cloud_provider': {cloud_provider}")
        return url

    if delete and os.path.exists(relative_path):
        os.remove(relative_path)

    return url


def upload_file_aws(bucket_name: str, relative_path: str, bucket_path: str = None, /, *, config) -> bool:
    """
    Upload a file to an S3 bucket
    :param config:
    :param relative_path: path to File to upload
    :param bucket_name: Bucket to upload to
    :param bucket_path: S3 object name, including path If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if bucket_path is None:
        bucket_path = relative_path

    # Upload the file
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=config["cloud_access_key_id"],
        aws_secret_access_key=config["cloud_secret_access_key"]
    )
    try:
        response = s3_client.upload_file(relative_path, bucket_name, bucket_path)
    except Exception as e:
        logging.error(e)
        return False
    return True


def upload_file_alibaba(bucket_name: str, relative_path: str, bucket_path: str, *, content_type: str = None, config) -> bool:
    """
    :return: True if file was uploaded, else False
    """
    if content_type is None:
        content_type = oss2.utils.content_type_by_name(relative_path)
    headers = {"Content-Type": content_type}

    auth = oss2.Auth(config["cloud_access_key_id"], config["cloud_secret_access_key"])
    bucket = oss2.Bucket(auth, config["cloud_storage_url"].replace("{}.", ""), bucket_name)
    try:
        result = bucket.put_object_from_file(bucket_path, relative_path, headers)
        print(f'Upload status: {result.status}')
        return True
    except Exception as e:
        logging.error(e)
        return False
