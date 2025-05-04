import logging
import boto3
from botocore.exceptions import BotoCoreError, ClientError


logger = logging.getLogger(__name__)


def connect_s3(
    endpoint_url: str,
    access_key: str,
    secret_key: str,
    region_name="us-east-1"
):
    """
    Connect to S3/MinIO and return a boto3 client.

    Args:
        endpoint_url (str): URL of the S3/MinIO service.
        access_key (str): Access key ID.
        secret_key (str): Secret access key.
        region_name (str): AWS region (default is 'us-east-1').
        validate (bool): Whether to validate the connection immediately.

    Returns:
        boto3.client: An S3 client.

    Raises:
        Exception: If connection fails.
    """
    try:
        logger.info("Initializing S3 client...")
        s3 = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=endpoint_url,
            region_name=region_name
        )
        logger.info("Successfully connected to S3.")
        return s3
    except (BotoCoreError, ClientError) as e:
        logger.error(f"Failed to connect to S3: {e}")
        raise


def ensure_bucket_exists(
    s3_client: str,
    bucket_name: str
):
    """
    Ensure that the specified bucket exists in the S3 service.

    Args:
        s3_client (boto3.client): The S3 client.
        bucket_name (str): The name of the bucket to check or create.

    Raises:
        Exception: If there is an error checking or creating the bucket.
    """
    try:
        buckets = s3_client.list_buckets().get("Buckets", [])
        if not any(bucket["Name"] == bucket_name for bucket in buckets):
            s3_client.create_bucket(Bucket=bucket_name)
            logger.info(f"Bucket '{bucket_name}' created.")
        else:
            logger.info(f"Bucket '{bucket_name}' already exists.")
    except Exception as e:
        logger.error(f"Failed to check/create bucket: {e}")
        raise
