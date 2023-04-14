import boto3
from os import getenv
from dotenv import load_dotenv
import logging
from botocore.exceptions import ClientError
import argparse

load_dotenv()

parser = argparse.ArgumentParser()
parser.add_argument('bucket_name', type=str)

args = parser.parse_args()
s3 = boto3.client('s3')


def init_client():
    try:
        client = boto3.client("s3",
                              aws_access_key_id=getenv("aws_access_key_id"),
                              aws_secret_access_key=getenv("aws_secret_access_key"),
                              aws_session_token=getenv("aws_session_token"),
                              region_name=getenv("aws_region_name"))
        client.list_buckets()

        return client
    except ClientError as e:
        logging.error(e)
    except:
        logging.error("Unexpected error")


if __name__ == "__main__":
    s3_client = init_client()


response = s3_client.list_buckets()

for bucket in response['Buckets']:
    if bucket['Name'] == args.bucket_name:
        response = s3_client.delete_bucket(Bucket=args.bucket_name)
        print(f'bucket {args.bucket_name} has just been deleted')
        break
else:
    print(f'bucket {args.bucket_name} does not exist')
