import boto3
from os import getenv
from dotenv import load_dotenv
import logging
from botocore.exceptions import ClientError
import argparse

load_dotenv()

parser = argparse.ArgumentParser()
parser.add_argument('bucket_name', type=str,
                    help='This is bucket name, please specify')

args = parser.parse_args()
s3 = boto3.client('s3')


def init_client():
    try:
        client = boto3.client("s3",
                              aws_access_key_id=getenv("aws_access_key_id"),
                              aws_secret_access_key=getenv(
                                  "aws_secret_access_key"),
                              aws_session_token=getenv("aws_session_token"),
                              region_name=getenv("aws_region_name"))
        client.list_buckets()

        return client
    except ClientError as e:
        logging.error(e)
    except:
        logging.error("Unexpected error")


def create_bucket(s3_client, bucket_name, region=getenv("aws_region_name")):
    try:
        location = {'LocationConstraint': region}
        response = s3_client.create_bucket(
            Bucket=args.bucket_name,
            CreateBucketConfiguration=location
        )
    except ClientError as e:
        logging.error(e)
        return False
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return True
    return False

def list_buckets(s3_client):
    try:
        return s3_client.list_buckets()
    except ClientError as e:
        logging.error(e)
        return False

def generate_public_read_policy(bucket_name):
    import json
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PublicReadGetObject",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": [
                    f"arn:aws:s3:::{bucket_name}/dev/*",
                    f"arn:aws:s3:::{bucket_name}/test/*"
                ]
            }
        ],
    }
    return json.dumps(policy)

def delete_bucket(s3_client, bucket_name):
    try:
        response = s3_client.delete_bucket(Bucket=args.bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return True
    return False


if __name__ == "__main__":
    s3_client = init_client()


response = s3_client.list_buckets()


for bucket in s3_client.list_buckets()['Buckets']:
    if bucket['Name'] == args.bucket_name:
        print(f'The bucket {args.bucket_name} already exists.')
        break
else:
    s3_client.create_bucket(Bucket=args.bucket_name)
    print(f'The bucket {args.bucket_name} has just been created.')

try:
    result = s3_client.get_bucket_policy(Bucket=args.bucket_name)
    print("The bucket policy already exists")
except:
    s3_client.put_bucket_policy(
        Bucket=args.bucket_name, Policy=generate_public_read_policy(
            args.bucket_name)
    )
    print("The bucket policy has just been created successfully")

for bucket in response['Buckets']:
    if bucket['Name'] == args.bucket_name:
        response = s3_client.delete_bucket(Bucket=args.bucket_name)
        print(f'bucket {args.bucket_name} has just been deleted')
        break
else:
    print(f'bucket {args.bucket_name} does not exist')
