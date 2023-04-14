from time import localtime
from hashlib import md5
import boto3
from os import getenv
from dotenv import load_dotenv
import logging
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
import argparse
import json
import magic
import os
import logging
import errno
import sys
import os
import threading
import ntpath
from pathlib import Path
from hurry.filesize import size, si
from urllib.request import urlopen, Request
from random import choice


load_dotenv()

parser = argparse.ArgumentParser()
parser.add_argument('bucket_name', type=str,
                    help='This is bucket name, please specify')
parser.add_argument('--url', type=str,
                    help='Website link for downloading a file')
parser.add_argument('--file_name', "-fn", type=str,
                    help='Name of the uploaded file')
parser.add_argument('--file_path', "-fp", type=str, help='The path of file for uploading')
parser.add_argument('--threshold', "-mth", type=int, default=1024 *
                    1024 * 1024, help='Threshold in bytes (default values is 1GB)')
parser.add_argument('--days', '-d', type=int,
                    help='The amount of days after when object will be deleted')
parser.add_argument('-del', dest='delete',
                    action='store_true', help='Delete the file')

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


def download_upload(s3_client, bucket_name, url, file_name, keep_local=False):
    from urllib.request import urlopen
    import urllib
    import io
    url = args.url
    format = urllib.request.urlopen(url).info()['content-type']
    format = format.split('/')
    formatlist = ["jpg", "jpeg", "png", "webp", "mp4"]
    if format[1] in formatlist:
        with urlopen(args.url) as response:
            content = response.read()
            try:
                s3_client.upload_fileobj(
                    Fileobj=io.BytesIO(content),
                    Bucket=args.bucket_name,
                    ExtraArgs={'ContentType': 'image/jpg'},
                    Key=args.file_name
                )
                print("your picture has just been uploaded")
            except Exception as e:
                logging.error(e)

        if keep_local:
            with open(file_name, mode='wb') as jpg_file:
                jpg_file.write(content)
    else:
        print("uploading file with such extension is impossible")
    return "https://s3-{0}.amazonaws.com/{1}/{2}".format(
        'us-west-2',
        bucket_name,
        file_name
    )


def file_with_bigsize_upload(s3_client, bucket_name, file_name, file_path, threshold):
    config = TransferConfig(threshold=args.threshold)
    with open(args.filepath, 'rb') as f:
        object_key = args.file_name or args.file_path.split('/')[-1]
        s3_client.upload_fileobj(
            f, args.bucket_name, object_key, Config=config)
    print(f'{args.file_name} was uploaded successfully')


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

if args.tool == "download_upload":
    download_upload(s3_client, args.bucket_name, args.url,
                    args.file_name, keep_local=False)
if args.tool == 'file_with_bigsize_upload':
    meme = args.filepath.split('.')[-1]
    if args.memetype == meme:
        file_with_bigsize_upload(s3_client, args.bucket_name, args.file_name,
                                 args.filepath, args.threshold)
    else:
        print(f'{meme} uploading file with such exstension is impossible')
