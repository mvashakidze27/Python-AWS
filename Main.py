from time import localtime
from hashlib import md5
import boto3
from os import getenv
from dotenv import load_dotenv
import logging
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
import argparse
import magic
from datetime import date, datetime
import logging
from pathlib import Path

load_dotenv()

parser = argparse.ArgumentParser()
parser.add_argument('bucket_name', type=str,
                    help='This is bucket name, please specify')
parser.add_argument('--url', type=str,
                    help='Website link for downloading a file')
parser.add_argument('--file_name', "-fn", type=str,
                    help='Name of the uploaded file')
parser.add_argument('--file_path', "-fp", type=str,
                    help='The path of file for uploading')
parser.add_argument('--threshold', "-mth", type=int, default=1024 *
                    1024 * 1024, help='Threshold in bytes (default values is 1GB)')
parser.add_argument('--days', '-d', type=int,
                    help='The amount of days after when object will be deleted')
parser.add_argument('-del', dest='delete',
                    action='store_true', help='Delete the file')
parser.add_argument('-vers', dest='versioning',
                    action='store_true', help='to check version')
parser.add_argument('-verslist', dest='versionlist',
                    action='store_true', help='version list')
parser.add_argument('-previous_version', dest='previous_version',
                    action='store_true', help='Returning back to previous version')


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
            Bucket=bucket_name,
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
        response = s3_client.delete_bucket(Bucket=bucket_name)
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
    format = urllib.request.urlopen(url).info()['content-type']
    format = format.split('/')
    formatlist = ["jpg", "jpeg", "png", "webp", "mp4"]
    if format[1] in formatlist:
        with urlopen(url) as response:
            content = response.read()
            try:
                s3_client.upload_fileobj(
                    Fileobj=io.BytesIO(content),
                    Bucket=bucket_name,
                    ExtraArgs={'ContentType': 'image/jpg'},
                    Key=file_name
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
    config = TransferConfig(threshold=threshold)
    with open(filepath, 'rb') as f:
        object_key = file_name or file_path.split('/')[-1]
        s3_client.upload_fileobj(
            f, bucket_name, object_key, Config=config)
    print(f'{file_name} was uploaded successfully')


def create_bucket_policy(s3_client, bucket_name):
    s3_client.put_bucket_policy(
        Bucket=bucket_name, Policy=generate_public_read_policy(
            bucket_name)
    )
    print("Bucket policy was created successfully")


def read_bucket_policy(s3_client, bucket_name):
    try:
        policy = s3_client.get_bucket_policy(Bucket=bucket_name)
        policy_str = policy["Policy"]
        print(policy_str)
    except ClientError as e:
        logging.error(e)
        return False


def set_object_access_policy(s3_client, bucket_name, file_name):
    try:
        response = s3_client.put_object_acl(
            ACL="public-read",
            Bucket=bucket_name,
            Key=file_name
        )
    except ClientError as e:
        logging.error(e)
        return False
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return True
    return False


def lifecycle(s3_client, bucket_name, days):
    lifecycle_config = {
        'Rules': [
            {
                'ID': 'Delete after {} days'.format(days),
                'Status': 'Enabled',
                'Prefix': '',
                'Expiration': {
                    'Days': days
                }
            }
        ]
    }
    s3_client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle_config)
    print(f'Bucket {bucket_name} will be deleted in {days} days ')


def delete_file(s3_client, bucket_name, file_name):
    response = s3_client.delete_object(
        Bucket=bucket_name, Key=file_name)
    print(f'{file_name} file has just been deleted')


def previous_version(s3_client, bucket_name, file_name):
    try:
        response = s3_client.get_object(
            Bucket=bucket_name, Key=file_name)
        latest_version_id = response['VersionId']
        response = s3_client.list_object_versions(
            Bucket=bucket_name, Prefix=file_name)
        previous_version_id = response['Versions'][1]['VersionId']
        response = s3_client.copy_object(
            Bucket=bucket_name,
            Key=file_name,
            CopySource={'Bucket': bucket_name, 'Key': file_name, 'VersionId': previous_version_id})
        print(f'{file_name} converted to the previous version')
    except:
        print(f'{file_name} couldn\'t convert to the previous version')


def list_of_versions(s3_client, bucket_name, file_name):
    response = s3_client.list_object_versions(
        Bucket=bucket_name, Prefix=file_name)
    num_versions = len(response['Versions'])
    dates = [v['LastModified'] for v in response['Versions']]
    print(f'Bucket name: {bucket_name}')
    print(f'File name: {file_name}')
    print(f'Number of versions: {num_versions}')
    print('Creation dates of versions:')
    for date in dates:
        print(date)


def versioning(s3_client, bucket_name):
    output = s3_client.get_bucket_versioning(Bucket=bucket_name,)
    try:
        status = output['Status']
        print(f'On bucket {bucket_name} versioning is turned on')
    except:
        print(f'On bucket {bucket_name} versioning is turned off')


def upload_file_to_s3_with_magic(s3_client, bucket_name, file_name, file_path):
    file_mime_type = magic.from_file(file_path, mime=True)
    file_extension = file_mime_type.split('/')[-1] + '/' + file_name

    with open(file_path, 'rb') as file:
        s3_client.upload_fileobj(file, bucket_name, file_extension)
        print(f'{file_name} has been successfully uploaded to {bucket_name}')


def delete_old_versions(s3_client, bucket_name, file_name, days):
    response = s3_client.list_object_versions(
        Bucket=bucket_name, Prefix=file_name)
    versions = response.get('Versions', [])
    today = str(date.today())

    for version in versions:
        version_id = version['VersionId']
        response = s3_client.head_object(
            Bucket=bucket_name, Key=file_name, VersionId=version_id)
        last_modified = response['LastModified']
        last_modified_date = str(last_modified.date())
        last_modified_datetime = datetime.strptime(
            last_modified_date, "%Y-%m-%d")
        today_datetime = datetime.strptime(today, "%Y-%m-%d")
        delta = today_datetime - last_modified_datetime

        if delta.days > days:
            s3_client.delete_object(
                Bucket=bucket_name, Key=file_name, VersionId=version_id)
            print(
                f'The version {version_id} has been deleted as it was created more than {days} days ago')


def upload_html_file(s3_client, bucket_name, file_path, object_key):
    with open(file_path, 'rb') as f:
        s3_client.upload_fileobj(f, bucket_name, object_key, ExtraArgs={
            'ContentType': 'text/html'})


def configure_static_website(s3_client, bucket_name):
    website_configuration = {
        'ErrorDocument': {'Key': 'error.html'},
        'IndexDocument': {'Suffix': 'index.html'},
    }
    s3_client.put_bucket_website(
        Bucket=bucket_name, WebsiteConfiguration=website_configuration)
    print("Static website hosted successfully!")


def upload_source_to_s3(s3_client, bucket_name, local_folder_path):
    for root, dirs, files in os.walk(local_folder_path):
        for file in files:
            local_file_path = os.path.join(root, file)
            s3_key = os.path.relpath(
                local_file_path, local_folder_path).replace('\\', '/')
            s3_client.upload_source(local_file_path, bucket_name, s3_key, ExtraArgs={
                'ContentType': 'text/html'})


def create_and_configure_bucket(s3_client, bucket_name):
    s3_client.create_bucket(Bucket=bucket_name)
    create_bucket_policy(s3_client, bucket_name)
    configure_static_website(s3_client, bucket_name)


def get_s3_website_url(s3_client, bucket_name):
    response = s3_client.get_bucket_location(Bucket=bucket_name)
    region = response.get('LocationConstraint', 'us-east-1')
    return f"http://{bucket_name}.s3-website-{region}.amazonaws.com"


def get_quote_stats(quotes):
    stats = {"quotes": 0}

    for index, quote in enumerate(quotes):
        author = quote["author"]
        if author not in stats:
            stats[author] = {"quote_index": [index], "quotes_available": 1}
        else:
            stats[author]["quote_index"].append(index)
            stats[author]["quotes_available"] += 1

        stats["quotes"] += 1

    return stats


def main():
    headers = {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
    }

    with urlopen(Request("https://type.fit/api/quotes", data=None, headers=headers)) as response:
        quotes = json.loads(response.read().decode())

        if args.inspire == "true":
            print(json.dumps(choice(quotes)["text"], indent=4))
        else:
            quote_stats = get_quote_stats(quotes)

            for quote in quotes:
                if quote["author"] == args.inspire:
                    print(quote["text"])
                    if args.save:
                        with open("quotes.json", "w") as f:
                            json.dump(quote, f)

                        with open("quotes.json", "rb") as f:
                            s3_client.upload_fileobj(
                                f, args.bucket_name, "quotes.json")
                            print(
                                f'quotes.json was uploaded to {args.bucket_name} bucket')

                    break


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

if args.tool == 'read_bucket_policy' or args.tool == 'rbp':
    read_bucket_policy(s3_client, args.bucket_name)
if args.tool == 'create_bucket_policy' or args.tool == 'cbp':
    create_bucket_policy(s3_client, args.bucket_name)
if args.tool == "download_upload":
    download_upload(s3_client, args.bucket_name, args.url,
                    args.file_name, keep_local=False)
if args.tool == 'lifecycle':
    lifecycle(s3_client, args.bucket_name, args.days)
if args.tool == 'file_with_bigsize_upload':
    meme = args.filepath.split('.')[-1]
    if args.memetype == meme:
        file_with_bigsize_upload(s3_client, args.bucket_name, args.file_name,
                                 args.filepath, args.threshold)
    else:
        print(f'{meme} uploading file with such exstension is impossible')
if args.delete == True:
    delete_file(s3_client, args.bucket_name, args.file_name)
if args.versioning == True:
    versioning(s3_client, args.bucket_name)
if args.versionlist == True:
    list_of_versions(s3_client, args.bucket_name, args.file_name)
if args.previous_version == True:
    previous_version(s3_client, args.bucket_name, args.file_name)
if args.tool == "upload_file_to_s3_with_magic":
    upload_file_to_s3_with_magic(s3_client, args.bucket_name,
                                 args.file_name, args.filepath)
if args.tool == "delete_old_versions":
    delete_old_versions(args.s3_client, args.bucket_name,
                        args.file_name, args.days)
if args.tool == "static_website":
    upload_html_file(s3_client, args.bucket_name,
                     args.filepath, args.file_name)
    configure_static_website(s3_client, args.bucket_name)
if args.tool == "upload_and_host":
    create_and_configure_bucket(args.s3_client, args.bucket_name)
    upload_source_to_s3(args.s3_client, args.bucket_name, args.filepath)
    print(get_s3_website_url(args.s3_client, args.bucket_name))
