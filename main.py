import boto3, os, argparse

def make_directories(key: str, parent: str) -> None:
    path = key.split('/')
    current_directory = None
    for index, current_dir in enumerate(path):
        if index == (len(path) - 1): break
        if index == 0:
            current_directory = current_dir
        else:
            current_directory = os.path.join(current_directory, current_dir)
        if not os.path.isdir(os.path.join(parent, current_directory)):
            os.mkdir(os.path.join(parent, current_directory))

def main(bucket: str, prefix: str = None, dl_path: str = '.') -> None:
    dl_path = os.path.abspath(dl_path)
    if not os.path.isdir(dl_path):
        os.mkdir(dl_path)

    s3 = boto3.resource(
        's3',
        endpoint_url=os.getenv('S3_ENDPOINT'),
        aws_access_key_id=os.getenv('S3_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('S3_SECRET_KEY'),
        region_name=os.getenv('S3_REGION', None)
    )

    bucket_ = s3.Bucket(bucket)

    all_objects = lambda : bucket_.objects.all() if not prefix else bucket_.objects.filter(Prefix=prefix)

    for object in all_objects():
        make_directories(object.key, dl_path)
        download_abs_path = os.path.join(dl_path, object.key)
        bucket_.download_file(object.key, download_abs_path)
        

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='PYS3DL',
        description='Download files and directories from s3 compatible storages',
        usage="python main.py --bucket BUCKET --prefix PREFIX"
    )

    parser.add_argument('--bucket', '-b', type=str, help='Specify bucket name', required=True)
    parser.add_argument('--prefix', '-p', type=str, help='Specify Key prefix')
    parser.add_argument('--dl-path', '-d', type=str, help='Path to store downloaded files')
    
    args = parser.parse_args()

    main(args.bucket, args.prefix, args.dl_path)
