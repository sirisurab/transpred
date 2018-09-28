from minio import Minio
from typing import Dict, Union
from minio.error import ResponseError, BucketAlreadyExists, BucketAlreadyOwnedByYou
KEY: str = 'minio'
SECRET: str = 'minio123'
ENDPOINT: str = 'minio:9000'
USE_SSL: bool = False

def get_client():
    return Minio(ENDPOINT,
                      access_key=KEY,
                      secret_key=SECRET,
                      secure=USE_SSL)

def fetch_s3_options() -> Dict[str, Union[str, bool, Dict[str, str]]]:
    return {
        'anon': False,
        'use_ssl': USE_SSL,
        'key': KEY,
        'secret': SECRET,
        'client_kwargs':{
            'region_name': 'us-east-1',
            'endpoint_url': 'http://'+ENDPOINT
        }
    }

def copy_files(source_folder:str, dest_bucket:str) -> bool:
    mc = get_client()
    print('created minio client')
    try:
        mc.make_bucket(dest_bucket)
        print('made bucket '+dest_bucket)
    except BucketAlreadyOwnedByYou as err:
        pass
    except BucketAlreadyExists as err:
        pass
    except ResponseError as err:
        raise err

    try:
        mc.copy_object(bucket_name=dest_bucket, object_source=source_folder)
        print('copied from '+source_folder+' to bucket '+dest_bucket)
    except ResponseError as err:
        raise err
    else:
        return True


def copy_file(dest_bucket: str, file: str, source: str) -> bool:
    mc = get_client()
    print('created minio client')
    try:
        mc.make_bucket(dest_bucket)
        print('made bucket '+dest_bucket)
    except BucketAlreadyOwnedByYou as err:
        print('bucket already owned by you '+dest_bucket)
        pass
    except BucketAlreadyExists as err:
        print('bucket already exists '+dest_bucket)
        pass
    except ResponseError as err:
        print('error creating bucket '+dest_bucket)
        raise err

    try:
        #mc.copy_object(bucket_name=dest_bucket, object_name=file, object_source=source)
        mc.fput_object(bucket_name=dest_bucket, object_name=file, file_path=source)
        print('pushed file '+file+' from '+source+' to minio bucket '+dest_bucket)
    except ResponseError as err:
        raise err
    else:
        return True


def create_bucket(bucket: str) -> bool:
    mc = get_client()
    print('created minio client')
    try:
        mc.make_bucket(bucket)
        print('made bucket '+bucket)
    except BucketAlreadyOwnedByYou as err:
        print('bucket already owned by you '+bucket)
        pass
    except BucketAlreadyExists as err:
        print('bucket already exists '+bucket)
        pass
    except ResponseError as err:
        print('error creating bucket '+bucket)
        raise err
    return True
