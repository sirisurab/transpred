from minio import Minio
from minio.error import ResponseError, BucketAlreadyExists, BucketAlreadyOwnedByYou

def get_client():
    return Minio('minio:9000',
                      access_key='minio',
                      secret_key='minio123',
                      secure=False)

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


def copy_file(dest_bucket: str, file: str, source_folder: str) -> bool:
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
        mc.copy_object(bucket_name=dest_bucket, object_name=file, object_source=source_folder)
        print('copied file '+file+' from '+source_folder+' to bucket '+dest_bucket)
    except ResponseError as err:
        raise err
    else:
        return True