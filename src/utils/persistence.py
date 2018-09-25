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