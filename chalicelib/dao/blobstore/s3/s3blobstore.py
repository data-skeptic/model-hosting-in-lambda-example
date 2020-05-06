import boto3
from botocore.client import Config
import botocore
from botocore.exceptions import ClientError
from chalicelib.dao.blobstore import abstract_blobstore as blobstore
from collections import namedtuple
import logging
from operator import attrgetter
import os
import time

log = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))


S3Obj = namedtuple('S3Obj', ['key', 'mtime', 'size', 'ETag'])


class S3Blobstore(blobstore.AbstractBlobstore):


    def __init__(self, bucket_name, access_key=None, secret_key=None, region='us-east-2'):
        super().__init__(bucket_name)
        if access_key is None:
            self.client = boto3.client('s3', config=Config(signature_version='s3v4'), region_name=region)
            self.resource = boto3.resource('s3', region_name=region)
        else:
            self.client = boto3.client('s3', aws_access_key_id=access_key,  aws_secret_access_key=secret_key, config=Config(signature_version='s3v4'), region_name=region)
            self.resource = boto3.resource('s3', aws_access_key_id=access_key,  aws_secret_access_key=secret_key, region_name=region)
        self.bucket_name = bucket_name


    def save_blob(self, key, contents, metadata=None, public=False, content_type='binary/octet-stream'):
        if contents is None:
            log.error(f'Recieved None contents for {key}')
            return
        log.debug(f'S3Blobstore save {key} of type {type(contents)} and length {len(contents)}')
        kwargs = {
            'Bucket': self.bucket_name,
            'Key': key,
            'Body': contents,
            'ContentType': content_type
        }
        if public:
            kwargs['ACL'] = 'public-read'
        if metadata is not None:
            kwargs['Metadata'] = metadata
        self.client.put_object(**kwargs)
        if self.exists(f'{key}.waiting'):
            self.delete_blob(f'{key}.waiting')


    def get_blob_attribute(self, key, attribute):
        self.resource.Object(key)
        o = self.resource.Object(bucket_name=self.bucket_name, key=key)
        return o.metadata[attribute]


    def get_url_from_key(self, key):
        i = key.rfind('.')
        return '{}/{}/{}/'.format(self.client.meta.endpoint_url, self.bucket_name, key[:i])

    def get_key_from_url(self, url):
        i = len('{}/{}/'.format(self.client.meta.endpoint_url, self.bucket_name))
        return url[i:]


    def delete_blob(self, key):
        log.debug(f'S3Blobstore delete {key}')
        r = self.client.delete_object(Bucket=self.bucket_name, Key=key)
        # {'ResponseMetadata': {'RequestId': 'D5EB64638995D2EE', 'HostId': 'D8U8j+ZVoUOAR8RbndRcgBxVGkb0qosqQsZcnXzfhjRdr8U/2HVXJN0ToSqlrE7XA7GbKgehDpc=', 'HTTPStatusCode': 204, 'HTTPHeaders': {'x-amz-id-2': 'D8U8j+ZVoUOAR8RbndRcgBxVGkb0qosqQsZcnXzfhjRdr8U/2HVXJN0ToSqlrE7XA7GbKgehDpc=', 'x-amz-request-id': 'D5EB64638995D2EE', 'date': 'Wed, 13 Nov 2019 22:51:12 GMT', 'server': 'AmazonS3'}, 'RetryAttempts': 1}}
        print(r['ResponseMetadata']['RequestId'])
        print(r['ResponseMetadata']['HTTPStatusCode'])
        return r


    def get_blob(self, key):
        try:
            b = self.bucket_name
            resp = self.client.get_object(Bucket=b, Key=str(key))
            body = resp['Body'].read()
            log.debug(f'S3Blobstore get {key} of type {type(body)} and length {len(body)}')
            return body
        except ClientError as ex:
            log.debug(f'S3Blobstore ERROR getting {key}')
            log.error(ex)
            if ex.response['Error']['Code'] == 'NoSuchKey':
                return None
            else:
                raise ex


    def move(self, src_key: str, dest_key: str):
        print("MOVE", src_key, '->', dest_key)
        copy_source = {'Bucket': self.bucket_name, 'Key': src_key}
        self.client.copy_object(CopySource=copy_source, Bucket=self.bucket_name, Key=dest_key)
        self.client.delete_object(Bucket=self.bucket_name, Key=src_key)


    def ls(self, prefix, suffix):
        return self._get_matching_s3_keys(prefix, suffix)


    def get_blob_metadata(self, key):
        try:
            r = self.client.head_object(Bucket=self.bucket_name, Key=key)
            if 'Metadata' in r:
                result = r['Metadata']
            else:
                result = {}
        except:
            # TODO: handle exceptions more specifically
            return None
        result['status_code'] = r['ResponseMetadata']['HTTPStatusCode']
        result['last_modified'] = r['LastModified']
        result['content_length'] = r['ContentLength']
        return result


    def exists(self, key):
        try:
            self.resource.Object(self.bucket_name, key).load()
            return True
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                # Something else has gone wrong.
                raise


    def get_keys_matching_pattern(self, pattern='*.crawl'):
        arr = pattern.split('*')
        if len(arr) == 1:
            m = self.get_blob_metadata(pattern)
            if m is None:
                return []
            else:
                return [pattern]
        r = list(self._get_matching_s3_keys(prefix=arr[0], suffix=arr[-1]))
        print(r)
        return r


    def _get_matching_s3_keys(self, prefix='', suffix=''):
        """
        Generate the keys in an S3 bucket.
        :param bucket: Name of the S3 bucket.
        :param prefix: Only fetch keys that start with this prefix (optional).
        :param suffix: Only fetch keys that end with this suffix (optional).
        """
        kwargs = {'Bucket': self.bucket_name, 'Prefix': prefix}
        while True:
            resp = self.client.list_objects_v2(**kwargs)
            if 'Contents' in resp:
                for obj in resp['Contents']:
                    key = obj['Key']
                    if key.endswith(suffix):
                        yield key

            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break


    def generate_presigned_url(self, key, ttl=1000):
        d = {'Bucket': self.bucket_name, 'Key': key}
        return self.client.generate_presigned_url('get_object', Params=d, ExpiresIn=ttl)


    def s3list(self, path, start=None, end=None, recursive=True, list_dirs=True,
               list_objs=True, limit=None):
        bucket = self.resource.Bucket(self.bucket_name)
        kwargs = dict()
        if start is not None:
            if not(start.startswith(path)):
                start = os.path.join(path, start)
            # note: need to use a string just smaller than start, because
            # the list_object API specifies that start is excluded (the first
            # result is *after* start).
            kwargs.update(Marker=self.__prev_str(start))
        if end is not None:
            if not end.startswith(path):
                end = os.path.join(path, end)
        if not recursive:
            kwargs.update(Delimiter='/')
            if not path.endswith('/'):
                path += '/'
        kwargs.update(Prefix=path)
        if limit is not None:
            kwargs.update(PaginationConfig={'MaxItems': limit})

        paginator = bucket.meta.client.get_paginator('list_objects')
        for resp in paginator.paginate(Bucket=bucket.name, **kwargs):
            q = []
            if 'CommonPrefixes' in resp and list_dirs:
                q = [S3Obj(f['Prefix'], None, None, None) for f in resp['CommonPrefixes']]
            if 'Contents' in resp and list_objs:
                q += [S3Obj(f['Key'], f['LastModified'], f['Size'], f['ETag']) for f in resp['Contents']]
            # note: even with sorted lists, it is faster to sort(a+b)
            # than heapq.merge(a, b) at least up to 10K elements in each list
            q = sorted(q, key=attrgetter('key'))
            if limit is not None:
                q = q[:limit]
                limit -= len(q)
            for p in q:
                if end is not None and p.key >= end:
                    return
                yield p


    def __prev_str(self, s):
        if len(s) == 0:
            return s
        s, c = s[:-1], ord(s[-1])
        if c > 0:
            s += chr(c - 1)
        s += ''.join(['\u7FFF' for _ in range(10)])
        return s