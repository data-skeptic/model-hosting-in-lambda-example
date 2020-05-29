import boto3
from boto3.dynamodb.conditions import Key
from chalicelib.dao.docstore.abstract_docstore import AbstractDocstore as Docstore
from chalicelib.utils import common
import logging
import os


log = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))


class DynamoDocstore(Docstore):


    def __init__(self, table_name, access_key=None, secret_key=None):
        super().__init__()
        self.table_name = table_name
        if access_key is not None:
            #region = os.getenv("AWS_REGION")
            region = 'us-east-1'
            self.client = boto3.client('dynamodb', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region)
            self.table = boto3.resource('dynamodb', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region).Table(table_name)
            self.has_iam = True
        else:
            self.client = boto3.client('dynamodb')
            self.table = boto3.resource('dynamodb').Table(table_name)
            self.has_iam = False


    def get_document(self, key, initialize=False):
        log.debug(f'S3Blobstore get_document {key}')
        k = {}
        k['object_id'] = key
        try:
            resp = self.table.get_item(Key=k)
            return resp['Item']
        except:
            return None


    def get_all(self, limit=100):
        r = self.table.scan(Limit=limit)
        items = r['Items']
        return items


    def delete_document(self, key):
        k = {}
        k['object_id'] = key
        r = self.table.delete_item(Key=k)
        return r


    def save_document(self, key, contents):
        t = type(contents)
        if t==dict:
            item = contents
        else:
            item = contents.__dict__
        item['object_id'] = key
        item = common.clean_json_dict(item)
        r = self.table.put_item(Item=item)
        # TODO: inspect r
        return key


    def update_document(self, key, contents):
        ue = "set"
        ean = {}
        eav = {}
        if type(contents) == dict:
            d = contents
        else:
            d = contents.as_dict()
        for k in d.keys():
            if k != 'object_id':
                v = d[k]
                if v is not None and v != '':
                    ean[f'#{k}'] = k
                    eav[f':{k}'] = v
                    if len(ue) > 3:
                        ue += ','
                    ue +=  f' #{k} = :{k}'
        pk = { 'object_id': key }
        try:
            response = self.table.update_item(
                Key=pk,
                UpdateExpression=ue,
                ExpressionAttributeNames=ean,
                ExpressionAttributeValues=eav,
                ReturnValues="UPDATED_NEW"
            )
        except Exception as e:
            log.error(e)
            raise e
        return response


    def get(self, idx_name, eav=None, kce=None):
        """
        resp = self.client.query(
           TableName=self.table_name,
           IndexName=idx_name,
           ExpressionAttributeValues=eav,
           KeyConditionExpression=kce,
        )
        """
        response = self.table.scan()
        c = response['Count']
        sc = response['ScannedCount']
        log.debug(f'Count: {c} | Scanned Count: {sc}')
        data = response['Items']
        return data


    def search(self, pattern):
        """ Please override this.  Your persistence layer should do better! """
        items = self.get_all() # TODO: in need of optimization
        results = []
        print(f'----[{len(items)}]---------')
        for item in items:
            k = item['object_id']
            # ext = Uri2S3key.get_extension(pattern)
            # item_key = Uri2S3key.encode(k, ext)
            if common.match(pattern, k):
                results.append(item)
        log.info(f'====[{len(results)}, {type(results)}]========')
        return results


    def increment_counter(self, object_id, name, amount=1):
        res = self.client.update_item(
            TableName = self.table_name,
            Key = {
                'object_id': {
                    'S': object_id
                }
            },
            ExpressionAttributeNames = {
                '#value': name
            },
            ExpressionAttributeValues = {
                ':amount': {
                    'N': str(amount)
                },
                ':start': {
                    'N': '0'
                }
            },
            UpdateExpression = 'SET #value = if_not_exists(#value, :start) + :amount',
            # UpdateExpression = 'ADD #value :amount',
            ReturnValues = 'UPDATED_NEW'
        )
        print('dynamo.increment_counter resp', res)
        return True


    def gsi_query(self, index_name, key, search_value):
        res = self.table.query(
                IndexName = index_name,
                KeyConditionExpression = Key(key).eq(search_value)
            )
        items = res['Items']
        return items




