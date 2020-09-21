import jsons
from chalicelib.db import WorkerThread, ModelsDatabase
from chalicelib.dao.blobstore.s3.s3blobstore import S3Blobstore
from chalicelib.dao.docstore.dynamo import DynamoDocstore as Docstore
import shutil
import tempfile
import os

def mockS3_init(self, bucket_name, access_key=None, secret_key=None, region='us-east-2'):
    self.client = {}
    self.resource = {}
    self.bucket_name = bucket_name

def mockDocstore_init(self, table_name, access_key=None, secret_key=None):
    self.table_name = table_name
    self.client = {}
    self.table = {}
    self.has_iam = False if access_key is None else True

S3Blobstore.__init__ = mockS3_init
Docstore.__init__ = mockDocstore_init

blobstore = S3Blobstore('fake_bucket', 'foo', 'bar')
docstore = Docstore('fake_table', 'foo', 'bar')

ludwig_record = """
{
"dest_key": "user/alex@dataskeptic.com/chitchat-dry-run.model.zip", 
"model_id": "chitchat-dry-run.model/latest", 
"object_id": "user/alex@dataskeptic.com/chitchat-dry-run.model.zip", 
"key": "user/alex@dataskeptic.com/chitchat.csv", 
"example1": "{\\"Question\\": \\"Can you ask me a question? \\"}", 
"owner": "ludwig_api",
"last_modified": "2020-09-08 19:38:13+00:00", 
"version": "latest", 
"content_length": "20718097", 
"feature_column": "Question", 
"type": "chalicelib.applications.ludwig.qa.Request"
}
"""

prophet_record = """
{
"version": "latest",
"ds_col_name": "dt",
"pickle_key": "user/alex@dataskeptic.com/prophet-unemployment/unemployment_data.prophet.pkl",
"status": "SUCCEEDED",
"owner": "prophet_api",
"object_id": "user/alex@dataskeptic.com/prophet-unemployment/unemployment_data.prophet.pkl",
"yhat_col_name": "unemployment_pred",
"model_id": "prophet_api_unemployment_alex/latest",
"key": "user/alex@dataskeptic.com/prophet-unemployment/unemployment_data.csv",
"period_char": "MS",
"time_period": "months",
"type": "chalicelib.applications.csvtools.csv2prophet.Request"
}
"""

def test_worker_set_status():
	worker = WorkerThread({'status':'UNINITIALIZED'}, {}, blobstore, docstore)
	worker._set_status('INITIALIZING')
	expected = {'status':'INITIALIZING'}
	assert expected == worker.status
    
def test_worker_extract_model_files():
	tmpdir = tempfile.mkdtemp()
	tmparchive = os.path.join(tmpdir, 'archive')
	root_dir = "./tests/sample"
	content = open(shutil.make_archive(tmparchive, 'zip', root_dir), 'rb').read()
	worker = WorkerThread({'status':'UNINITIALIZED'}, {}, blobstore, docstore)
	model_object_id = 'mock/latest'
	model_dir = worker._extract_model_files(content, model_object_id)
	expected_dir = f'./models/mock'
	shutil.rmtree(tmpdir)
	shutil.rmtree(expected_dir)
	assert model_object_id in model_dir

def test_worker_load_model():
	from ludwig.api import LudwigModel
	blobstore.get_blob = lambda x: b'fake content'
	worker = WorkerThread({'status':'UNINITIALIZED'}, {}, blobstore, docstore)
	worker._extract_model_files = lambda x, y: './models/mock/latest/model'
	record = jsons.loads(ludwig_record)
	model_object_id = record['model_id']
	LudwigModel.load = lambda x: {}
	res = worker._load_model(record)
	assert res
	assert model_object_id in worker.cache
	worker.cache[model_object_id]['metadata']['marker'] = 'loaded'
	res2 = worker._load_model(record) # exit early if model already loaded
	assert worker.cache[model_object_id]['metadata']['marker'] == 'loaded'

def test_worker_load_prophet_model():
	import pickle
	pickle.loads = lambda x: {}
	blobstore.get_blob = lambda x: b'fake content'
	worker = WorkerThread({'status':'UNINITIALIZED'}, {}, blobstore, docstore)
	worker._extract_model_files = lambda x, y: './models/mock/latest/model'
	record = jsons.loads(prophet_record)
	model_object_id = record['model_id']
	res = worker._load_prophet_model(record)
	assert res
	assert model_object_id in worker.cache
	worker.cache[model_object_id]['metadata']['marker'] = 'loaded'
	res2 = worker._load_prophet_model(record) # exit early if model already loaded
	assert worker.cache[model_object_id]['metadata']['marker'] == 'loaded'

def test_worker_run():
	docstore.gsi_query = lambda x,y,z: []
	worker = WorkerThread({'status':'UNINITIALIZED'}, {}, blobstore, docstore)
	worker.run()
	assert 'INITIALIZED' in worker.status['status']


def test_modelsdb_get_model():
	modelsdb = ModelsDatabase(blobstore, docstore)
	assert len(modelsdb.cache.keys()) == 0
	# TODO: figure out how to make test_app/conftest.py not interfere with this ModelsDatabase class
	# perhaps via more localized mocking?