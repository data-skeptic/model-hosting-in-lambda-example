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

def mockModelsDB_init(self, blobstore, docstore):
    self.blobstore = blobstore
    self.docstore = docstore
    self.cache = {}
    self.status = {'status':'UNINITIALIZED'}

def mockRun(self):
	self.cache['test/latest'] = {}

S3Blobstore.__init__ = mockS3_init
Docstore.__init__ = mockDocstore_init

blobstore = S3Blobstore('fake_bucket', 'foo', 'bar')
docstore = Docstore('fake_table', 'foo', 'bar')

ludwig_lookup = {"dest_key":"model.zip"}

ludwig_record = """
{
"dest_key": "user/alex@dataskeptic.com/chitchat-dry-run.model.zip", 
"model_id": "test/latest", 
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

prophet_lookup = {"pickle_key":"data.prophet.pkl"}

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

def test_worker_load_model(monkeypatch):
	from ludwig.api import LudwigModel
	monkeypatch.setattr(blobstore,'get_blob', lambda x: b'fake content')
	monkeypatch.setattr(WorkerThread, '_extract_model_files', lambda x, y, z: './models/mock/latest/model')
	monkeypatch.setattr(LudwigModel, 'load', lambda x: {})
	
	worker = WorkerThread({'status':'UNINITIALIZED'}, {}, blobstore, docstore)
	record = jsons.loads(ludwig_record)
	model_object_id = record['model_id']
	res = worker._load_model(record)
	assert res
	assert model_object_id in worker.cache
	worker.cache[model_object_id]['metadata']['marker'] = 'loaded'
	res2 = worker._load_model(record) # exit early if model already loaded
	assert worker.cache[model_object_id]['metadata']['marker'] == 'loaded'

def test_worker_load_prophet_model(monkeypatch):
	import pickle
	monkeypatch.setattr(pickle,'loads',lambda x: {})
	monkeypatch.setattr(blobstore,'get_blob', lambda x: b'fake content')
	monkeypatch.setattr(WorkerThread, '_extract_model_files', lambda x, y, z: './models/mock/latest/model')
	
	worker = WorkerThread({'status':'UNINITIALIZED'}, {}, blobstore, docstore)
	record = jsons.loads(prophet_record)
	model_object_id = record['model_id']
	res = worker._load_prophet_model(record)
	assert res
	assert model_object_id in worker.cache
	worker.cache[model_object_id]['metadata']['marker'] = 'loaded'
	res2 = worker._load_prophet_model(record) # exit early if model already loaded
	assert worker.cache[model_object_id]['metadata']['marker'] == 'loaded'

def test_worker_run(monkeypatch):
	monkeypatch.setattr(docstore, 'gsi_query', lambda x,y,z: [])
	monkeypatch.setattr(docstore, 'get_batch_documents', lambda keys: [])
	
	worker = WorkerThread({'status':'UNINITIALIZED'}, {}, blobstore, docstore)
	worker.run()
	assert 'INITIALIZED' in worker.status['status']


def test_modelsdb_get_model(monkeypatch):
	from ludwig.api import LudwigModel
	monkeypatch.setattr(blobstore,'get_blob', lambda x: b'fake content')
	monkeypatch.setattr(docstore,'get_document', lambda x: docstore.table.get(x))
	monkeypatch.setattr(WorkerThread, '_extract_model_files', lambda x, y, z: './models/mock/latest/model')
	monkeypatch.setattr(LudwigModel, 'load', lambda x: {})
	monkeypatch.setattr(ModelsDatabase,'__init__', mockModelsDB_init)
	docstore.table['test/latest'] = ludwig_lookup
	docstore.table['model.zip'] = jsons.loads(ludwig_record)
	
	modelsdb = ModelsDatabase(blobstore, docstore)
	assert len(modelsdb.cache.keys()) == 0
	modelsdb.get_model('test','latest')
	assert 'test/latest' in modelsdb.cache


def test_modelsdb_get_model_type(monkeypatch):
	monkeypatch.setattr(ModelsDatabase,'__init__', mockModelsDB_init)
	
	modelsdb = ModelsDatabase(blobstore, docstore)
	modelsdb.cache['test/latest'] = {"metadata":{"type": "chalicelib.applications.ludwig.qa.Request"}}
	expected = "chalicelib.applications.ludwig.qa.Request"
	assert modelsdb.get_model_type('test','latest') == expected


def test_modelsdb_remove_model(monkeypatch):
	monkeypatch.setattr(ModelsDatabase,'__init__', mockModelsDB_init)
	
	modelsdb = ModelsDatabase(blobstore, docstore)
	modelsdb.cache['test/latest'] = {}
	modelsdb.remove_model('test','latest')
	assert 'test/latest' not in modelsdb.cache


def test_modelsdb_load_model(monkeypatch):
	from ludwig.api import LudwigModel
	monkeypatch.setattr(blobstore,'get_blob', lambda x: b'fake content')
	monkeypatch.setattr(docstore,'get_document', lambda x: docstore.table.get(x))
	monkeypatch.setattr(WorkerThread, '_extract_model_files', lambda x, y, z: './models/mock/latest/model')
	monkeypatch.setattr(LudwigModel, 'load', lambda x: {})
	monkeypatch.setattr(ModelsDatabase,'__init__', mockModelsDB_init)
	docstore.table['test/latest'] = ludwig_lookup
	docstore.table['model.zip'] = jsons.loads(ludwig_record)
	
	modelsdb = ModelsDatabase(blobstore, docstore)
	assert len(modelsdb.cache.keys()) == 0
	modelsdb.load_model('test','latest')
	assert 'test/latest' in modelsdb.cache


def test_modelsdb_init(monkeypatch):
	monkeypatch.setattr(WorkerThread, 'run', mockRun)
	
	modelsdb = ModelsDatabase(blobstore, docstore)
	assert 'test/latest' in modelsdb.cache


def test_modelsdb_reload_models(monkeypatch):
	monkeypatch.setattr(WorkerThread, 'run', mockRun)
	monkeypatch.setattr(ModelsDatabase,'__init__', mockModelsDB_init)
	
	modelsdb = ModelsDatabase(blobstore, docstore)
	assert 'test/latest' not in modelsdb.cache
	modelsdb.reload_models()
	assert 'test/latest' in modelsdb.cache