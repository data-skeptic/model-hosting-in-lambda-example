from chalicelib.model import Model
import io
import zipfile
import jsons
from ludwig.api import LudwigModel
from threading import Thread
import os.path
import time

class WorkerThread(Thread):

    def __init__(self, status, cache, blobstore, docstore):
        Thread.__init__(self)
        self.status = status
        self.cache = cache
        self.blobstore = blobstore
        self.docstore = docstore


    def _set_status(self, new_status:str):
        self.status['status'] = new_status


    def _extract_model_files(self, content, model_object_id):
        file_like_object = io.BytesIO(content)
        zipfile_ob = zipfile.ZipFile(file_like_object)
        model_files = [x for x in zipfile_ob.namelist()] # TODO: filter unneccesary files
        print("extracting files...")
        zipfile_ob.extractall(f'./models/{model_object_id}', members=model_files)
        print("... files extracted")
        return f'models/{model_object_id}/model'


    def _load_model(self, record):
        model_object_id = record['model_id']
        if model_object_id in self.cache:
            return True
        self.cache[model_object_id] = {}
        print(model_object_id)
        self.cache[model_object_id]['metadata'] =  record
        key = record['dest_key']
        payload_key = list(jsons.loads(record['example1']).keys())[0]
        content = self.blobstore.get_blob(key)
        model_dir = self._extract_model_files(content, model_object_id)
        model = LudwigModel.load(model_dir)
        model_wrapper = Model(model, record['type'], payload_key)
        self.cache[model_object_id]['model'] = model_wrapper
        return True

    
    def run(self):
        # this only adds to the models, does not remove until restart.
        self._set_status('INITIALIZING')
        user_models = self.docstore.gsi_query('OwnerIndex', 'owner', 'ludwig_api')
        for record in user_models:
            self._load_model(record)
        self._set_status('INITIALIZED')


class ModelsDatabase(object):


    def __init__(self, blobstore, docstore):
        self.blobstore = blobstore
        self.docstore = docstore
        self.cache = {}
        self.status = {'status':'UNINITIALIZED'} # when dict, thread updates original object; when string, doesn't work
        thread = WorkerThread(self.status, self.cache, self.blobstore, self.docstore)
        thread.start()


    def get_model(self, model_id, version) -> Model:
        model_object_id = f'{model_id}/{version}'
        if model_object_id in self.cache:
            return self.cache[model_object_id]['model']
        else:             
            # lazy load the model
            print("model not present in cache, checking docstore")
            lookup_record = self.docstore.get_document(model_object_id)
            record = self.docstore.get_document(lookup_record.get('dest_key'))
            if record:
                # TODO: instead we should start a new thread 
                # and in the meantime tell the user
                # that the cache is being updated with the model
                self._set_status('INITIALIZING')
                print("loading model into cache")
                self._load_model(record)
                self._set_status('INITIALIZED')
                return self.cache[model_object_id]['model']
        raise KeyError(f'could not load model for {model_object_id}. does not exist.')


    def get_model_type(self, model_id, version):
        model_object_id = f'{model_id}/{version}'
        if model_object_id in self.cache:
            return self.cache[model_object_id]['metadata']['type']
        else:
           raise KeyError(f'could not load model type for {model_object_id}. does not exist.') 


    def remove_model(self, model_id, version):
        model_object_id = f'{model_id}/{version}'
        if model_object_id in self.cache:
            del self.cache[model_object_id]
            return f'removed {model_object_id} from cache successfully \n', 200
        else:
            return 'model does not exist', 500


    def load_model(self, model_id, version):
        model_object_id = f'{model_id}/{version}'
        if model_object_id in self.cache:
            return 'model already loaded', 500            
        lookup_record = self.docstore.get_document(model_object_id)
        record = self.docstore.get_document(lookup_record.get('dest_key'))
        self._load_model(record)
        return f'loaded {model_object_id} into cache successfully \n', 200

    
    def reload_models(self):
        thread = WorkerThread(self.status, self.cache, self.blobstore, self.docstore)
        thread.start()


    def _extract_model_files(self, content, model_object_id):
        file_like_object = io.BytesIO(content)
        zipfile_ob = zipfile.ZipFile(file_like_object)
        model_files = [x for x in zipfile_ob.namelist()] # TODO: filter unneccesary files
        print("extracting files...")
        zipfile_ob.extractall(f'./models/{model_object_id}', members=model_files)
        return f'models/{model_object_id}/model'


    def _load_model(self, record):
        model_object_id = record['model_id']
        if model_object_id in self.cache:
            return True
        self.cache[model_object_id] = {}
        print(model_object_id)
        self.cache[model_object_id]['metadata'] =  record
        key = record['dest_key']
        payload_key = list(jsons.loads(record['example1']).keys())[0]
        content = self.blobstore.get_blob(key)
        model_dir = self._extract_model_files(content, model_object_id)
        model = LudwigModel.load(model_dir)
        model_wrapper = Model(model, record['type'], payload_key)
        self.cache[model_object_id]['model'] = model_wrapper
        return True


    def _set_status(self, new_status:str):
        self.status['status'] = new_status


    def save_prediction(self, prediction):
        pass
        # TODO: persist result