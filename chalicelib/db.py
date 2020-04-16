from chalicelib.model import Model
import io
import zipfile
from ludwig.api import LudwigModel

class ModelsDatabase(object):


    def __init__(self, blobstore):
        self.blobstore = blobstore


    def get_model(self, model_id, version) -> Model:
        base = 'user/alex@dataskeptic.com/apps/forager' #TODO: discuss with kyle
        key = f'{base}/{model_id}/{version}'
        # TODO: use MemCache?
        content = self.blobstore.get_blob(key)
        model_dir = self._extract_model_files(content)
        model = LudwigModel.load(f'./{model_dir}')
        return model

    def save_prediction(self, prediction):
        pass
        # TODO: persist result

    def _extract_model_files(self, content):
        file_like_object = io.BytesIO(content)
        zipfile_ob = zipfile.ZipFile(file_like_object)
        model_dir = 'model/'
        model_files = [x for x in zipfile_ob.namelist() if x.startswith(model_dir)]
        zipfile_ob.extractall('./tmp', members=model_files)
        return f'tmp/{model_dir}'