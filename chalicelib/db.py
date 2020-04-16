from chalicelib.model import Model


class ModelsDatabase(object):


    def __init__(self, blobstore):
        self.blobstore = blobstore


    # model = models_db.
    def get_model(self, model_id, version) -> Model:
        key = f'{model_id}/{version}'
        # TODO: use MemCache?
        content = self.blobstore.get_blob(key)
        # TODO: unpickle the content
        # will be a zip file, so unzip and then get tf model
        # tf models are not compatible with pickle
        # use ludwig's methods to load tf models
        # copy ludwig/ludwig folder into chalicelib to reduce deployment size
        # will need tensorflow to load the model
        # maybe can do with tensorflow lite to reduce deployment size
        model = None
        return model

    def save_prediction(self, prediction):
        pass
        # TODO: persist result
