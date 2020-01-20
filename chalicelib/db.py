from chalicelib.model import Model


class ModelsDatabase(object):


    def __init__(self, blobstore):
        self.blobstore = blobstore


    model = models_db.
    def get_model(self, model_id, version) -> Model:
        key = f'{model_id}/{version}'
        # TODO: use MemCache?
        content = self.blobstore.get_blob(key)
        # TODO: unpickle the content
        model = ?
        return model

    def save_prediction(self, prediction):
        # TODO: persist result
