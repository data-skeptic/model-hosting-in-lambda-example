import pytest
import chalicelib.db

class MockModel:
    def __init__(self, model_id, version):
        self.model_id = model_id
        self.version = version
        self.counter = 0

    def predict(self, req, blobstore):
        return {"output":"result"}


    def increment_counter(self, docstore, model_id, version):
    	self.counter += 1
    	return {"counter": self.counter}


# monkey patch ModelsDatabase.__init__ to avoid running model-loader thread
def new_init(self, blobstore, docstore):
    self.blobstore = blobstore
    self.docstore = docstore
    self.cache = {'test/latest': MockModel(blobstore, docstore)}
    self.status = {'status':'INITIALIZED'}


@pytest.fixture
def app():
    chalicelib.db.ModelsDatabase.__init__ = new_init
    chalicelib.db.ModelsDatabase.get_model = lambda self, x, y: MockModel(x,y)
    chalicelib.db.ModelsDatabase.reload_models = lambda x: None
    chalicelib.db.ModelsDatabase.load_model = lambda self, x, y: 'model already loaded'
    from app import app as flask_app
    yield flask_app


@pytest.fixture
def client(app):
    return app.test_client()