from chalice import Chalice
from chalicelib.db import ModelsDatabase
from chalicelib.features import FeatureExtractor

app = Chalice(app_name='mhost')

models_db = ModelsDatabase()
fe = FeatureExtractor()

@app.route('/')
def index():
    return {'hello': 'world'}


@app.route('/model/{model_id}/{version}/predict', methods=['POST'])
def predict(model_id, version):
    try:
        req = app.current_request.json_body
    except:
        # TODO: improve exception handling
        raise Exception('Invalid JSON body')
    model = models_db.get_model(model_id, version)
    features = fe.extract_features(model, req)
    resp = model.predict(features)
    models_db.save_prediction(resp)
    return resp
