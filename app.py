from chalice import Chalice
from chalicelib.db import ModelsDatabase
from chalicelib.features import FeatureExtractor
from chalicelib.dao.blobstore.s3.s3blobstore import S3Blobstore

import os

app = Chalice(app_name='mhost')

if os.getenv('PRIMARY_BUCKET') is None:
    from dotenv import load_dotenv
    load_dotenv()

access_key = os.getenv('AWS_ACCESS_KEY_ID')
secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
bucket_name     = os.getenv('PRIMARY_BUCKET')
blobstore = S3Blobstore(bucket_name, access_key, secret_key)

models_db = ModelsDatabase(blobstore)
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
