from chalice import Chalice
from chalicelib.db import ModelsDatabase
from chalicelib.features import FeatureExtractor
from chalicelib.dao.blobstore.s3.s3blobstore import S3Blobstore
from chalicelib.dao.docstore.dynamo import DynamoDocstore as Docstore
from chalicelib.utils.ludwig_qa import untokenize

import os

app = Chalice(app_name='mhost')

access_key = os.getenv('ACCESS_KEY')
secret_key = os.getenv('SECRET_KEY')
bucket_name = os.getenv('PRIMARY_BUCKET')
table_name  = os.getenv('DYNAMO_TABLE')

print(bucket_name, "is the bucket")
blobstore = S3Blobstore(bucket_name, access_key, secret_key)
docstore = Docstore(table_name, access_key, secret_key)

models_db = ModelsDatabase(blobstore, docstore)
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
    model = models_db.get_model(model_id, version) # `model` is an instance of LudwigModel
    features = fe.extract_features(model, req) # TODO: implement this
    pred = model.predict(data_dict=[req]).to_dict('records')[0]
    text = untokenize(pred)
    resp = {"text":text}
    models_db.save_prediction(resp)
    return resp
