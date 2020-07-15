from flask import Flask, request, jsonify, abort
from dotenv import load_dotenv
import os
import time

if os.getenv("IS_PROD") == "true":
    print("Loading in prod")
else:
    print("Loading in dev")
    load_dotenv(verbose=True)

from chalicelib import log_config
from chalicelib.db import ModelsDatabase
from chalicelib.features import FeatureExtractor
from chalicelib.dao.blobstore.s3.s3blobstore import S3Blobstore
from chalicelib.dao.docstore.dynamo import DynamoDocstore as Docstore


app = Flask(__name__)

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
    resp = {
        'model_loading_status': models_db.status,
        'models_loaded' : list(models_db.cache.keys())
    }
    return jsonify(resp)


@app.route('/model/<model_id>/<version>/predict', methods=['POST'])
def predict(model_id, version):
    try:
        req = request.get_json(force=True)
    except:
        return 'Invalid JSON body \n', 500
    t0 = time.time()
    try:
        model = models_db.get_model(model_id, version) # LudwigModel
    except KeyError as e:
        print(e)
        return str(e), 404
    # # features = fe.extract_features(model, req) # TODO: implement this
    t1 = time.time()
    try:
        prediction = model.predict(req, blobstore) #TODO: why a delay the first time?
    except ValueError as e:
        print(e)
        return str(e), 404
    t2 = time.time()
    docstore.increment_counter(f'{model_id}/{version}', 'api_calls', 1)
    t3 = time.time()
    resp = {
        "time_to_get_model" : t1 - t0,
        "time_to_get_prediction" : t2 - t1,
        "time_to_increment_counter" : t3 - t2,
        "total_response_time" : t3 - t0
    }
    if isinstance(prediction, dict):
        resp = {**resp, **prediction}
    else:
        resp['prediction'] = prediction
    # models_db.save_prediction(resp)
    return jsonify(resp)


@app.route('/reload')
def reload():
    try:
        models_db.reload_models()
    except:
        return 'reload unsuccessful', 400
    return 'reload successful', 200


@app.route('/model/<model_id>/<version>/remove', methods=['DELETE'])
def remove(model_id, version):
    try:
        resp = models_db.remove_model(model_id, version)
    except:
        return 'removal unsuccessful', 400
    return resp


@app.route('/model/<model_id>/<version>/load', methods=['GET'])
def load(model_id, version):
    try:
        resp = models_db.load_model(model_id, version)
    except:
        return 'model load unsuccessful', 400
    return resp


@app.route('/api/prophet/<app_name>', methods=['POST'])
def prophet_predict(app_name):
    try:
        req = request.get_json(force=True)
    except KeyError as e:
        return f"post request missing key {e} \n", 500
    except:
        return 'Invalid JSON body \n', 500
    t0 = time.time()
    try:
        model = get_fbprophet_model(app_name) # fbprophet.forecaster.Prophet
    except KeyError as e:
        print(e)
        return str(e), 500
    t1 = time.time()
    try:
        preds = get_fbprophet_prediction(model, req)
    except ValueError as e:
        print(e)
        return str(e), 404
    t2 = time.time()
    resp = {
        "time_to_get_model" : t1 - t0,
        "time_to_get_prediction" : t2 - t1,
        "total_response_time" : t2 - t0
    }
    resp['predictions'] = preds
    return jsonify(resp)


def get_fbprophet_model(app_name):
    import pickle
    import fbprophet
    pkl_key = f"user/alex@dataskeptic.com/apps/forager/prophet-test/{app_name}.prophet.pkl"
    if not blobstore.exists(pkl_key):
        raise KeyError(f'no model associated with {app_name}')
    content = blobstore.get_blob(pkl_key)
    return pickle.loads(content)


def get_fbprophet_prediction(model, req):
    horizon = int(req['horizon'])
    future = model.make_future_dataframe(periods=horizon)
    forecast = model.predict(future)
    preds = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]\
        .tail(horizon).to_dict(orient='records')
    return preds


def main():

    app.run(debug=False, host=os.getenv("host"), port=int(os.getenv("PORT")), threaded=True)


if __name__ == '__main__':
    main()