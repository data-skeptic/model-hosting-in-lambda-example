from chalicelib import log_config
from chalicelib.dao.blobstore.s3.s3blobstore import S3Blobstore
import os
import sys
import shutil
import io
import jsons
import zipfile
import pandas as pd
from ludwig.api import LudwigModel
import time


from dotenv import load_dotenv
load_dotenv()


access_key = os.getenv('AWS_ACCESS_KEY_ID')
secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
bucket_name     = os.getenv('PRIMARY_BUCKET')
blobstore = S3Blobstore(bucket_name, access_key, secret_key)


def make_inference(s3: S3Blobstore, key:str, payload:dict):
    s = time.time()
    content = s3.get_blob(key)
    t_s3 = time.time()
    file_like_object = io.BytesIO(content)
    zipfile_ob = zipfile.ZipFile(file_like_object)
    model_dir = 'model/'
    model_files = [x for x in zipfile_ob.namelist() if x.startswith(model_dir)]
    zipfile_ob.extractall(members=model_files)
    t_unzip = time.time()
    model = LudwigModel.load('./model')
    t_load_model = time.time()
    preds = model.predict(data_dict=[payload]).to_dict('records')[0]
    text = untokenize(preds)
    resp = {"text": text}
    t_get_prediction = time.time()
    model.close()
    shutil.rmtree(model_dir)
    t_clean_up = time.time()
    timing = {
        'time_to_get_blob':t_s3 - s,
        'time_to_unzip_blob':t_unzip - t_s3,
        'time_to_load_model':t_load_model - t_unzip,
        'time_to_get_prediction':t_get_prediction - t_load_model,
        'time_to_clean_up':t_clean_up - t_get_prediction,
        'end_to_end_time':t_clean_up - s
    }
    resp['telemetry'] = timing
    return resp


def untokenize(r):
    sentence = r['Answer_predictions']
    result = ' '.join(sentence).replace(' , ',', ').replace(' .','.').replace(' !','!').replace(" ' ", "'")
    result = result.replace(' ?','?').replace(' : ',': ').replace(' \'', '\'')
    result = result[0].upper() + result[1:]
    result = result.replace("<PAD>","")
    result = result.strip()
    return result


if __name__=="__main__":
    s = time.time()
    if len(sys.argv) == 3:
        key = str(sys.argv[1])
        raw_payload = str(sys.argv[2]).replace("'","\"")
        payload = jsons.loads(raw_payload)
        print("s3key: ", key)
        print("inference payload: ", payload)
        pred = make_inference(blobstore, key, payload)
        print("prediction", pred)
    else:
        print('provide a key (str) and a payload (dict as str) argument')
    e = time.time()
    print("script execution time: ", e-s)