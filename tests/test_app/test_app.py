import jsons


def test_index(app, client):
    res = client.get('/')
    assert res.status_code == 200
    expected = {
        'model_loading_status': {'status': 'INITIALIZED'},
        'models_loaded' : ['test/latest']
    }
    assert expected == jsons.loads(res.get_data(as_text=True))


def test_predict(app, client):
    res = client.post('/model/test/latest/predict', data=jsons.dumps({"input":"test"}))
    assert res.status_code == 200
    expected = 'result'
    assert expected == jsons.loads(res.get_data(as_text=True))['output']


def test_reload(app, client):
    res = client.get('/reload')
    assert res.status_code == 200


def test_remove(app, client):
    res = client.delete('/model/test/latest/remove')
    assert res.status_code == 200


def test_load(app, client):
    res = client.get('/model/test/latest/load')
    assert res.status_code == 200