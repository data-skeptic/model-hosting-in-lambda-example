from chalicelib.utils import ludwig_qa
import os

class Model(object):


    def __init__(self, model, model_type, request_key):
        self.model = model
        self.type = model_type
        self.request_key = request_key


    def predict(self, req, blobstore):
        if self.type == "chalicelib.applications.ludwig.speaker_verification.Request":
            remote_audio_path = req[self.request_key]
            content = blobstore.get_blob(remote_audio_path)
            if content is None:
                raise ValueError(f'no content for blob {remote_audio_path}')
            with open('payload.wav','wb') as f:
                f.write(content) # TODO: clean up temp files https://stackoverflow.com/questions/13344538/how-to-clean-up-temporary-file-used-with-send-file
            local_audio_path = os.path.abspath('payload.wav')
            entry = {self.request_key:local_audio_path}
            preds = self.model.predict(data_dict=[entry]).to_dict('records')[0]
            return preds
        elif self.type == "chalicelib.applications.ludwig.image_classification.Request":
            remote_image_path = req[self.request_key]
            content = blobstore.get_blob(remote_image_path)
            if content is None:
                raise ValueError(f'no content for blob {remote_image_path}')
            with open('image.png','wb') as f:
                f.write(content)
            local_image_path = os.path.abspath('image.png')
            entry = {self.request_key:local_image_path}
            preds = self.model.predict(data_dict=[entry]).to_dict('records')[0]
            return preds
        elif self.type == "chalicelib.applications.ludwig.qa.Request":
            preds = self.model.predict(data_dict=[req]).to_dict('records')[0]
            text = ludwig_qa.untokenize(preds)
            return text
        else:
            raise ValueError(f'model of type {self.type} not supported.')
