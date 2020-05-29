import logging

logging.getLogger('requests').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('botocore.credentials').setLevel(logging.ERROR)
logging.getLogger('chardet').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.WARN)
logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
logging.getLogger('paramiko.transport').setLevel(logging.ERROR)
logging.getLogger('chalicelib.dao.blobstore').setLevel(logging.WARN)
logging.getLogger('elasticsearch').setLevel(logging.ERROR)
logging.getLogger('tensorflow').setLevel(logging.ERROR)