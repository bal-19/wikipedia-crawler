import traceback
import s3fs

class Connection:
    def __init__(self):
        self.client_kwargs = {
            'key': 'GLZG2JTWDFFSCQVE7TSQ',
            'secret': 'VjTXOpbhGvYjDJDAt2PNgbxPKjYA4p4B7Btmm4Tw',
            'endpoint_url': 'http://192.168.180.9:8000',
            'anon': False
        }
        self.s3 = s3fs.core.S3FileSystem(**self.client_kwargs)

    def upload(self, rpath, lpath):
        try:
            self.s3.upload(rpath=rpath, lpath=lpath)
            print('upload success \n')
        except Exception as e:
            traceback.print_exc()
            print(f'failed to upload: {e} \n')