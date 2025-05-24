import os
import json
import base64
from glob import glob

class FileInterface:
    def __init__(self):
        # Tentukan direktori files/ relatif terhadap direktori kerja saat ini
        self.files_dir = os.path.join(os.getcwd(), 'files')
        # Buat direktori files/ jika belum ada
        if not os.path.exists(self.files_dir):
            os.makedirs(self.files_dir)

    def list(self, params=[]):
        try:
            # Gunakan path absolut ke direktori files/
            filelist = glob(os.path.join(self.files_dir, '*.*'))
            # Ekstrak hanya nama file, bukan path lengkap
            filelist = [os.path.basename(f) for f in filelist]
            result = dict(status='OK', data=filelist)
        except Exception as e:
            result = dict(status='ERROR', data=str(e))
        return result

    def get(self, params=[]):
        try:
            filename = params[0]
            if filename == '':
                result = dict(status='ERROR', data='Filename or file data is empty')
                return result
            file_path = os.path.join(self.files_dir, filename)
            with open(file_path, 'rb') as fp:
                isifile = base64.b64encode(fp.read()).decode()
            result = dict(status='OK', data_namafile=filename, data_file=isifile)
        except Exception as e:
            result = dict(status='ERROR', data=str(e))
        return result
        
    def upload(self, params=[]):
        try:
            filename = params[0]
            filedata_b64 = params[1]
            if filename == '' or filedata_b64 == '':
                result = dict(status='ERROR', data='Filename or file data is empty')
                return result
            missing_padding = len(filedata_b64) % 4
            if missing_padding != 0:
                filedata_b64 += '=' * (4 - missing_padding)
            filedata = base64.b64decode(filedata_b64)
            file_path = os.path.join(self.files_dir, filename)
            with open(file_path, 'wb') as f:
                f.write(filedata)
            result = dict(status='OK', data_namafile=filename)
        except Exception as e:
            result = dict(status='ERROR', data=str(e))
        return result
        
    def delete(self, params=[]):
        try:
            filename = params[0]
            if filename == '':
                result = dict(status='ERROR', data='Filename is empty')
                return result
            file_path = os.path.join(self.files_dir, filename)
            if os.path.exists(file_path):
                os.remove(file_path)
                result = dict(status='OK', data=f'File {filename} deleted successfully')
            else:
                result = dict(status='ERROR', data=f'File {filename} not found')
        except Exception as e:
            result = dict(status='ERROR', data=str(e))
        return result

if __name__ == '__main__':
    f = FileInterface()
    print(f.list())
    print(f.get(['pokijan.jpg']))