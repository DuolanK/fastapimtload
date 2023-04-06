import boto3
from fastapi import FastAPI, Request, UploadFile, File
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import os
from boto3.s3.transfer import TransferConfig, S3Transfer
import tempfile


BUCKET_NAME = "uploads3duo"

app = FastAPI()
templates = Jinja2Templates(directory="templates")


@app.get('/', response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/upload-files")
def multi_part_upload_with_s3(request: Request, file: UploadFile = File(...)):
    # Multipart upload
    config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,
                            multipart_chunksize=1024 * 25, use_threads=True)
    s3 = boto3.client('s3',
                      aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),  # используем переменную окружения
                      aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
                      region_name='ru-central1',
                      endpoint_url='https://s3.yandexcloud.net')
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        # сохраняем содержимое загруженного файла во временный файл
        tmp.write(file.file.read())
        # получаем путь к временному файлу
        file_path = tmp.name
        # закрываем временный файл
        tmp.close()
    # формируем ключ объекта в S3
    key_path = 'multipart_files/' + file.filename
    transfer = S3Transfer(s3, config)
    transfer.upload_file(file_path, BUCKET_NAME, key_path,
                         extra_args={'ACL': 'public-read', 'ContentType': 'text/pdf'},
                         callback=ProgressPercentage(file.filename))

    # удаляем временный файл
    os.unlink(file_path)

class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0

    def __call__(self, bytes_amount):
        self._seen_so_far += bytes_amount
        percentage = (self._seen_so_far / self._size) * 100
        print(
            "\r%s  %s / %s  (%.2f%%)" % (
                self._filename, self._seen_so_far, self._size,
                percentage),
            end=''
        )
