import os
import paramiko
import boto3
from boto3.s3.transfer import TransferConfig, S3Transfer
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from fastapi import FastAPI, Request, UploadFile, File
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import sys

load_dotenv()

BUCKET_NAME = "uploads3duo"
LOCAL_FILE_PATH = "/path/to/local/file"
REMOTE_FILE_PATH = "/path/to/remote/file"

app = FastAPI()
templates = Jinja2Templates(directory="templates")


def upload_file_to_vm(file_obj):
    # Connect to the remote VM via SSH
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="vm_address", username="vm_username", password="vm_password")

    # Create a transport object to upload files
    transport = ssh.get_transport()
    sftp = transport.open_sftp()

    # Upload the file to the remote VM
    sftp.putfo(file_obj, REMOTE_FILE_PATH)

    # Close the SFTP session and SSH connection
    sftp.close()
    ssh.close()


def upload_file_to_s3(file_obj, bucket_name, key_path):
    # Create an S3 client object
    s3 = boto3.client('s3',
                      aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                      aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
                      region_name='ru-central1',
                      endpoint_url='https://s3.yandexcloud.net')

    # Configure the S3 transfer settings
    config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,
                            multipart_chunksize=1024 * 25, use_threads=True)

    # Initiate the transfer to S3
    transfer = S3Transfer(s3, config)
    transfer.upload_file(file_obj, bucket_name, key_path,
                         extra_args={'ACL': 'public-read', 'ContentType': 'text/pdf'},
                         callback=ProgressPercentage(object))

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
        sys.stdout.flush()

    @app.get('/', response_class=HTMLResponse)
    def home(request: Request):
        return templates.TemplateResponse("index.html", {"request": request})

    @app.post("/upload-files")
    def multi_part_upload_with_s3(request: Request, file: UploadFile = File(...)):
        # Open the file as a file-like object
        file_obj = file.file

        # Upload the file to the remote VM in a separate thread
        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(upload_file_to_vm, file_obj)

        # Upload the file to S3 in a separate thread
        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(upload_file_to_s3, file_obj, BUCKET_NAME, "multipart_files/" + file.filename)

        # Send the response to the client
        return {"message": "File uploaded successfully"}