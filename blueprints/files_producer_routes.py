import time
import settings
from flask import Blueprint
from flask import request, jsonify
from fastcdc import fastcdc
from uuid import uuid4
from hashlib import sha256
from producers.files_producer import set_up_producer, delivery_report
from serialization_classes.file_data import FileData

time.sleep(30)

MIN_SIZE = 64
AVG_SIZE = 256
MAX_SIZE = 1024

producer = set_up_producer()

files_producer_routes = Blueprint('files_producer_routes', __name__)

@files_producer_routes.route('/upload', methods=['POST'])
def upload_file():
    # handling file source: https://roytuts.com/python-flask-rest-api-file-upload/
    if 'file' not in request.files:
        res = jsonify({
            'message': 'There is no file in part of request.'
        })
        res.status_code = 400
        return res

    uploaded_file = request.files['file']
    if uploaded_file.filename == '':
        res = jsonify({
            'message': 'No file was uploaded.'
        })
        res.status_code = 400
        return res

    try:
        min_size = int(request.form.get('min_size')) if 'min_size' in request.form \
            and int(request.form.get('min_size')) and int(request.form.get('min_size')) > MIN_SIZE else MIN_SIZE
        avg_size = int(request.form.get('avg_size')) if 'avg_size' in request.form \
            and int(request.form.get('avg_size')) and int(request.form.get('avg_size')) > AVG_SIZE else AVG_SIZE
        max_size = int(request.form.get('max_size')) if 'max_size' in request.form \
            and int(request.form.get('max_size')) and int(request.form.get('max_size')) > MAX_SIZE else MAX_SIZE

        file_content_bytes = uploaded_file.read()

        producer.poll(0.0)

        chunks = list(fastcdc(file_content_bytes, min_size=min_size, 
            avg_size=avg_size, max_size=max_size, fat=True, hf=sha256))
        total_chunks = len(chunks)

        for i, chunk in enumerate(chunks):
            end_of_file = i == (total_chunks - 1)
            file_data = FileData(file_name=uploaded_file.filename, chunk=chunk.data, chunk_hash=chunk.hash,
                                chunk_serial_num=i, end_of_file=end_of_file)
            producer.produce(topic=settings.FILES_TOPIC, key=str(uuid4()), value=file_data,
                            on_delivery=delivery_report)

        producer.flush()

        res = jsonify({
            'message': 'File was successfully uploaded.'
        })
        res.status_code = 200
        return res

    except ValueError:
        res = jsonify({
            'message': 'min_size, avg_size or max_size isn\'t interger.'
        })
        res.status_code = 400
        return res

    except Exception as e:
        print(e)
        res = jsonify({
            'message': 'Something went wrong.'
        })
        res.status_code = 500
