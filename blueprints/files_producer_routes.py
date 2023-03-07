import os
import time
import settings
from flask import Blueprint
from flask import request, jsonify
from fastcdc import fastcdc
from uuid import uuid4
from hashlib import sha256, md5, sha1
from producers.files_producer import set_up_producer, delivery_report
from serialization_classes.file_data import FileData


def handle_not_valid_request(res, message):
    res = jsonify({ 'message': message })
    res.status_code = 400
    return res


def create_directory(directory_name):
    if not os.path.exists(directory_name) or not os.path.isdir(directory_name):
        os.mkdir(directory_name)


def process_and_publish_file(producer, file_name, experiment_name, file_content_bytes,
                            min_size, avg_size, max_size, hash_function):
    # perform chunking and time it takes
    start = time.perf_counter_ns()
    chunks = list(fastcdc(file_content_bytes, min_size=min_size,
        avg_size=avg_size, max_size=max_size, fat=True, hf=hash_function))
    end = time.perf_counter_ns()

    total_chunks = len(chunks)

    for i, chunk in enumerate(chunks):
        end_of_file = i == (total_chunks - 1)
        file_data = FileData(file_name=file_name, chunk=chunk.data, chunk_hash=chunk.hash,
                            chunk_serial_num=i, end_of_file=end_of_file, experiment_name=experiment_name)
        producer.produce(topic=settings.FILES_TOPIC, key=str(uuid4()), value=file_data,
                        on_delivery=delivery_report)

    producer.flush()

    experiment_dir = f'./{settings.EXPERIMENTS_DATA_DIR}/{experiment_name}'
    create_directory(experiment_dir)

    # write chunking time to file and total number of collisions to file
    with open(f'{experiment_dir}/{experiment_name}_chunking_milisec.csv', 'a') as file:
        file.write(f'{file_name};{(end-start) / 1000000}\n')


def read_and_process_files(files_names, dir_path, experiment_name, 
                           min_size, avg_size, max_size, hash_function):
    counter = 0
    for file_name in files_names:
        producer.poll(0.0)

        if counter % 1000 == 0:
            print(f'{counter} files were send.')

        file_relative_path = f'{dir_path}/{file_name}'
        file = open(file_relative_path, 'rb')
        file_content_bytes = file.read()

        counter += 1

        process_and_publish_file(producer, file_name, experiment_name, file_content_bytes,
                                min_size, avg_size, max_size, hash_function)


time.sleep(settings.WAIT_BEFORE_START)

MIN_SIZE = 64
AVG_SIZE = 256
MAX_SIZE = 1024

HASH_FUNCTIONS = {
    'sha256': sha256,
    'md5': md5,
    'sha1': sha1
}

producer = set_up_producer()

create_directory(settings.EXPERIMENTS_DATA_DIR)

files_producer_routes = Blueprint('files_producer_routes', __name__)

@files_producer_routes.route('/upload', methods=['POST'])
def upload_file():
    if 'experiment_name' not in request.form:
        return handle_not_valid_request(res, 'Provide any experiment name in experiment_name field.')

    if 'hash_function' not in request.form or request.form.get('hash_function') not in HASH_FUNCTIONS:
        return handle_not_valid_request(res, 'Provide valid hash function (sha256, md5 or sha1) in hash_function field')

    # handling file, source: https://roytuts.com/python-flask-rest-api-file-upload/
    if 'file' not in request.files:
        return handle_not_valid_request(res, 'There is no file in part of request.')

    uploaded_file = request.files['file']
    if uploaded_file.filename == '':
        return handle_not_valid_request('No file was uploaded.')

    try:
        min_size = int(request.form.get('min_size')) if 'min_size' in request.form \
            and int(request.form.get('min_size')) and int(request.form.get('min_size')) > MIN_SIZE else MIN_SIZE
        avg_size = int(request.form.get('avg_size')) if 'avg_size' in request.form \
            and int(request.form.get('avg_size')) and int(request.form.get('avg_size')) > AVG_SIZE else AVG_SIZE
        max_size = int(request.form.get('max_size')) if 'max_size' in request.form \
            and int(request.form.get('max_size')) and int(request.form.get('max_size')) > MAX_SIZE else MAX_SIZE
        
        experiment_name = request.form.get('experiment_name')
        hash_function = HASH_FUNCTIONS[request.form.get('hash_function')]

        file_content_bytes = uploaded_file.read()

        producer.poll(0.0)

        process_and_publish_file(producer, uploaded_file.filename, experiment_name, file_content_bytes,
                                 min_size, avg_size, max_size, hash_function)
        
        res = jsonify({
            'message': 'File was successfully uploaded.'
        })
        res.status_code = 200

    except ValueError:
        return handle_not_valid_request(res, 'min_size, avg_size or max_size isn\'t interger.')

    except Exception as e:
        print(e)
        res = jsonify({
            'message': 'Something went wrong.'
        })
        res.status_code = 500
        
    return res


@files_producer_routes.route('/load-from-fs', methods=['POST'])
def load_data_from_fs():
    res = None

    if 'experiment_name' not in request.form:
        return handle_not_valid_request(res, 'Provide any experiment name in experiment_name field.')

    if 'hash_function' not in request.form or request.form.get('hash_function') not in HASH_FUNCTIONS:
        return handle_not_valid_request(res, 'Provide valid hash function (sha256, md5 or sha1) in hash_function field') 

    dir_path = None
    if 'directory_path' in request.form:
        dir_path = request.form['directory_path']
        if not os.path.exists(dir_path) or not os.path.isdir(dir_path):
            return handle_not_valid_request(res, 'Provide valid relative path to experiments input data')  
    
    try:
        min_size = int(request.form.get('min_size')) if 'min_size' in request.form \
            and int(request.form.get('min_size')) and int(request.form.get('min_size')) > MIN_SIZE else MIN_SIZE
        avg_size = int(request.form.get('avg_size')) if 'avg_size' in request.form \
            and int(request.form.get('avg_size')) and int(request.form.get('avg_size')) > AVG_SIZE else AVG_SIZE
        max_size = int(request.form.get('max_size')) if 'max_size' in request.form \
            and int(request.form.get('max_size')) and int(request.form.get('max_size')) > MAX_SIZE else MAX_SIZE
        
        experiment_name = request.form.get('experiment_name')
        hash_function = HASH_FUNCTIONS[request.form.get('hash_function')]

        files_names = os.listdir(dir_path)

        read_and_process_files(files_names, dir_path, experiment_name, min_size, avg_size, max_size, hash_function)

        res = jsonify({
            'message': 'Uploading process finished.'
        })
        res.status_code = 200

    except ValueError:
        return handle_not_valid_request(res, 'min_size, avg_size or max_size isn\'t interger.')

    except Exception as e:
        print(e)
        res = jsonify({
            'message': 'Something went wrong.'
        })
        res.status_code = 500
        
    return res
    
