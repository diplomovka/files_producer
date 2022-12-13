import os

FILES_TOPIC = str(os.getenv('FILES_TOPIC') or 'FILES_TOPIC')
FILES_SCHEMA_PATH = str(os.getenv('FILES_SCHEMA_PATH') or './avro_files/files.avsc')

BOOTSTRAP_SERVERS = str(os.getenv('BOOTSTRAP_SERVERS') or 'localhost:9092')
SCHEMA_REGISTRY_URL = str(os.getenv('SCHEMA_REGISTRY_URL') or 'http://localhost:8085')
ENCODING = str(os.getenv('ENCODING') or 'utf_8')
AVRO_FILES_ENCODING = str(os.getenv('AVRO_FILES_ENCODING') or 'utf-8')
