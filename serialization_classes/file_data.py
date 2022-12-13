class FileData:
    def __init__(self, file_name, chunk, chunk_hash, chunk_serial_num, end_of_file):
        self.file_name = file_name
        self.chunk = chunk
        self.chunk_hash = chunk_hash
        self.chunk_serial_num = chunk_serial_num
        self.end_of_file = end_of_file
