import requests

url = 'http://localhost:5000/file/upload'

files = {'file': open('test.txt', 'rb')}
r = requests.post(url, files=files)
print(r)