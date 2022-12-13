from flask import Flask
from blueprints.files_producer_routes import files_producer_routes

app = Flask(__name__)
app.register_blueprint(files_producer_routes, url_prefix='/file')

@app.route('/')
def main_route():
    return '<p>routes /file </p>' 

if __name__ == '__main__':
    app.run()