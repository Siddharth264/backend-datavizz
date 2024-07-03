from flask import Flask, jsonify, request
from flask_pymongo import PyMongo
from bson.objectid import ObjectId
from bson.json_util import dumps
import json
import os

app = Flask(__name__)
app.config["MONGO_URI"] = os.environ.get('MONGO_URI')
mongo = PyMongo(app)

@app.route('/', methods=['GET'])
def home():
    return jsonify({'message': 'Hello World'})

@app.route('/data', methods=['GET'])
def get_all_data():
    data = mongo.db.your_collection.find()
    return dumps(data), 200

@app.route('/data/<id>', methods=['GET'])
def get_data_by_id(id):
    data = mongo.db.your_collection.find_one({'_id': ObjectId(id)})
    if data:
        return dumps(data), 200
    else:
        return jsonify({'error': 'Data not found'}), 404

def load_data_to_mongodb(file_path):
    if mongo.db.your_collection.count_documents({}) == 0:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if isinstance(data, list):
                mongo.db.your_collection.insert_many(data)
            else:
                mongo.db.your_collection.insert_one(data)
        print("Data loaded into MongoDB.")
    else:
        print("Data already exists in MongoDB. Skipping insertion.")

if __name__ == '__main__':
    json_file_path = os.path.join(os.path.dirname(__file__), 'jsondata.json')
    if os.path.exists(json_file_path):
        load_data_to_mongodb(json_file_path)
    app.run(debug=True)
