from flask import Flask, jsonify
from flask_pymongo import PyMongo
from bson.objectid import ObjectId
from bson.json_util import dumps
import json
import os

app = Flask(__name__)
app.config["MONGO_URI"] = 'mongodb+srv://sidsingh264:siddharth@datavizz.v6il78s.mongodb.net/datavizz?retryWrites=true&w=majority&appName=datavizz'

mongo = PyMongo(app)
db = mongo.db  # Access the database through the PyMongo instance

if db is not None:
    print("Connected to MongoDB.")
else:
    print("Error connecting to MongoDB.")

@app.route('/', methods=['GET'])
def home():
    return jsonify({'message': 'Hello World'})

@app.route('/data', methods=['GET'])
def get_all_data():
    try:
        if db is not None:
            data = db.datavizz.find()
            return dumps(data), 200
        else:
            return jsonify({'error': 'Database connection error'}), 500
    except Exception as e:
        return jsonify({'error': f'Database error: {str(e)}'}), 500

@app.route('/data/<id>', methods=['GET'])
def get_data_by_id(id):
    try:
        if db is not None:
            data = db.datavizz.find_one({'_id': ObjectId(id)})
            if data:
                return dumps(data), 200
            else:
                return jsonify({'error': 'Data not found'}), 404
        else:
            return jsonify({'error': 'Database connection error'}), 500
    except Exception as e:
        return jsonify({'error': f'Database error: {str(e)}'}), 500

def load_data_to_mongodb(file_path):
    try:
        if db is not None:
            if db.datavizz.count_documents({}) == 0:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        db.datavizz.insert_many(data)
                    else:
                        db.datavizz.insert_one(data)
                print("Data loaded into MongoDB.")
            else:
                print("Data already exists in MongoDB. Skipping insertion.")
        else:
            print("Database connection error. Unable to load data.")
    except Exception as e:
        print(f"Error loading data to MongoDB: {e}")

if __name__ == '__main__':
    json_file_path = os.path.join(os.path.dirname(__file__), 'jsondata.json')
    if os.path.exists(json_file_path):
        load_data_to_mongodb(json_file_path)
    app.run(debug=True)
