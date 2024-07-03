from flask import Flask, jsonify, request
from flask_pymongo import PyMongo
from bson.objectid import ObjectId
from bson.json_util import dumps
import json
import os
from collections import defaultdict
import random

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

@app.route('/data/fields', methods=['GET'])
def get_data_by_fields():
    try:
        if db is not None:
            fields = request.args.getlist('field')
            if not fields:
                return jsonify({'error': 'No fields specified'}), 400

            # Build the projection dictionary
            projection = {field: 1 for field in fields}
            
            # Query the database with projection and limit
            data = db.datavizz.find({}, projection).limit(50)
            return dumps(data), 200
        else:
            return jsonify({'error': 'Database connection error'}), 500
    except Exception as e:
        return jsonify({'error': f'Database error: {str(e)}'}), 500

@app.route('/data/summary', methods=['GET'])
def get_data_summary():
    try:
        if db is not None:
            data = db.datavizz.find()
            region_summary = defaultdict(lambda: defaultdict(int))
            
            for entry in data:
                region = entry.get('region')
                country = entry.get('country')
                
                # Skip entries with empty region or country
                if not region or not country:
                    continue
                
                region_summary[region][country] += 1
            
            summary = []
            for region, countries in region_summary.items():
                region_data = {'region': region}
                for country, count in countries.items():
                    region_data[country] = count
                    region_data[f"{country}Color"] = f"hsl({random.randint(0, 360)}, 70%, 50%)"
                summary.append(region_data)
            
            return jsonify(summary), 200
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
