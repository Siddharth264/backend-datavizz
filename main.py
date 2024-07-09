from flask import Flask, jsonify, request
from flask_pymongo import PyMongo
from bson.objectid import ObjectId
from bson.json_util import dumps
import json
import os
from collections import defaultdict
import random
from flask_cors import CORS
from datetime import datetime
app = Flask(__name__)
CORS(app)

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
                sector = entry.get('sector')
                
                # Skip entries with empty region or sector
                if not region or not sector:
                    continue
                
                region_summary[region][sector] += 1
            
            summary = []
            for region, sectors in region_summary.items():
                region_data = [('region', region)]  # Start with region as the first tuple
                for sector, count in sectors.items():
                    region_data.append((sector, count))
                    region_data.append((f"{sector}Color", f"hsl({random.randint(0, 360)}, 70%, 50%)"))
                summary.append(dict(region_data))  # Convert list of tuples to dict
            
            # Sort regions by total papers and get the top 7
            summary = sorted(summary, key=lambda x: sum([v for k, v in x.items() if k != 'region' and not k.endswith('Color')]), reverse=True)[:7]
            
            return jsonify(summary), 200
        else:
            return jsonify({'error': 'Database connection error'}), 500
    except Exception as e:
        return jsonify({'error': f'Database error: {str(e)}'}), 500


@app.route('/data/sectors', methods=['GET'])
def get_distinct_sectors():
    try:
        if db is not None:
            pipeline = [
                {'$match': {'sector': {'$ne': ''}}},
                {'$group': {'_id': '$sector', 'count': {'$sum': 1}}},
                {'$sort': {'count': -1}},
                {'$limit': 6}
            ]
            sectors = list(db.datavizz.aggregate(pipeline))
            result = [{'sector': s['_id'], 'count': s['count']} for s in sectors]
            return jsonify(result), 200
        else:
            return jsonify({'error': 'Database connection error'}), 500
    except Exception as e:
        return jsonify({'error': f'Database error: {str(e)}'}), 500

@app.route('/data/countries', methods=['GET'])
def get_country_data():
    try:
        if db is not None:
            data = db.datavizz.find()
            # Load country IDs from the uploaded JSON file
            with open('country_ids.json', 'r', encoding='utf-8') as file:
                country_ids = json.load(file)
            
            country_paper_counts = defaultdict(int)
            for entry in data:
                country = entry.get('country')
                if country and country in country_ids:
                    country_paper_counts[country_ids[country]] += 1

            result = [{'id': country_id, 'value': count} for country_id, count in country_paper_counts.items()]
            return jsonify(result), 200
        else:
            return jsonify({'error': 'Database connection error'}), 500
    except Exception as e:
        return jsonify({'error': f'Database error: {str(e)}'}), 500
    

@app.route('/data/countries_relevance', methods=['GET'])
def get_country_relevance():
    try:
        if db is not None:
            pipeline = [
                {'$match': {'country': {'$ne': ''}}},  # Filter out documents where country is empty
                {'$group': {'_id': '$country', 'relevance_score': {'$sum': '$relevance'}}}
            ]
            country_relevance = list(db.datavizz.aggregate(pipeline))
            
            result = [{'country': entry['_id'], 'relevance_score': entry['relevance_score']} for entry in country_relevance]
            result = sorted(result, key=lambda x: x['relevance_score'], reverse=True)

            return jsonify(result), 200
        else:
            return jsonify({'error': 'Database connection error'}), 500
    except Exception as e:
        return jsonify({'error': f'Database error: {str(e)}'}), 500             

@app.route('/data/metrics', methods=['GET'])
def get_country_metrics():
    try:
        if db is not None:
            pipeline = [
                {"$match": {"$and": [
                    {"country": {"$ne": ""}},
                    {"topic": {"$ne": ""}},
                    {"sector": {"$ne": ""}},
                    {"pestle": {"$ne": ""}}
                ]}},
                {"$group": {
                    "_id": "$country",
                    "total_relevance": {"$sum": "$relevance"},
                    "total_intensity": {"$sum": "$intensity"},
                    "most_frequent_topic": {"$push": "$topic"},
                    "most_frequent_sector": {"$push": "$sector"},
                    "most_frequent_pestle": {"$push": "$pestle"}
                }},
                {"$addFields": {
                    "most_frequent_topic": {"$arrayElemAt": ["$most_frequent_topic", 0]},
                    "most_frequent_sector": {"$arrayElemAt": ["$most_frequent_sector", 0]},
                    "most_frequent_pestle": {"$arrayElemAt": ["$most_frequent_pestle", 0]}
                }},
                {"$project": {
                    "_id": 0,
                    "country": "$_id",
                    "total_relevance": 1,
                    "total_intensity": 1,
                    "most_frequent_topic": 1,
                    "most_frequent_sector": 1,
                    "most_frequent_pestle": 1
                }}
            ]

            country_metrics = list(db.datavizz.aggregate(pipeline))
            if country_metrics:
                return jsonify(country_metrics), 200
            else:
                return jsonify({'error': 'No data available'}), 404
        else:
            return jsonify({'error': 'Database connection error'}), 500
    except Exception as e:
        return jsonify({'error': f'Database error: {str(e)}'}), 500

@app.route('/data/topics', methods=['GET'])
def get_topic_data():
    try:
        if db is not None:
            data = db.datavizz.find()
            topic_counts = defaultdict(int)
            for entry in data:
                topic = entry.get('topic')
                if topic:
                    topic_counts[topic] += 1

            result = [
                {
                    'id': topic,
                    'label': topic,
                    'value': count,
                    'color': f'hsl({random.randint(0, 360)}, 70%, 50%)'
                } for topic, count in topic_counts.items()
            ]
            result = sorted(result, key=lambda x: x['value'], reverse=True)[:6]
            return jsonify(result), 200
        else:
            return jsonify({'error': 'Database connection error'}), 500
    except Exception as e:
        return jsonify({'error': f'Database error: {str(e)}'}), 500


@app.route('/data/time_series', methods=['GET'])
def get_time_series_data():
    try:
        if db is not None:
            data = db.datavizz.find({}, {'published': 1})  # Retrieve only the published field
            year_counts = defaultdict(int)
            for entry in data:
                date_str = entry.get('published')
                if date_str:
                    try:
                        # Parse date assuming format is 'January, 20 2017 03:51:25'
                        date_obj = datetime.strptime(date_str, '%B, %d %Y %H:%M:%S')
                        year_counts[date_obj.year] += 1
                    except ValueError:
                        # Handle cases where the date format is different
                        continue
            # Sort the data by year
            sorted_data = sorted(year_counts.items())
            result = [{'year': year, 'count': count} for year, count in sorted_data]
            return jsonify(result), 200
        else:
            return jsonify({'error': 'Database connection error'}), 500
    except Exception as e:
        return jsonify({'error': f'Database error: {str(e)}'}), 500
    
@app.route('/data/pestles', methods=['GET'])
def get_pestle_data():
    try:
        if db is not None:
            data = db.datavizz.find()
            pestle_counts = defaultdict(int)
            for entry in data:
                pestle = entry.get('pestle')
                if pestle:
                    pestle_counts[pestle] += 1

            result = [
                {
                    'id': pestle,
                    'label': pestle,
                    'value': count,
                    'color': f'hsl({random.randint(0, 360)}, 70%, 50%)'
                } for pestle, count in pestle_counts.items()
            ]
            result = sorted(result, key=lambda x: x['value'], reverse=True)[:6]
            return jsonify(result), 200
        else:
            return jsonify({'error': 'Database connection error'}), 500
    except Exception as e:
        return jsonify({'error': f'Database error: {str(e)}'}), 500

@app.route('/data/sectorspie', methods=['GET'])
def get_sector_data():
    try:
        if db is not None:
            data = db.datavizz.find()
            sector_counts = defaultdict(int)
            for entry in data:
                sector = entry.get('sector')
                if sector:
                    sector_counts[sector] += 1

            result = [
                {
                    'id': sector,
                    'label': sector,
                    'value': count,
                    'color': f'hsl({random.randint(0, 360)}, 70%, 50%)'
                } for sector, count in sector_counts.items()
            ]
            result = sorted(result, key=lambda x: x['value'], reverse=True)[:6]
            return jsonify(result), 200
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
