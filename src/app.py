from flask import Flask, request
from flask_restx import Api, Resource, fields

import redis
import pandas as pd
import os
from datetime import datetime


# Initialize flask App
app = Flask(__name__)
api = Api(app, version='1.0', title='Device Location API', description='APIs to retrieve device location data')

# connect to redis
cache = redis.Redis(host="redis", port=6379, db=0)

# Read data from CSV file and sort by sts column
data = pd.read_csv(os.environ["INCOMING_FILE_PATH"]).sort_values(by='sts')

# Group data by device ID
grouped_data = data.groupby('device_fk_id')

# Iterate over each group and store device info in Redis cache
for device_id, group in grouped_data:
    # Calculate start and end locations for device
    start_location = (group['latitude'].iloc[0], group['longitude'].iloc[0])
    end_location = (group['latitude'].iloc[-1], group['longitude'].iloc[-1])
    
    # Combine location points into list of tuples
    location_points = list(zip(group['latitude'], group['longitude'], group['time_stamp']))
    
    # Store device info in Redis hash
    cache.hset(device_id, 'latitude', group['latitude'].iloc[-1])
    cache.hset(device_id, 'longitude', group['longitude'].iloc[-1])
    cache.hset(device_id, 'time_stamp', group['time_stamp'].iloc[-1])
    cache.hset(device_id, 'start_lat', start_location[0])
    cache.hset(device_id, 'start_lon', start_location[1])
    cache.hset(device_id, 'end_lat', end_location[0])
    cache.hset(device_id, 'end_lon', end_location[1])
    cache.hset(device_id, 'location_points', str(location_points))



# Define data model for Swagger documentation
device_location = api.model('DeviceLocation', {
    'device_id': fields.Integer(),
    'latitude': fields.Float(),
    'longitude': fields.Float(),
    'time_stamp': fields.String()
})

start_end_location = api.model('StartEndLocation', {
    'device_id': fields.Integer(),
    'start_location': fields.Nested(api.model('StartLocation', {
        'latitude': fields.Float(),
        'longitude': fields.Float()
    })),
    'end_location': fields.Nested(api.model('EndLocation', {
        'latitude': fields.Float(),
        'longitude': fields.Float()
    }))
})

locations = api.model('Locations', {
    'device_id': fields.Integer(),
    'location_points': fields.List(
        fields.Nested(api.model('Location', {
            "latitude": fields.Float(),
            "longitude": fields.Float(),
            "time_stamp": fields.String()
        }))
    )
})


# Define API endpoints

@api.route('/latest-info/<int:device_id>')
class LatestInfo(Resource):

    @api.doc(params={'device_id': 'Device ID for which to retrieve latest information'})
    @api.response(200, 'Success', device_location)
    @api.response(404, 'Device ID not found')
    def get(self, device_id):
        try:
            
            device_data = cache.hgetall(device_id)
            if device_data is None:
                api.abort(404, 'Device ID not found')

            latest_data = {
                "device_id": device_id,
                "latitude": device_data[b'latitude'].decode('utf-8'),
                "longitude": device_data[b'longitude'].decode('utf-8'),
                "time_stamp": datetime.strptime(device_data[b'time_stamp'].decode('utf-8'), '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
            }

            return latest_data, 200
        except Exception as err:
            return {"message": err.__str__()}, 400


@api.route('/start-end-locations/<int:device_id>')
class StartEndLocations(Resource):

    @api.doc(params={'device_id': 'Device ID for which to retrieve start and end locations'})
    @api.response(200, 'Success', start_end_location)
    @api.response(404, 'Device ID not found')
    def get(self, device_id):
        try:
            
            device_data = cache.hgetall(device_id)
            if device_data is None:
                api.abort(404, 'Device ID not found')
            
            start_lat = float(device_data[b'start_lat'].decode('utf-8'))
            start_lon = float(device_data[b'start_lon'].decode('utf-8'))
            end_lat = float(device_data[b'end_lat'].decode('utf-8'))
            end_lon = float(device_data[b'end_lon'].decode('utf-8'))

            data = {
                'device_id': device_id,            
                'start_location': {'latitude': start_lat, 'longitude': start_lon},
                'end_location': {'latitude': end_lat, 'longitude': end_lon}
            }
            return data, 200
        except Exception as err:
            return {"message": err.__str__()}, 400


@api.route('/location-points/<int:device_id>')
class LocationPoints(Resource):
    @api.doc(params={'device_id': 'Device ID for which to retrieve location points', 'start_time': 'Start time in format yyyy-mm-dd hh:mm:ss', 'end_time': 'End time in format yyyy-mm-dd hh:mm:ss'})
    @api.response(200, 'Success', locations)
    @api.response(404, 'Device ID not found')
    def get(self, device_id):
        try:
            device_data = cache.hgetall(device_id)
            if device_data is None:
                api.abort(404, 'Device ID not found')

            start_time = datetime.strptime(request.args.get('start_time'), '%Y-%m-%d %H:%M:%S')
            end_time = datetime.strptime(request.args.get('end_time'), '%Y-%m-%d %H:%M:%S')

            location_points = []
            for data in eval(device_data[b'location_points'].decode('utf-8')):
                if start_time <= datetime.strptime(data[2], '%Y-%m-%dT%H:%M:%SZ') <= end_time:
                    location_points.append({
                        "latitude": data[0],
                        "longitude": data[1],
                        "time_stamp": datetime.strptime(data[2], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
                    })
            
            data = {
                "device_id": device_id,
                "location_points": location_points 
            }

            return data, 200
            
        except Exception as err:
            return {"message": err.__str__()}, 400



if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)