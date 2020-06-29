from flask import Flask, request, abort
from arcgis2geojson import arcgis2geojson
import json
import cama_convert
from db_connect import DBConnect
from flask import g

app = Flask(__name__)


def get_db():
    """Opens a new database connection if there is none yet for the
    current application context.
    """
    if not hasattr(g, 'mongodb'):
        db = DBConnect()
        db.connect_db()
        g.mongodb = db
    return g.mongodb.get_connection()


@app.teardown_appcontext
def close_db(error):
    db = g.pop('mongodb', None)
    if db is not None:
        db.disconnect_db()


# Mahesh: Done
@app.route('/')
def index():
    response = {
        'message': 'This service provided by the Center for Assured and Scalable Data Engineering, Arizona State '
                   'University. '
    }
    return response


@app.route('/to_geojson', methods=['POST'])
def to_geojson():
    # work in progress
    """Need to talk to Hans regarding the input and functionality"""
    response = dict()
    return response


@app.route('/to_arcgis', methods=['POST'])
def to_arcgis():
    # work in progress
    """Need to talk to Hans regarding the input and functionality"""
    response = dict()
    return response


@app.route('/wetland_flow', methods=['POST'])
def wetland_flow():
    request_data = request.get_json()
    request_data['request'] = 'plot_hydrograph_from_wetlands'
    try:
        mongo_client = get_db()
        response = cama_convert.do_request(request_data, mongo_client)
        return response
    except Exception as e:
        abort(500, e)


@app.route("/reservoir_flow", methods=["POST"])
def reservoir_flow():
    request_data = request.get_json()
    request_data["request"] = "plot_hydrograph_nearest_reservoir"
    try:
        mongo_client = get_db()
        response = cama_convert.do_request(request_data, mongo_client)
        return response
    except Exception as e:
        abort(500, e)


# Mahesh: Doubt regarding output
@app.route('/comparative_flow', methods=['POST'])
def comparative_flow():
    request_data = request.get_json()
    request_data['request'] = 'plot_hydrograph_deltas'
    try:
        mongo_client = get_db()
        response = cama_convert.do_request(request_data, mongo_client)
        return response
    except Exception as e:
        abort(500, e)


@app.route('/vegetation_lookup', methods=['POST'])
def vegetation_lookup():
    request_data = request.get_json()
    request_data["request"] = "veg_lookup"
    try:
        mongo_client = get_db()
        response = cama_convert.do_request(request_data, mongo_client)
        return response
    except Exception as e:
        abort(500, e)


@app.route('/update_manning', methods=['POST'])
def update_manning():
    request_data = request.get_json()
    request_data["request"] = "update_manning"
    try:
        mongo_client = get_db()
        response = cama_convert.do_request(request_data, mongo_client)
        return response
    except Exception as e:
        abort(500, e)


@app.route("/cama_status", methods=["POST"])
def cama_status():
    request_data = request.get_json()
    request_data["request"] = "cama_status"
    try:
        mongo_client = get_db()
        response = cama_convert.do_request(request_data, mongo_client)
        return response
    except Exception as e:
        abort(500, e)


@app.route('/cama_set', methods=['POST'])
def cama_set():
    request_data = request.get_json()
    request_data['request'] = 'cama_set'
    mongo_client = get_db()
    try:
        response = cama_convert.do_request(request_data, mongo_client)
        return response
    except Exception as e:
        abort(500, e)


@app.route('/cama_run', methods=['POST'])
def came_run():
    request_data = request.get_json()
    request_data['request'] = 'cama_run'
    try:
        mongo_client = get_db()
        response = cama_convert.do_request(request_data, mongo_client)
        return response
    except Exception as e:
        abort(500, e)


@app.route('/coord_to_grid', methods=['POST'])
def coor_to_grid():
    request_data = request.get_json()
    request_data['request'] = 'coord_to_grid'
    try:
        mongo_client = get_db()
        response = cama_convert.do_request(request_data, mongo_client)
        return response
    except Exception as e:
        abort(500, e)


@app.route('/peak_flow', methods=['POST'])
def peak_flow():
    # completed, not working, the server disk space is less to hold data for 100 yrs(Need to check with 10 yrs data)
    request_data = request.get_json()
    request_data['request'] = 'peak_flow'
    try:
        mongo_client = get_db()
        response = cama_convert.do_request(request_data, mongo_client)
        return response
    except Exception as e:
        abort(500, e)


@app.route('/get_flow', methods=['POST'])
def get_flow():
    request_data = request.get_json()
    request_data['request'] = 'get_flow'
    try:
        mongo_client = get_db()
        response = cama_convert.do_request(request_data, mongo_client)
        return response
    except Exception as e:
        abort(500, e)


@app.route('/update_groundwater', methods=['POST'])
def update_groundwater():
    request_data = request.get_json()
    request_data['request'] = 'update_wetland'
    try:
        mongo_client = get_db()
        response = cama_convert.do_request(request_data, mongo_client)
        return response
    except Exception as e:
        abort(500, e)


@app.route("/delete_results", methods=["POST"])
def delete_results():
    request_data = request.get_json()
    request_data["request"] = "delete_results"
    try:
        mongo_client = get_db()
        response = cama_convert.do_request(request_data, mongo_client)
        return response
    except Exception as e:
        abort(500, e)


@app.route("/get_results", methods=["GET"])
def list_results():
    try:
        mongo_client = get_db()
        database = mongo_client["output"]
        files_collection = database["files"]
        response = list(files_collection.find({}, {"_id": 0}))
        return json.dumps(response)
    except Exception as e:
        abort(500, e)


if __name__ == '__main__':
    app.run(host='0.0.0.0')  # run app in debug mode on port 80
