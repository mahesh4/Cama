from flask import Flask, request, abort
from arcgis2geojson import arcgis2geojson
import cama_convert

app = Flask(__name__)


@app.route('/')
def index():
    # completed, working
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
        response = cama_convert.do_request(request_data)
        return response
    except Exception as e:
        abort(500, e)


@app.route('/reservoir_flow', methods=['POST'])
def reservoir_flow():
    # completed
    request_data = request.get_json()
    request_data['request'] = 'plot_hydrograph_nearest_reservoir'
    try:
        response = cama_convert.do_request(request_data)
        return response
    except Exception as e:
        abort(500, e)


@app.route('/comparative_flow', methods=['POST'])
def comparative_flow():
    request_data = request.get_json()
    print(request_data)
    request_data['request'] = 'plot_hydrograph_deltas'
    try:
        response = cama_convert.do_request(request_data)
        print('response is :')
        print(response)
        return response
    except Exception as e:
        print(e)
        abort(500, e)


@app.route('/vegetation_lookup', methods=['POST'])
def vegetation_lookup():
    request_data = request.get_json()
    request_data['request'] = 'veg_lookup'
    try:
        response = cama_convert.do_request(request_data)
        return response
    except Exception as e:
        abort(500, e)


@app.route('/update_manning', methods=['POST'])
def update_manning():
    request_data = request.get_json()
    request_data['request'] = 'update_manning'
    try:
        response = cama_convert.do_request(request_data)
        return response
    except Exception as e:
        abort(500, e)


@app.route('/cama_status/pre', methods=['POST'])
def cama_status_pre():
    request_data = request.get_json()
    request_data['request'] = 'cama_status_pre'
    try:
        response = cama_convert.do_request(request_data)
        return response
    except Exception as e:
        abort(500, e)


@app.route('/cama_status/post', methods=['POST'])
def cama_status_post():
    request_data = request.get_json()
    request_data['request'] = 'cama_status_post'
    try:
        response = cama_convert.do_request(request_data)
        return response
    except Exception as e:
        abort(500, e)


@app.route('/cama_set', methods=['POST'])
def cama_set():
    request_data = request.get_json()
    request_data['request'] = 'cama_set'
    try:
        response = cama_convert.do_request(request_data)
        return response
    except Exception as e:
        abort(500, e)


@app.route('/cama_run/pre', methods=['POST'])
def came_run_pre():
    request_data = dict()
    request_data['request'] = 'cama_run_pre'
    try:
        response = cama_convert.do_request(request_data)
        return response
    except Exception as e:
        abort(500, e)


@app.route('/cama_run/post', methods=['POST'])
def came_run_post():
    request_data = dict()
    request_data['request'] = 'cama_run_post'
    try:
        response = cama_convert.do_request(request_data)
        return response
    except Exception as e:
        abort(500, e)


@app.route('/coord_to_grid', methods=['POST'])
def coor_to_grid():
    request_data = request.get_json()
    request_data['request'] = 'coord_to_grid'
    try:
        response = cama_convert.do_request(request_data)
        return response
    except Exception as e:
        abort(500, e)


@app.route('/peak_flow', methods=['POST'])
def peak_flow():
    # completed, not working, the server disk space is less to hold data for 100 yrs(Need to check with 10 yrs data)
    request_data = request.get_json()
    request_data['request'] = 'peak_flow'
    try:
        response = cama_convert.do_request(request_data)
        return response
    except Exception as e:
        abort(500, e)


@app.route('/get_flow', methods=['POST'])
def get_flow():
    request_data = request.get_json()
    request_data['request'] = 'get_flow'
    try:
        response = cama_convert.do_request(request_data)
        return response
    except Exception as e:
        abort(500, e)


@app.route('/update_flow', methods=['POST'])
def update_flow():
    request_data = request.get_json()
    request_data['request'] = 'update_flow'
    print("update_flow called")
    try:
        response = cama_convert.do_request(request_data)
        return response
    except Exception as e:
        abort(500, e)


if __name__ == '__main__':
    app.run(host='0.0.0.0')  # run app in debug mode on port 80
