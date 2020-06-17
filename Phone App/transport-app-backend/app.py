from flask import Flask, request, render_template, jsonify
from predictions import Prediction
from tfl_pred import TfL
import requests
import os
import datetime as dt
import numpy as np
import pickle
import psycopg2

app = Flask(__name__)
# app.config.from_object(os.environ['APP_SETTINGS'])
model = Prediction()
tfl = TfL()

infile = open("pickles/part1_trained_vals",'rb')
part1_vals = pickle.load(infile)
infile.close()

infile = open("pickles/alpha",'rb')
alpha = pickle.load(infile)
infile.close()

infile = open("pickles/part1_scaler",'rb')
part1_scaler = pickle.load(infile)
infile.close()


@app.route('/', methods=['GET', 'POST'])
def init():
    testObj = {
        "time": "couldn't calculate predicted journey time"
    }

    if request.method == "POST":
        # get url that the user has entered
        try:
            response = request.json
            stop_a = response.get("from")
            stop_b = response.get("to")
            route = response.get("route")

            if (stop_a in storedStops.keys()) and (stop_b in storedStops.keys()):
                a_id = storedStops.get(stop_a)
                b_id = storedStops.get(stop_b)

                print(a_id, b_id)
                tflPredTime = tfl.tfl_predict(a_id, b_id, route)

                print(tflPredTime)
                testObj["time"] = tflPredTime

        except:
            print("Unable to get URL. Please make sure it's valid and try again.")

    return jsonify(testObj)


@app.route('/predict', methods=['GET', 'POST'])
def predictTime():
    testObj = {
        "success": True,
        "time": "couldn't calculate predicted journey time",
        "tflTime": False,
        "fromError": False
    }

    storedStops = {
        "Willesden Bus Garage": "490014687E",
        "Okehampton Road": "490010521S",
        "Notting Hill Gate Station": "490015039C",
        "North End Road": "490010357F",
        "Phillimore Gardens": "490010984U",
        "Piccadilly Circus": "490000179B"
    }

    if request.method == "POST":
        # get url that the user has entered
        try:
            response = request.json
            print("REQUEST : ", response)

            stop_a = response.get("from")
            stop_b = response.get("to")
            route = response.get("route")

            gmt = dt.timezone.utc

            time_of_request = dt.datetime.now(tz=gmt)
            day_of_week, time_of_day = model.convert_given_global_data(time_of_request)
            dow = np.array([day_of_week])
            gap = model.get_gap(stop_a, stop_b, route)
            print("Found gap, day of week and time of day")

            global_vals = np.append(gap, dow)
            global_vals = np.append(global_vals, time_of_day[0])
            global_vals = global_vals.reshape(1, -1)
            scaler = part1_scaler.get("scaler")
            transformed = scaler.transform(global_vals)[0]
            print("Scaled data")

            start_stop, end_stop = model.get_recent_journeys_from_db(stop_a, stop_b, route)

            if len(start_stop) == 0:
                print("Invalid stops given")
                testObj["fromError"] = True
                testObj["success"] = False 

            last_10_journeys = model.calc_journey_times(start_stop, end_stop)
            print("Found last 10 journeys")

            # Do part 1 prediction
            part1_intercept = part1_vals.get("intercept")
            part1_coeffs = part1_vals.get("coeffs")

            multiplied = np.multiply(part1_coeffs, transformed)
            summed = np.sum(multiplied)

            part1_pred = part1_intercept + summed
            print("Part 1 prediction: ", part1_pred)

            # Do part 2 prediction
            part2_pred = model.calc_part2_prediction(last_10_journeys)
            print("Part 2 prediction: ", part2_pred)

            # Get combined prediction
            best_alpha = alpha.get("alpha")
            y_pred = best_alpha * (part1_pred) + (1 - best_alpha) * (part2_pred)

            print("Overall prediction: ", y_pred)

            # Try to get TfL prediction
            if (stop_a in storedStops.keys()) and (stop_b in storedStops.keys()):
                a_id = storedStops.get(stop_a)
                b_id = storedStops.get(stop_b)

                tflPredTime = tfl.tfl_predict(a_id, b_id, route)

                print(tflPredTime)
                testObj["tflTime"] = tflPredTime

            # Send prediction back to front end
            testObj["time"] = y_pred
            return jsonify(testObj)

        except:
            print("Error trying to calculate prediction")

    return jsonify(testObj)


if __name__ == '__main__':
    app.run(host='0.0.0.0')