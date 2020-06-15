from flask import Flask, request, render_template, jsonify
from predictions import Prediction
import requests
import os
import datetime as dt
import numpy as np
import pickle
import psycopg2

app = Flask(__name__)
# app.config.from_object(os.environ['APP_SETTINGS'])
model = Prediction()

infile = open("pickles/part1_trained_vals",'rb')
part1_vals = pickle.load(infile)
infile.close()

infile = open("pickles/alpha",'rb')
alpha = pickle.load(infile)
infile.close()

@app.route('/db', methods=['GET', 'POST'])
def connectDb():
    table_name = "valid_stop_ids_52"   
    results = []

    conn = None
    try:
        print("CONNECTION ATTEMPT")

        conn = psycopg2.connect(host="localhost", database="bus_predictions", user="serenechongtrakul", port="5432")
        cursor = conn.cursor()
        sql = "SELECT *"
        sql += " FROM " + table_name
        
        cursor.execute(sql)
        results = cursor.fetchall() #list of tuples (stop_id, stop_name)

        print("CONNECTION MADE")
        cursor.close()
    
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error in getting recent journeys: ", error)
    
    finally:
        
        if conn is not None:
            conn.close()

        for bus in results:
            print(bus)

    return jsonify({"success": "Success!"})


@app.route('/', methods=['GET', 'POST'])
def init():
    testObj = {
        "success": True,
        "time": "couldn't calculate predicted journey time"
    }

    if request.method == "POST":
        # get url that the user has entered
        try:
            response = request.json
            print("REQUEST : ", response)

        except:
            print("Unable to get URL. Please make sure it's valid and try again.")

    return jsonify(testObj)


@app.route('/predict', methods=['GET', 'POST'])
def predictTime():
    testObj = {
        "success": True,
        "time": "couldn't calculate predicted journey time"
    }

    if request.method == "POST":
        # get url that the user has entered
        try:
            response = request.json
            print("REQUEST : ", response)

            stop_a = response.get("from")
            stop_b = response.get("to")
            route = response.get("route")

            time_of_request = dt.datetime.now()
            day_of_week, time_of_day = model.convert_given_global_data(time_of_request)
            dow = np.array([day_of_week])
            global_vals = np.append(dow, time_of_day[0])

            start_stop, end_stop = model.get_recent_journeys_from_db(stop_a, stop_b, route)
            last_10_journeys = model.calc_journey_times(start_stop, end_stop)

            # Do part 1 prediction
            part1_intercept = part1_vals.get("intercept")
            part1_coeffs = part1_vals.get("coeffs")
            part1_pred = part1_intercept + np.multiply(part1_coeffs, global_vals)

            # Do part 2 prediction
            part2_pred = model.calc_part2_prediction(last_10_journeys)

            # Get combined prediction
            best_alpha = alpha.get("alpha")
            y_pred = best_alpha * (part1_pred) + (1 - alpha) * (part2_pred)

            # Send prediction back to front end
            predObject = {
                "success": True,
                "time": y_pred
            }

            return jsonify(predObject)

        except:
            print("Unable to get URL. Please make sure it's valid and try again.")

    return jsonify(testObj)


if __name__ == '__main__':
    app.run(host='0.0.0.0')