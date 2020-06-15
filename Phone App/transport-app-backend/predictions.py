import calendar
import datetime as dt
import numpy as np
import psycopg2
import time
import math
import pickle

class Prediction(object):

    def convert_given_global_data(self, time_of_request):
        infile = open("pickles/one_hot_encoder",'rb')
        enc = pickle.load(infile)
        infile.close()

        one_hot_enc = enc.get("one_hot_encoder")
        
        d = calendar.day_name[time_of_request.weekday()]
        t = int(time_of_request.strftime('%H'))
            
        dow = 1
        
        if d in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]:
            dow = 0

        tod = one_hot_enc.transform(np.array([t]).reshape(-1, 1)).toarray()
        
        return dow, tod

    def calc_part2_prediction(self, last_10_journeys):

        weights = {"2": 0.55, "5": 0.35, "10": 0.1}

        pred = 0
        sum_weights = 0

        for j in range(0, 10):
            weight = 0

            if (j < 2):
                weight = weights["2"]
            elif (j < 5) & (j >= 2):
                weight = weights["5"]
            elif (j < 10) & (j >= 5):
                weight = weights["10"]
            elif (j < 15) & (j >= 10):
                weight = weights["15"]

            journey_time = last_10_journeys[j]

            if math.isnan(journey_time):
                continue

            pred += weight * journey_time
            sum_weights += weight

        if sum_weights == 0:
            pred = 0
            # shouldn't get here though

        pred = pred / sum_weights
        
        return pred

    def get_recent_journeys_from_db(self, start, end, route):
        
        table_name = "bus_information_" + route   
        results = {}

        conn = None
        try:
            conn = psycopg2.connect(host="db", database="postgres", user="postgres", password="example", port="5432")
            cursor = conn.cursor()
            sql = "SELECT *"
            sql += " FROM " + table_name
            sql += " WHERE bus_stop_name = " + start
            sql += " AND direction = 'inbound'"
            sql += " ORDER BY time_of_arrival DESC"
            sql += " LIMIT 10"
            
            cursor.execute(sql)
            results["start"] = cursor.fetchall() #list of tuples (vehicle_id, bus_stop_name, expected_arrival, time_of_req, direction)
            
            sql = "SELECT *"
            sql += " FROM " + table_name
            sql += " WHERE bus_stop_name = " + end
            sql += " AND direction = 'inbound'"
            sql += " ORDER BY time_of_arrival DESC"
            sql += " LIMIT 10"
            
            cursor.execute(sql)
            results["end"] = cursor.fetchall()
            
            cursor.close()
        
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error in getting recent journeys: ", error)
        
        finally:
            
            if conn is not None:
                conn.close()

            stop_b = []
            stop_a = []

            for bus in results.get("start"):
                vehicle_id = bus[0]
                bus_stop_name = bus[1]
                direction = bus[4]
                eta = bus[2]
                time_of_req = bus[3]

                vehicle_info = {
                                "vehicle_id": vehicle_id,
                                "bus_stop_name": bus_stop_name,
                                "direction": direction,
                                "expected_arrival": eta,
                                "time_of_req": time_of_req
                                }
                
                stop_b.append(vehicle_info)
                
            for bus in results.get("end"):
                vehicle_id = bus[0]
                bus_stop_name = bus[1]
                direction = bus[4]
                eta = bus[2]
                time_of_req = bus[3]

                vehicle_info = {
                                "vehicle_id": vehicle_id,
                                "bus_stop_name": bus_stop_name,
                                "direction": direction,
                                "expected_arrival": eta,
                                "time_of_req": time_of_req
                                }
                
                stop_a.append(vehicle_info)

            comp_time = time.time() - start
            print("Get journeys: ", comp_time)
            return stop_a, stop_b

        
    def calc_journey_times(self, start_stop, end_stop):
        journey_times = []
        for i, stop in enumerate(start_stop):
            time_a = stop.get("expected_arrival")
            time_b = end_stop[i].get("expected_arrival")
            diff = time_b - time_a + dt.timedelta(seconds = 30)
            journey_times.append(diff)
            
        # sorted by most recent journey first
        return journey_times
    