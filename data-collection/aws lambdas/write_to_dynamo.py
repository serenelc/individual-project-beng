import json
import boto3
import csv
import datetime as dt
from pathlib import Path

def lambda_handler(event, context):
    dynamodb = boto3.client('dynamodb')
    
    file_name = 'bus_data.csv'
    bus_file = Path.cwd() / file_name

    if bus_file.is_file():
        try:
            with open(file_name) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter = ',')
                line_count = 0
                for row in csv_reader:
                    if line_count != 0:
                        vehicle_id = row[0]
                        bus_stop_name = row[1]
                        direction = row[2]
                        eta = row[3]
                        timestamp = row[4]
                        arrived = 'True' if row[5] == 'True' else 'False'
                        
                        dynamodb.put_item(TableName='bus_arrivals_9', Item={'vehicle_id': {'S': vehicle_id},
                                                        'bus_stop_name': {'S': bus_stop_name},
                                                        'direction': {'N': direction},
                                                        'expected_arrival': {'S': eta},
                                                        'timestamp': {'S': timestamp},
                                                        'arrived': {'S': arrived}})
    

                    line_count += 1
        except IOError:
            print("I/O error in loading information from csv file into dynamodb")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Success')
    }
