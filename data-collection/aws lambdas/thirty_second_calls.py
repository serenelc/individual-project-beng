import json
import time
import boto3

def lambda_handler(event, context):
    client = boto3.client('lambda')
    # bus_routes = ["452", "9", "52", "328", "277", "267", "7", "14"]
    bus_routes = ["452", "9"]
    
    for route in bus_routes:
        #Pass in bus route
        json_bus = json.dumps(route)
        
        print("Calling lambda 1st time for bus route {}".format(route))
        response = client.invoke(
            FunctionName= 'get_bus_arrival_times',
            InvocationType='Event',
            Payload= json_bus
        )
        print(response.get("StatusCode"))
    
    print("SLEEP")
    time.sleep(30)
    
    for route in bus_routes:
        #Pass in bus route
        json_bus = json.dumps(route)
        print("Calling lambda 2nd time for bus route {}".format(route))
        response = client.invoke(
            FunctionName='get_bus_arrival_times',
            InvocationType='Event',
            Payload= json_bus
        )
        print(response.get("StatusCode"))
    
    return {
        'statusCode': 200,
        'body': json.dumps('Completed 2 calls to get_bus_arrival_times')
    }
