import json
import time
import boto3

def lambda_handler(event, context):
    client = boto3.client('lambda')
    
    print("Calling lambda 1st time")
    response = client.invoke(
        FunctionName= 'get_bus_arrival_times',
        InvocationType='Event'
    )
    
    print("SLEEP")
    time.sleep(30)
    print("Calling lambda 2nd time")
    response = client.invoke(
        FunctionName='get_bus_arrival_times',
        InvocationType='Event'
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Completed 2 calls to get_bus_arrival_times')
    }
