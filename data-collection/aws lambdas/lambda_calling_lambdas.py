import json
import time
import boto3

def lambda_handler(event, context):
    client = boto3.client('lambda')
    print("Calling lambda 1st time")
    response = client.invoke(
        FunctionName= 'write_bus_data_to_dynamo',
        InvocationType='Event'
    )
    print("SLEEP")
    time.sleep(10)
    print("Calling lambda 2nd time")
    response = client.invoke(
        FunctionName='write_bus_data_to_dynamo',
        InvocationType='Event'
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
