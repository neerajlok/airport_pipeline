import boto3
import json

def lambda_handler(event, context):
    # TODO implement
    sf = boto3.client('stepfunctions', region_name = 'us-east-1')
    response = sf.start_execution(stateMachineArn = 'arn:aws:states:us-east-1:992382771476:stateMachine:airline-state-machine')
    return {
        'statusCode': 200,
        'body': json.dumps('Successfully invoked Step function')
    }
