##
##
## Connect to Kinesis, list our streams in region US-East-1
## C:/EdsScripts/KinesisListStreams.py
##
##

import boto3
import json

client = boto3.client('kinesis', region_name='us-east-1')

dctResponse1 = client.list_streams(Limit = 10)

print (dctResponse1)



