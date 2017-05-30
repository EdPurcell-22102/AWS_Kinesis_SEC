##
##
## Connect to Kinesis, create a new stream "SalientCRGTSECDemo" in region US-East-1
##
## C:/EdsScripts/KinesisSECCreateStream.py
##
## 2017-05-24 - Modified wording of "Waiting 30 seconds" message
##

import boto3
import time

nShardCount = 10

strRegionName = 'us-east-1'
strStreamName = 'SalientCRGTSECDemo'

kinesis_client = boto3.client('kinesis', region_name = strRegionName)

print ('\nCreating stream %s with %i shards in region %s \n' % (strStreamName, nShardCount, strRegionName))

dctResponse1 = kinesis_client.create_stream(StreamName = strStreamName, ShardCount = nShardCount)

print ('Response from create stream command:  %s \n\nWaiting for 30 seconds to check whether stream was successfully created... ' % (dctResponse1))

time.sleep(30)

dctResponse2 = kinesis_client.list_streams(Limit = 10)

print ('Response from list streams command:  ',dctResponse2)

