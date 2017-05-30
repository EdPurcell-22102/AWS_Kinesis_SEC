##
##
## Connect to Kinesis, get descriptive data for stream "SalientCRGTSECDemo" in region US-East-1
##
## C:/EdsScripts/KinesisSECDescribeStream.py
##
## 2017-05-25  Modify final print statements to make output prettier
## 2017-05-15  Delete debug print statements
##
##

import boto3
import time

lstShardIds = []

nNbrShards  = 0

strKinesis    = 'kinesis'
strRegionName = 'us-east-1'
strStreamName = 'SalientCRGTSECDemo'

kinesis_client = boto3.client(strKinesis, region_name = strRegionName)

dctResponse = kinesis_client.describe_stream(StreamName = strStreamName)

# print ('Response from create stream command:  %s \n' % (dctResponse))

dctStreamDescription = dctResponse['StreamDescription']
print ('\nStream Name = %s\n' % (dctStreamDescription['StreamName']))

for dctShard in dctStreamDescription['Shards']:
	print ('ShardID = %s' % (dctShard['ShardId']))
	lstShardIds.append(dctShard['ShardId'])
	nNbrShards = len(dctStreamDescription['Shards'])
print ('\nIn stream %s there are %i shards.' % (dctStreamDescription['StreamName'], nNbrShards))
