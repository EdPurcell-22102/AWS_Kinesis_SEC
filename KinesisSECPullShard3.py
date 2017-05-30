##
##
## C:/EdsScripts/KinesisPullSECShard3.py
##
## Pull multiple records stock trade records from Kinesis stream (shard 2) in JSON format and unwrap them
##
##  2017-05-25  Clean up output print statements
##  2017-05-16  Add nShardIdNdx, nSleepTime, nTotalPulled, delete debug print statements
##

import boto3
import json
import time


dctResponse          = {}

lstShardIds          = []

nRecLimit            = 50
nShardIdNdx          = 2
nSleepTime           = 1.0
nTotalPulled         = 0

strKinesis           = 'kinesis'
strNewShardIterator  = ''
strPartitionKey      = '1'
strRegionName        = 'us-east-1'
strShardId           = ''
strShardIteratorType = 'TRIM_HORIZON'
# strShardIteratorType = 'LATEST'
strStreamName        = 'SalientCRGTSECDemo'


kinesis_client = boto3.client(strKinesis, region_name = strRegionName)

#---Get the Shard ID-------------------

dctResponse = kinesis_client.describe_stream(StreamName = strStreamName)

dctStreamDescription = dctResponse['StreamDescription']

for dctShard in dctStreamDescription['Shards']:
	lstShardIds.append(dctShard['ShardId'])
	nNbrShards = len(dctStreamDescription['Shards'])

strShardId = lstShardIds[nShardIdNdx]

dctShardIterator = kinesis_client.get_shard_iterator(StreamName = strStreamName, 
											ShardId = strShardId, 
											ShardIteratorType = strShardIteratorType)

strNewShardIterator = dctShardIterator['ShardIterator']

print ('\nPulling batches of %i records from Kinesis stream %s, shard = %s' % (nRecLimit, strStreamName, strShardId))

dctResponse = kinesis_client.get_records(ShardIterator = strNewShardIterator, Limit = nRecLimit)

lstRecords = dctResponse['Records']
dctRecords0 = lstRecords[0]
bstrRecords0 = dctRecords0['Data']
strRecords0 = bstrRecords0.decode("utf-8")
dctData = json.loads(strRecords0)

print ('\nReturned data = %s \n' % (dctData))
print ('Time = %s' % (dctData['Time']))
print ('Exchange = %s' % (dctData['Exchange']))
print ('Trade_Volume = %s' % (dctData['Trade_Volume']))
print ('Trade_Price = %s\n' % (dctData['Trade_Price']))

nLenLstRecords = len(dctResponse['Records'])
nTotalPulled   += nLenLstRecords

print ('Number of returned records = %i, total = %i ' % (nLenLstRecords, nTotalPulled))

strNewShardIterator = dctResponse['NextShardIterator']

time.sleep(nSleepTime)


while nLenLstRecords > 1:
	dctResponse = kinesis_client.get_records(ShardIterator = strNewShardIterator, Limit = nRecLimit)
	
	strNewShardIterator = dctResponse['NextShardIterator']
	nLenLstRecords = len(dctResponse['Records'])
	nTotalPulled   += nLenLstRecords
	if nLenLstRecords > 0:
		print ('Number of returned records = %i, total = %i ' % (nLenLstRecords, nTotalPulled))
	
	time.sleep(nSleepTime)

	