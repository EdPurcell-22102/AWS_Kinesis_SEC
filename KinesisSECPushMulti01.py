##
##
## C:/EdsScripts/KinesisSECPushMulti01.py
##
## Read in a flat file, convert each line to JSON format and push to Kinesis multiple records at a time
##
## 2017-05-25 - Add error checking on the Kinesis put records command
## 2017-05-24 - Increase number of records in each batch
##              Decrease sleep time between each batch
##

import boto3
import json
import time

dctOutput              = {}
dctOutput2             = {}
dctRecord              = {}
dctResponse            = {}

lstBoth = ['0','1','2','3','4','5','6','7','8','9','10',
           '11','12','13','14','15','16','17','18','19',
		   '20','21','22','23','24','25','26','27','28','29'];
lstFieldNames   = [];
lstRecords      = [];

nNdx            = 0
nRecNbr         = 0
nRecsInBatch    = 0
nRecsLimit      = 500
nSleepTime      = 0.3

# strFileName     = 'C:/EdsData/EQY_Sample10'
# strFileName     = 'C:/EdsData/EQY_Sample1000'
strFileName     = 'C:/EdsData/EQY_US_ALL_TRADE_20161223'
strKinesis      = 'kinesis'
strPartitionKey = '1'
strRegionName   = 'us-east-1'
strStreamName   = 'SalientCRGTSECDemo'


# Connect to Kinesis and get list of Kinesis streams

kinesis_client = boto3.client(strKinesis, region_name = strRegionName)

# Open the source file and process the trade records
		   
fh = open(strFileName, 'r')

for lstLine in fh:
	lstFields = lstLine.rstrip().split('|')
	if len(lstFields) > 1:
		if nNdx == 0:
			for nNdz, val in enumerate(range(15)):
				lstFieldNames.append(lstFields[nNdz])
		else:
			for nNdy, val in enumerate(range(15)):
				lstBoth[nNdy*2] = lstFieldNames[nNdy]
				lstBoth[(nNdy*2)+1] = lstFields[nNdy]
						
			dctBoth = dict(lstBoth[i:i+2] for i in range(0, len(lstBoth), 2))
			dctBoth['RecNbr'] = nRecNbr
			# print ('dctBoth = %s \n' % (dctBoth))
			strPartitionKey = lstFields[1]
			# print ('strPartitionKey = %s' % strPartitionKey)
			
			dctRecord = {'Data' : json.dumps(dctBoth), 'PartitionKey' : strPartitionKey}  
			
			lstRecords.append(dctRecord)
			if (nRecNbr)%nRecsLimit == 0: 
				# print ('nRecNbr = %i\n lstRecords = %s \n' % (nRecNbr, lstRecords))
				print ('Pushing %i records to Kinesis, total = %i' % (nRecsLimit, nRecNbr))
				dctResponse = kinesis_client.put_records(Records = lstRecords, StreamName = strStreamName)
				if dctResponse['FailedRecordCount'] > 0:
					print ('Number of records not pushed to Kinesis %i' % (dctResponse['FailedRecordCount']))
				else:
					print ('   All records successfully pushed to Kinesis')
				lstRecords = []
				time.sleep(nSleepTime)
				
		nRecNbr += 1
		nNdx += 1

if len(lstRecords) > 0:
	print ('Pushing last batch of records to Kinesis')
	dctResponse = kinesis_client.put_records(Records = lstRecords, StreamName = strStreamName)
	if dctResponse['FailedRecordCount'] > 0:
		print ('Number of records not pushed to Kinesis %i' % (dctResponse['FailedRecordCount']))
	else:
		print ('All records successfully pushed to Kinesis')
	print ('Total records pushed to stream %s: %i\n\n' % (strStreamName, nRecNbr - 1))

fh.close()


