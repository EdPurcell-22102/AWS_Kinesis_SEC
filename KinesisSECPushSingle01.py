##
##
## C:/EdsScripts/File2KinesisSingle01.py
## Read in a flat file, convert each line to JSON format and push to Kinesis one record at a time
##
##

import boto3
import json

dctOutput              = {}
dctOutput2             = {}
dctResponse            = {}
dctResponseListStreams = {}

lstFieldNames   = []
nRecNbr         = 0
nNdx            = 0

strFileName     = 'C:/EdsScripts/EQY_Sample10'
strPartitionKey = '1'
strRegionName   = 'us-east-1'
strStreamName   = 'SalientCRGTSECDemo'


lstBoth = ['0','1','2','3','4','5','6','7','8','9','10',
           '11','12','13','14','15','16','17','18','19',
		   '20','21','22','23','24','25','26','27','28','29']

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
			
			dctOutput = {nRecNbr : dctBoth}
			dctOutput2[nRecNbr] = dctBoth
			
		nRecNbr += 1
		nNdx += 1

		
# print ('dctOutput2 = %s \n' % (dctOutput2))
# print ('json.dumps of dctOutput2 = %s \n' % (json.dumps(dctOutput2)))

# Connect to Kinesis and get list of Kinesis streams

client = boto3.client('kinesis', region_name = strRegionName)

dctResponseListStreams = client.list_streams(Limit = 10)

print ('\nList of Kinesis Streams:  %s \n\n' % (dctResponseListStreams))

for nNdx, nVal in enumerate(range(1, nRecNbr, 1)):
	print ('Pushing record %i  %s  to Kinesis. \n' % (nVal, dctOutput2[nVal]))
	
	dctResponse[nNdx] = client.put_record(
		StreamName = strStreamName, 
		Data = json.dumps(dctOutput2[nVal]), 
		PartitionKey = strPartitionKey)
		
	print ('Response from put record %i command: %s \n' % (nNdx, dctResponse[nNdx]))

fh.close()


