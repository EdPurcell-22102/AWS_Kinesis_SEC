##
##
## Connect to Kinesis, delete stream "SalientCRGTSECDemo" in region US-East-1
##
## C:/EdsScripts/KinesisSECDeleteStream.py
##
## 2017-05-24 - Modified wording of "waiting 30 seconds" message.
##
##

import boto3
import time

client = boto3.client('kinesis', region_name='us-east-1')

dctResponse1 = client.list_streams(Limit = 10)

print ('List of Kinesis Streams before deletion:  ', dctResponse1)

dctResponse2 = client.delete_stream(StreamName = 'SalientCRGTSECDemo')

print ('Response from delete command:  %s\n\nWaiting 30 seconds to check whether stream was deleted...' % (dctResponse2))

time.sleep(30)

dctResponse3 = client.list_streams(Limit = 10)

print ('List of Streams after deletion:  ', dctResponse3)