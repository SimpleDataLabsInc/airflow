import boto3
import sys

s3Bucket = sys.argv[1]
s3Key = sys.argv[2]
outfile = sys.argv[3]

s3 = boto3.resource('s3', aws_access_key_id = 'AKIAITROTGC7VVQJOV7Q', aws_secret_access_key ='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')
s3.meta.client.download_file(s3Bucket, s3Key, outfile)

