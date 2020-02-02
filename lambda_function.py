import json
import os
import boto3
from urllib.parse import urlparse
from urllib.request import urlopen

BUCKET_NAME = os.environ['S3_BUCKET_NAME']

s3 = boto3.client('s3')

# Utility function to generate S3 key from URL
buildKey = lambda _url: urlparse(_url).path[1:]

# main run function
def run(_inputFiles):
  response = {'files': []}
  for _inputFile in _inputFiles:
    print('processing ', _inputFile['url'])
    _outputFile = {'url': _inputFile['url']}
    fileDownloadResponse = urlopen(_inputFile['url'])
    print('file response headers:', fileDownloadResponse.headers)
    s3Response = s3.put_object(
      Body = fileDownloadResponse.read(),
      Bucket = BUCKET_NAME,
      Key = buildKey(_inputFile['url']),
      ContentType = fileDownloadResponse.headers['Content-Type'])
    _outputFile['s3Object'] = s3Response
    _outputFile['Content-Length'] = fileDownloadResponse.headers['Content-Length']
    print('done processing ', _inputFile['url'])
    response['files'].append(_outputFile)
  return response

# AWS lambda handler
def lambda_handler(event, context):
  print('S3 bucket name: ', BUCKET_NAME)
  print('type(event): ', type(event))
  print('event: ', event)

  return run(event['files'])
