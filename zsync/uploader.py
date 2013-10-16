AWS_ACCESS_KEY = "AKIAJW3VYJYPG3MZ3PWQ"
AWS_SECRET_KEY = "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"

CHUNK_SIZE = 50 * 1024 * 1024

import sys
import time
import traceback
from cStringIO import StringIO

import boto
from boto.s3.key import Key
from boto.s3.bucket import Bucket

from futures import ThreadPoolExecutor

class Uploader(object):

  def __init__(self, access_key, secret_key):
    self.access_key = access_key
    self.secret_key = secret_key

    self.connection = boto.connect_s3(access_key, secret_key)

  def upload_part(self, multipart, chunk, index):
    bucket = self.connection.get_bucket(multipart.bucket_name, validate=False)
    retries = 0

    def do_upload(bucket, chunk, index):
      part = boto.s3.multipart.MultiPartUpload(bucket)
      part.id = multipart.id
      part.key_name = multipart.key_name
      part.upload_part_from_file(StringIO(chunk), index+1, replace=True)

    while retries < 4:
      try:
        print "Started chunk %s" % index
        do_upload(bucket, chunk, index)
        print "Uploaded chunk %s" % index
        break
      except Exception as e:
        traceback.print_exc()

        print "Retrying part %s" % index
        retries += 1
        time.sleep(2**retries)

  def upload(self, stream, bucket, key):
    bucket = self.connection.get_bucket(bucket, validate=False)

    multipart = bucket.initiate_multipart_upload(key, headers={ "x-amz-acl" : "bucket-owner-full-control" })

    index = 0

    greenlets = []

    try:
      with ThreadPoolExecutor(max_workers=100) as executor:
        while True:
          buff = stream.read(CHUNK_SIZE)

          if len(buff) == 0:
            break

          future = executor.submit(self.upload_part, multipart, buff, index)
          greenlets.append(future)

          index += 1
    except KeyboardInterrupt:
      # sys.stdout.flush()
      pass

    [ future.result() for future in greenlets ]
    completed = multipart.complete_upload()

