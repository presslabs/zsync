import sys
import time
import traceback
from cStringIO import StringIO

import boto
from boto.s3.key import Key
from boto.s3.bucket import Bucket

from concurrent.futures import ThreadPoolExecutor
from threading import Semaphore

try:
  from raven import Client
  sentry_dsn = os.environ['ZSYNC_SENTRY_DSN']
  client = Client(sentry_dsn)
except:
  client = None


class ChunkNotCompleted(Exception):
  pass


class Uploader(object):
  """
  Uploads a stream to S3 with multipart upload
  """

  def __init__(self, access_key, secret_key, chunk_size, concurrency):
    self.access_key = access_key
    self.secret_key = secret_key

    self.chunk_size = int(chunk_size) * 1024 * 1024
    self.concurrency = int(concurrency)

  def _get_s3_bucket(self, bucket_name):
    try:
      connection = boto.connect_s3(self.access_key, self.secret_key)
      bucket = connection.get_bucket(bucket_name, validate=True)
    except:
      if client:
        client.captureException()

    return bucket

  def upload_part(self, multipart, chunk, index):
    bucket = self._get_s3_bucket(multipart.bucket_name)

    retries = 0

    def do_upload(bucket, chunk, index):
      try:
        part = boto.s3.multipart.MultiPartUpload(bucket)
        part.id = multipart.id
        part.key_name = multipart.key_name
        part.upload_part_from_file(StringIO(chunk), index+1, replace=True)
      except:
        if client:
          client.captureException()

    while retries < 5:
      try:
        do_upload(bucket, chunk, index)
        break
      except:

        if client:
          client.captureException()

        retries += 1
        time.sleep(2**retries)

    if retries == 5:
      raise ChunkNotCompleted("Chunk %s not completed" % index)

  def upload(self, stream, bucket, key, storage_class):
    bucket = self._get_s3_bucket(bucket)

    try:
      multipart = bucket.initiate_multipart_upload(
        key,
        headers={
          "x-amz-acl": "bucket-owner-full-control",
          "x-amz-storage-class": storage_class
        },
        metadata={'awesome-key': 'test-value'}
      )
    except:
      if client:
        client.captureException()

    index = 0

    greenlets = []

    sem = Semaphore(self.concurrency)
    def release(future):
      sem.release()

    try:
      with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
        while True:
          sem.acquire()
          buff = stream.read(self.chunk_size)

          if len(buff) == 0:
            break

          future = executor.submit(self.upload_part, multipart, buff, index)
          future.add_done_callback(release)
          greenlets.append(future)

          index += 1
    except KeyboardInterrupt:
      # sys.stdout.flush()
      pass

    [ future.result() for future in greenlets ]
    completed = multipart.complete_upload()
