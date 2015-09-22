ONE_MBYTE = 1 * 1024 * 1024 # 1 Megabyte

import sys
import time
import random

import boto
from boto.s3.key import Key
from boto.s3.bucket import Bucket

from concurrent.futures import ThreadPoolExecutor

try:
  from raven import Client
  client = Client('http://6374ae28bffb49ffb354c4ae7e90586b:6edd2b5c47b04afcb8853fe9b0d99976@sentry.presslabs.net/5')
except:
  pass

class Downloader(object):

  def __init__(self, access_key, secret_key, chunk_size, concurrency):
    self.access_key = access_key
    self.secret_key = secret_key

    self.chunk_size = int(chunk_size) * 1024 * 1024
    self.concurrency = int(concurrency)

    self.connection = boto.connect_s3(access_key, secret_key)

  def download_part(self, fp, bucket, key, part):
    min_byte = int(part * self.chunk_size)
    max_byte = int((part+1) * self.chunk_size)

    resp = self.connection.make_request(
      "GET",
      bucket=bucket, key=key,
      headers={
        'Range':"bytes=%d-%d" % (min_byte, max_byte)
      }
    )

    data = resp.read(int(self.chunk_size))

    return data

  def download(self, fp, bucket, key):
    bucket = self.connection.get_bucket(bucket, validate=False)
    key = bucket.get_key(key)

    resp = self.connection.make_request("HEAD", bucket=bucket, key=key)
    size = int(resp.getheader("content-length"))

    results = {}

    if size < (self.concurrency - 1) * self.chunk_size:
      key.get_contents_to_file(fp)
    else:
      chunk_nb = size / self.chunk_size + 1

      with ThreadPoolExecutor(max_workers=self.concurrency) as executor:

        for i in range(self.concurrency):
          results[i] = executor.submit(self.download_part, fp, bucket, key, i)

        for part in range(self.concurrency, int(chunk_nb) + self.concurrency):
          data = results[int(part - self.concurrency)].result()
          fp.write(data)

          del results[int(part - self.concurrency)]

          if part < chunk_nb:
            results[part] = executor.submit(self.download_part, fp, bucket, key, part)
