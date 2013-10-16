AWS_ACCESS_KEY = "AKIAJ6NBXHHV3P6IIXRA"
AWS_SECRET_KEY = "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"

CHUNK_SIZE = 50 * 1024 * 1024
ONE_MBYTE = 1 * 1024 * 1024 # 1 Megabyte
NUM_PARTS = 10

import sys
import time
import traceback
from cStringIO import StringIO

import boto
from boto.s3.key import Key
from boto.s3.bucket import Bucket

from futures import ThreadPoolExecutor

class Downloader(object):

  def __init__(self, access_key, secret_key):
    self.access_key = access_key
    self.secret_key = secret_key
    self.connection = boto.connect_s3(access_key, secret_key)

  def download(self, fp, bucket, key):
    bucket = self.connection.get_bucket(bucket, validate=False)
    # prefix-lu-calin/magic@550PM
    key = bucket.get_key(key)

    resp = self.connection.make_request("HEAD", bucket=bucket, key=key)
    size = int(resp.getheader("content-length"))

    key.get_contents_to_file(fp)
