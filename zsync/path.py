from urlparse import urlparse

from zsync import Schema

class Path(object):

  def __init__(self, uri):
    parsed_url = urlparse(uri)

    self.scheme = parsed_url.scheme
    self.host = parsed_url.netloc

    if self.host.endswith(":"):
      self.host = self.host[:-1]

    self.path = parsed_url.path

    if self.scheme not in Schema.SUPPORTED:
      raise AttributeError("You need to suply a location that is either zfs or s3 or local")

    self.bucket = self.host

    if self.scheme == "" and self.host == "" and self.path != "":
      self.local = True

      self.kind = Schema.LOCAL
    else:
      self.local = False

    if self.path.startswith("/"):
      self.path = self.path[1:]


    if self.scheme == "zfs":
      self.remote = True

      self.kind = Schema.REMOTE
    else:
      self.remote = False

    if self.scheme == "s3":
      self.kind = Schema.S3
      self.s3 = True

      if self.host == "":
        raise AttributeError("When providing S3 locations you need to supply a bucket")

      self.path = self.path.split("/")
    else:

      self.s3 = False

      if self.path.find("@") != -1:
        split_path = self.path.split("@")
        if len(split_path) != 2:
          raise AttributeError("You need to specify only one snapshot")

        self.dataset, self.snapshot = split_path
