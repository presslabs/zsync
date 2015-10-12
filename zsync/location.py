import zsync

from zsync import Local, Remote, S3
from urlparse import urlparse

SUPPORTED_SCHEMA = ["zfs", "s3", ""]

class Location(object):
  """
  Handles generic parsing and creation of specific Location objects
  """
  def __init__(self, parsed_url):
    """
    Constructor that receives parsed URI and sets only the path for
    any object
    """
    self.parsed_url = parsed_url

    self._path = parsed_url.path

  @classmethod
  def parse(cls, uri):
    """
    Factory method that based on a URI creates specific objects:
    RemoteLocation, LocalLocation and S3Location
    """
    parsed_url = urlparse(uri)

    if parsed_url.scheme not in SUPPORTED_SCHEMA:
      raise AttributeError("You need to suply a location that is either zfs or s3 or local")

    scheme = parsed_url.scheme

    if scheme == "":
      scheme = "Local"
    elif scheme == "zfs":
      scheme = "Remote"
    else:
      scheme = "S3"

    klass_name = scheme.capitalize() + "Location"
    klass = getattr(zsync.location, klass_name)
    return klass(parsed_url)

  def set_host(self):
    self.host = self.parsed_url.netloc

    if self.host.endswith(":"):
      self.host = self.host[:-1]

  def set_dataset_and_snapshot(self):
    if self._path.find("@") != -1:
      split_path = self._path.split("@")

      if len(split_path) != 2:
        raise AttributeError("You need to specify only one snapshot")

      self.dataset, self.snapshot = split_path
    else:
      self.dataset = self._path
      self.snapshot = None

    if self.dataset.startswith("/"):
      self.dataset = self.dataset[1:]

class S3Location(Location):

  def __init__(self, parsed_url):
    super(S3Location, self).__init__(parsed_url)
    self.kind = S3

    self.set_host()
    self.bucket = self.host

    self.path = self._path.split("/")
    if len(path) > 1 and not self.path[-1]:
        self.dataset = self.path[-2]
    else:
        self.dataset = self.path[-1] 
    self.path = self.path[:-1]

    if self.dataset.find("@") != -1:
      data = self.dataset.split("@")

      self.dataset = data[0]
      self.snapshot = data[1]
    else:
      self.snapshot = None

    if self.host == "":
      raise AttributeError("When providing S3 locations you need to supply a bucket")

  def __repr__(self):
    return "<%s bucket=\"%s\" dataset=\"%s\" snapshot=\"%s\" prefix=\"%s\">" % (self.__class__.__name__, self.bucket, self.dataset, self.snapshot, "/".join(self.path))

class RemoteLocation(Location):

  def __init__(self, parsed_url):
    super(RemoteLocation, self).__init__(parsed_url)
    self.kind = Remote

    self.set_host()
    self.set_dataset_and_snapshot()

  def __repr__(self):
    return "<%s host=\"%s\" dataset=\"%s\" snapshot=\"%s\">" % (self.__class__.__name__, self.host, self.dataset, self.snapshot)

class LocalLocation(Location):

  def __init__(self, parsed_url):
    super(LocalLocation, self).__init__(parsed_url)
    self.kind = Local

    self.set_dataset_and_snapshot()

  def __repr__(self):
    return "<%s dataset=\"%s\" snapshot=\"%s\">" % (self.__class__.__name__, self.dataset, self.snapshot)
