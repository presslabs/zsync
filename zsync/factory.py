import zsync

class Factory(object):
  """
  Factory returns based on the location object instances of Local, Remote
  or S3.
  """

  def __init__(self, location, args, log):
    self.location = location
    self.args = args
    self.log = log

  def get(self):
    """
    Creates the instance of the specific object
    """
    klass_name = self.location.kind
    obj = klass_name(self.location, self.log)
    obj.args = self.args

    return obj
