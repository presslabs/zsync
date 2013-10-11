import zsync

class Factory(object):

  def __init__(self, location, args):
    self.location = location
    self.args = args

  def get(self):
    klass_name = self.location.kind
    obj = klass_name(self.location)
    obj.args = self.args

    return obj
