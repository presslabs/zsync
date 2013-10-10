import zsync

class Factory(object):

  def __init__(self, path):
    self.path = path

  def get(self):
    klass_name = self.path.kind
    return klass_name(self.path)
