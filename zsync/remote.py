from zsync import Pipeable, Receivable

class Remote(Pipeable, Receivable):

  def __init__(self, path):
    self.path = path

  def pipe(self, to):
    if to.path.kind == "S3" or to.path.kind == "Remote":
      raise NotImplementedError("Remote to Remote or Remote to S3 is not implemented")

    print "Pipe from %s to %s" % (self.path.kind, to.path.kind)

  def receive(self):
    pass
