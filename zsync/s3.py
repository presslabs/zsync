from zsync import Pipeable, Receivable

class S3(Pipeable):

  def __init__(self, path):
    self.path = path

  def pipe(self, to):
    print "Pipe from %s to %s" % (self.path.kind, to.path.kind)

  def receive(self):
    pass