from zsync.pipeable import Pipeable

class Remote(Pipeable):

  def __init__(self, path):
    self.path = path

  def pipe(self, to):
    print "Pipe from %s to %s" % (self.path.kind, to.path.kind)
