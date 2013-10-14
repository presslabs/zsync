from zsync import Pipeable, Receivable

class S3(Pipeable):

  def __init__(self, data):
    self.data = data

  def send(self, to):
    # HACK Circular dependency hack
    from zsync import Remote
    if isinstance(to, S3) or isinstance(to, Remote):
      raise NotImplementedError("Remote to Remote or Remote to S3 is not implemented")

  def receive(self, source):
    print source.stdout

    while True:
      buf = source.stdout.read()

      print len(buf)

      if len(buf) == 0:
        break
