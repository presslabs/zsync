from zsync import Pipeable, Receivable, S3

class Remote(Pipeable, Receivable):

  def __init__(self, data):
    self.data = data

  def pipe(self, to):
    if isinstance(to, S3) or isinstance(to, Remote):
      raise NotImplementedError("Remote to Remote or Remote to S3 is not implemented")

  def receive(self):
    pass
