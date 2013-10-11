from zsync import Pipeable, Receivable

class S3(Pipeable):

  def __init__(self, data):
    self.data = data

  def pipe(self, to):
    # hack in order to check this
    from zsync import Remote
    if isinstance(to, S3) or isinstance(to, Remote):
      raise NotImplementedError("Remote to Remote or Remote to S3 is not implemented")

  def receive(self):
    pass
