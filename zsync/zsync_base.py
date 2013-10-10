from path import Path
from zsync.factory import Factory

class ZSyncBase(object):

  def __init__(self, params):
    self.params = params

  def run(self):
    source_path = Path(self.params.source)
    destination_path = Path(self.params.destination)

    source = Factory(source_path).get()
    destination = Factory(destination_path).get()

    source.pipe(destination)
