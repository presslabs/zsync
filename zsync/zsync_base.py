from location import Location
from zsync.factory import Factory

class ZSyncBase(object):

  def __init__(self, params):
    self.params = params

  def run(self):
    source_location = Location.parse(self.params.source)
    destination_location = Location.parse(self.params.destination)

    source = Factory(source_location, self.params).get()
    destination = Factory(destination_location, self.params).get()

    source.send(destination)
