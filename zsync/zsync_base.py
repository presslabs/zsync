from location import Location
from zsync.factory import Factory

class ZSyncBase(object):
  """
  The entry point for the zsync command line utility. We start by parsing
  the source and the destionation and turning them into Location objects.

  Then we create objects from the three main classes: Local, Remote, S3
  based on the type of the path. Then we delegate the sending and receiving
  to this types of objects.
  """
  def __init__(self, params):
    self.params = params

  def run(self):
    source_location = Location.parse(self.params.source)
    destination_location = Location.parse(self.params.destination)

    source = Factory(source_location, self.params).get()
    destination = Factory(destination_location, self.params).get()

    source.send(destination)
