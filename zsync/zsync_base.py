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
  def __init__(self, params, log):
    self.params = params
    self.log = log

  def run(self):
    self.log.debug("Zsync started")

    if self.params.exclude != None and self.params.include != None:
      raise AttributeError("You can use either exclude or include")

    source_location = Location.parse(self.params.source)
    destination_location = Location.parse(self.params.destination)

    self.log.info("Source is set to %s", source_location)
    self.log.info("Destination is set to %s", destination_location)

    source = Factory(source_location, self.params, self.log).get()
    destination = Factory(destination_location, self.params, self.log).get()

    source.send(destination)
