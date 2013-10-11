from subprocess import PIPE, STDOUT, Popen

import zsync.local

class LocalSnapshot(object):

  def __init__(self, context):
    self.context = context

  def get_snapshots(self):
    command = Popen("zfs list", shell=True, stdout=PIPE, stderr=STDOUT)

    output = []
    for line in command.stdout.readlines():
      line = line.split(" ")

      if line[0] != "NAME":
        output.append(line[0])

    retval = command.wait()
    return output


  def get_latest_snapshot(self, volume):
    snapshots = self.get_snapshots()

    matches = []

    for snapshot in snapshots:
      if snapshot.startswith(volume + "@"):
        without_dataset = snapshot.split("@")[1]
        matches.append(without_dataset)

    if len(matches) == 0:
      raise AttributeError("%s doesn't have any snapshots yet" % volume)

    return matches[-1]

class Snapshot(object):

  def __init__(self, local=False, remote=False, s3=False, context=None):

    if local == True:
      self.strategy = LocalSnapshot(context)

  @classmethod
  def from_context(cls, context):
    if isinstance(context, zsync.local.Local):
      return Snapshot(local=True, context=context)


  def get_snapshots(self):
    return self.strategy.get_snapshots()

  def get_latest_snapshot(self, volume):
    return self.strategy.get_latest_snapshot(volume)
