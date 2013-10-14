from subprocess import PIPE, STDOUT, Popen

class LocalSnapshot(object):

  def __init__(self, context):
    self.context = context

  def get_snapshots(self):
    command = Popen("zfs list -t snap", shell=True, stdout=PIPE, stderr=STDOUT)

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

class RemoteSnapshot(object):

  def __init__(self, context):
    self.context = context

  def get_snapshots(self):
    e = "ssh"
    if self.context.args.e:
      e = self.context.args.e

    command = Popen(e + " " + self.context.data.host + " zfs list -t snap", shell=True, stdout=PIPE, stderr=STDOUT)

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
    elif remote == True:
      self.strategy = RemoteSnapshot(context)

  @classmethod
  def from_context(cls, context):
    # HACK Done this so we keep away from circular imports

    import zsync.local
    import zsync.remote

    if isinstance(context, zsync.local.Local):
      return Snapshot(local=True, context=context)
    if isinstance(context, zsync.remote.Remote):
      return Snapshot(remote=True, context=context)

  def get_snapshots(self):
    return self.strategy.get_snapshots()

  def get_latest_snapshot(self, volume):
    return self.strategy.get_latest_snapshot(volume)
