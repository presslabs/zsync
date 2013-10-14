from subprocess import PIPE, STDOUT, Popen

class S3Snapshot(object):

  def __init__(self, context):
    self.context = context

  def get_snapshots(self):
    pass

  def get_latest_snapshot(self, volume):
    return "0010PM"

class LocalSnapshot(object):

  def __init__(self, context):
    self.context = context

  def get_snapshots(self, volume):
    command = Popen("zfs list -t snap", shell=True, stdout=PIPE, stderr=STDOUT)

    output = []
    for line in command.stdout.readlines():
      line = line.split(" ")

      if line[0] != "NAME":
        snap_name = line[0]

        if snap_name.startswith(volume + "@"):
          without_dataset = snap_name.split("@")[1]
          output.append(without_dataset)

    retval = command.wait()
    return output

  def get_snapshots_between(self, volume, snapshot_start, snapshot_end):
    snapshots = self.get_snapshots(volume)

    adding = False

    result = []

    for snapshot in snapshots:
      if snapshot_start == snapshot:
        adding = True

      if adding == True:
        result.append(snapshot)

      if snapshot_end == snapshot:
        adding = False

    return result

  def get_latest_snapshot(self, volume):
    snapshots = self.get_snapshots(volume)

    if len(snapshots) == 0:
      raise AttributeError("%s doesn't have any snapshots yet" % volume)

    return snapshots[-1]

class RemoteSnapshot(object):

  def __init__(self, context):
    self.context = context

  def get_snapshots(self, volume):
    e = "ssh"
    if self.context.args.e:
      e = self.context.args.e

    command = Popen(e + " " + self.context.data.host + " zfs list -t snap", shell=True, stdout=PIPE, stderr=STDOUT)

    output = []

    for line in command.stdout.readlines():
      line = line.split(" ")

      if line[0] != "NAME":
        snap_name = line[0]

        if snap_name.startswith(volume + "@"):
          without_dataset = snap_name.split("@")[1]
          output.append(without_dataset)

    retval = command.wait()

    return output

  def get_snapshots_between(self, volume, snapshot_start, snapshot_end):
    snapshots = self.get_snapshots(volume)

    print snapshots

    adding = False

    result = []

    for snapshot in snapshots:
      if snapshot_start == snapshot:
        adding = True

      if adding == True:
        result.append(snapshot)

      if snapshot_end == snapshot:
        adding = False

    return result

  def get_latest_snapshot(self, volume):
    snapshots = self.get_snapshots(volume)

    if len(snapshots) == 0:
      raise AttributeError("%s doesn't have any snapshots yet" % volume)

    return snapshots[-1]

class Snapshot(object):

  def __init__(self, local=False, remote=False, s3=False, context=None):

    if local == True:
      self.strategy = LocalSnapshot(context)
    elif remote == True:
      self.strategy = RemoteSnapshot(context)
    elif s3 == True:
      self.strategy = S3Snapshot(context)

  @classmethod
  def from_context(cls, context):
    # HACK Done this so we keep away from circular imports

    import zsync.local
    import zsync.remote

    if isinstance(context, zsync.local.Local):
      return Snapshot(local=True, context=context)
    if isinstance(context, zsync.remote.Remote):
      return Snapshot(remote=True, context=context)
    if isinstance(context, zsync.s3.S3):
      return Snapshot(s3=True, context=context)


  def get_snapshots(self, volume):
    return self.strategy.get_snapshots(volume)

  def get_latest_snapshot(self, volume):
    return self.strategy.get_latest_snapshot(volume)

  def get_snapshots_between(self, volume, snapshot_start, snapshot_end):
    return self.strategy.get_snapshots_between(volume, snapshot_start, snapshot_end)
