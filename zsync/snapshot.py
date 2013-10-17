import boto

from subprocess import PIPE, STDOUT, Popen

AWS_ACCESS_KEY = "AKIAJW3VYJYPG3MZ3PWQ"
AWS_SECRET_KEY = "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"

class S3Snapshot(object):

  def __init__(self, context):
    self.context = context

  def get_snapshots(self, volume):
    bucket = self.context.data.bucket
    path = "/".join(self.context.data.path)
    path = path[1:]

    connection = boto.connect_s3(AWS_ACCESS_KEY, AWS_SECRET_KEY)
    bucket = connection.get_bucket(bucket, validate=False)

    snapshots = []

    for ks in bucket.list(path):
      name = ks.name
      name = name[len(path) + len(self.context.data.dataset) + 1 - len(volume):]

      if name.startswith(volume):
        snapshots.append({
          "name" : name,
          "last_modified" : ks.last_modified
        })

    snapshots = sorted(snapshots, key=lambda x: x["last_modified"])

    result = []

    for snap in snapshots:
      name = snap["name"]
      result.append(name.split("@")[1])

    return result

  def get_latest_snapshot(self, volume):
    snapshots = self.get_snapshots(volume)

    if len(snapshots) == 0:
      return None

    return snapshots[-1]

  def get_snapshots_between(self, volume, snapshot_start, snapshot_end):
    snapshots = self.get_snapshots(volume)

    adding = False

    result = []

    if snapshot_start == None:
      adding = True

    for snapshot in snapshots:
      if snapshot_start == snapshot:
        adding = True

      if adding == True:
        result.append(snapshot)

      if snapshot_end == snapshot:
        adding = False

    return result

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

    if snapshot_start == None:
      adding = True

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
      return None

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

    adding = False

    result = []

    if snapshot_start == None:
      adding = True

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
      return None

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
