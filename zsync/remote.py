from subprocess import PIPE, Popen

from snapshot import Snapshot
from zsync import Pipeable, Receivable, S3

class Remote(Pipeable, Receivable):

  def __init__(self, data):
    super(Remote, self).__init__()
    self.data = data

  def send(self, destination):
    if isinstance(destination, S3) or isinstance(destination, Remote):
      raise NotImplementedError("Remote to Remote or Remote to S3 is not implemented")

    local_snapshot_manager = Snapshot.from_context(self)

    snapshot = self.data.snapshot

    if snapshot == None:
      snapshot = local_snapshot_manager.get_latest_snapshot(self.data.dataset)

    if self.args.full:
      ssh_cmd = "%s %s zfs send %s@%s" % (self.args.e, self.data.host, self.data.dataset, snapshot)
      data = Popen(ssh_cmd, shell=True, stdout=PIPE)
    else:
      destination_strategy = Snapshot.from_context(destination)
      latest_snapshot = destination_strategy.get_latest_snapshot(destination.data.dataset)

      ssh_cmd = "%s %s zfs send -I %s@%s %s@%s" % (self.args.e, self.data.host, self.data.dataset, latest_snapshot, self.data.dataset, snapshot)
      data = Popen(ssh_cmd, shell=True, stdout=PIPE)

    destination.receive(data)

  def receive(self, source):
    ssh_cmd = self.args.e + " " + self.data.host + " zfs receive -F %s" % self.data.dataset

    receive_endpoint = Popen(ssh_cmd, stdin=source.stdout, stdout=PIPE, shell=True)
    source.stdout.close()
    output = receive_endpoint.communicate()[0]
