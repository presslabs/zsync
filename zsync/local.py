from subprocess import PIPE, Popen

from snapshot import Snapshot
from zsync import Pipeable, Receivable

"""
Local -> local:
zfs send send_volume_name | zfs receive -F dest_volume_name
zfs send -I send_volume_name@snap1 send_volume_name@span2 | zfs receive -F dest_volume_name

USAGE:
zfs tank/instances@now tank/new-instances
This means sync from the @now snapshot

zfs tank/instances tank/new-instances
This means sync from the latest snapshot of tank/instances


get latest snapshot
search snapshot that makes another snapshot
"""

class Local(Pipeable, Receivable):

  def __init__(self, data):
    self.data = data

  def send(self, destination):
    """
    local send to local receive:
    If full is present than there's no need for incremental send.
    Otherwise you need to find a snapshot name in the source dataset
    that matches the latest snapshot of the destination
    """
    local_snapshot_manager = Snapshot(local=True, context=self)

    snapshot = self.data.snapshot

    if snapshot == None:
      snapshot = local_snapshot_manager.get_latest_snapshot(self.data.dataset)

    if self.args.full:
      data = Popen([
        "zfs",
        "send",
        self.data.dataset + "@" + snapshot
      ], stdout=PIPE)
    else:
      destination_strategy = Snapshot.from_context(destination)
      latest_snapshot = destination_strategy.get_latest_snapshot(destination.data.dataset)

      data = Popen([
        "zfs",
        "send",
        "-I",
        self.data.dataset + "@" + latest_snapshot,
        self.data.dataset + "@" + snapshot
      ], stdout=PIPE)

    destination.receive(data)

  def receive(self, source):
    receive_endpoint = Popen(["zfs", "receive", "-F", self.data.dataset], stdin=source.stdout, stdout=PIPE)
    source.stdout.close()
    output = receive_endpoint.communicate()[0]
