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

  def _send_full_snahpshot(self, dataset, destination, first_snapshot):
    data = Popen([
      "zfs",
      "send",
      "%s@%s" % (dataset, first_snapshot)
    ], stdout=PIPE)
    destination.receive(data)

  def _send_incremental_snapshot(self, dataset, destination, first_snapshot,
                                 second_snapshot):
    data = Popen([
      "zfs",
      "send",
      "-I",
      "%s@%s" % (dataset, first_snapshot),
      "%s@%s" % (dataset, second_snapshot)
    ], stdout=PIPE)
    destination.receive(data)

  def _send_full_volume(self, until_snapshot, destination):
    # get all snapshots
    local_snapshot_manager = Snapshot(local=True, context=self)
    all_snapshots = local_snapshot_manager.get_snapshots(self.data.dataset)

    first_snapshot = all_snapshots[0]
    all_snapshots = all_snapshots[1:]

    self._send_full_snahpshot(self.data.dataset, destination, first_snapshot)

    if first_snapshot == until_snapshot:
      return

    current_snapshot = first_snapshot

    for remaining in all_snapshots:
      self._send_incremental_snapshot(self.data.dataset, destination,
                                      current_snapshot, remaining)
      current_snapshot = remaining

      if remaining == until_snapshot:
        return

  def _send_incremental_volume(self, until_snapshot, destination):
    local_snapshot_manager = Snapshot(local=True, context=self)
    destination_strategy = Snapshot.from_context(destination)

    destination_dataset = destination.data.dataset if hasattr(destination.data, "dataset") else destination.data.bucket
    latest_snapshot = destination_strategy.get_latest_snapshot(destination_dataset)
    all_snapshots = local_snapshot_manager.get_snapshots_between(self.data.dataset, latest_snapshot, until_snapshot)

    current_snapshot = latest_snapshot
    all_snapshots = all_snapshots[1:]

    for remaining in all_snapshots:
      self._send_incremental_snapshot(self.data.dataset, destination,
                                      current_snapshot, remaining)
      current_snapshot = remaining

  def send(self, destination):
    local_snapshot_manager = Snapshot(local=True, context=self)
    # if no snapshot provided get the last one
    snapshot = self.data.snapshot

    if snapshot == None:
      snapshot = local_snapshot_manager.get_latest_snapshot(self.data.dataset)

    if self.args.full:
      self._send_full_volume(snapshot, destination)
    else:
      self._send_incremental_volume(snapshot, destination)

  def receive(self, source):
    receive_endpoint = Popen(["zfs", "receive", "-F", self.data.dataset], stdin=source.stdout, stdout=PIPE)
    source.stdout.close()
    output = receive_endpoint.communicate()[0]
