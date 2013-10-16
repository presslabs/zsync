from subprocess import PIPE, Popen

from snapshot import Snapshot
from zsync import Pipeable, Receivable

class Remote(Pipeable, Receivable):

  def __init__(self, data):
    self.data = data

  def _send_full_snahpshot(self, dataset, destination, first_snapshot):
    ssh_cmd = "%s %s zfs send %s@%s" % (self.args.e, self.data.host, dataset, first_snapshot)
    data = Popen(ssh_cmd, shell=True, stdout=PIPE)
    destination.receive(data, dataset, first_snapshot)

  def _send_incremental_snapshot(self, dataset, destination, first_snapshot,
                                 second_snapshot):
    ssh_cmd = "%s %s zfs send -I %s@%s %s@%s" % (self.args.e, self.data.host, \
                                                dataset, first_snapshot, \
                                                dataset, second_snapshot )
    data = Popen(ssh_cmd, shell=True, stdout=PIPE)
    destination.receive(data, dataset, second_snapshot)

  def _send_full_volume(self, until_snapshot, destination):
    local_snapshot_manager = Snapshot.from_context(self)
    all_snapshots = local_snapshot_manager.get_snapshots(self.data.dataset)
    self._send_full_snahpshot(self.data.dataset, destination, until_snapshot)

  def _send_incremental_volume(self, until_snapshot, destination):
    local_snapshot_manager = Snapshot.from_context(self)
    destination_strategy = Snapshot.from_context(destination)

    destination_dataset = destination.data.dataset if hasattr(destination.data, "dataset") else destination.data.bucket

    # get latest snapshot from the destination
    latest_snapshot = destination_strategy.get_latest_snapshot(destination_dataset)
    # get local snapshots between, destination latest snapshot and the given snapshot
    all_snapshots = local_snapshot_manager.get_snapshots_between(self.data.dataset, latest_snapshot, until_snapshot)

    current_snapshot = all_snapshots[0]
    all_snapshots = all_snapshots[1:]

    if latest_snapshot == None:
      # if there are no snapshots means we need to create the volume
      self._send_full_snahpshot(self.data.dataset, destination, current_snapshot)

    for remaining in all_snapshots:
      self._send_incremental_snapshot(self.data.dataset, destination,
                                      current_snapshot, remaining)
      current_snapshot = remaining

  def send(self, destination):
    local_snapshot_manager = Snapshot.from_context(self)
    # if no snapshot provided get the last one
    snapshot = self.data.snapshot

    if snapshot == None:
      snapshot = local_snapshot_manager.get_latest_snapshot(self.data.dataset)

    if self.args.full:
      self._send_full_volume(snapshot, destination)
    else:
      self._send_incremental_volume(snapshot, destination)

  def receive(self, source, dataset, snapshot_name):
    ssh_cmd = self.args.e + " " + self.data.host + " zfs receive -F %s" % self.data.dataset

    receive_endpoint = Popen(ssh_cmd, stdin=source.stdout, stdout=PIPE, shell=True)
    source.stdout.close()
    output = receive_endpoint.communicate()[0]
