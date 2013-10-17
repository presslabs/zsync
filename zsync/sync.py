from zsync import Pipeable, Receivable

from zsync.snapshot import Snapshot

class Sync(Pipeable, Receivable):
  """
  Common logic for all the three sources implementation: S3, Local, Remote
  Provides means of sending a snapshot either as a full or incremental
  version
  """

  def __init__(self, data, log):
    self.data = data
    self.log = log

  def _send_full_volume(self, until_snapshot, destination):
    local_snapshot_manager = Snapshot(local=True, context=self)
    all_snapshots = local_snapshot_manager.get_snapshots(self.data.dataset)

    self.log.info("Sending FULL Volume from %s dataset=%s and snapshot=%s", self.__class__.__name__, self.data.dataset, until_snapshot)

    self._send_full_snahpshot(self.data.dataset, destination, until_snapshot)

  def send(self, destination):
    """
    Send either full or incremental snapshot
    """

    local_snapshot_manager = Snapshot(local=True, context=self)

    # if no snapshot provided get the last one
    snapshot = self.data.snapshot
    if snapshot == None:
      snapshot = local_snapshot_manager.get_latest_snapshot(self.data.dataset)

    if self.args.full:
      self._send_full_volume(snapshot, destination)
    else:
      self._send_incremental_volume(snapshot, destination)

  def _send_incremental_volume(self, until_snapshot, destination):
    """
    Sends an incremental snapshot to the destination
    """

    local_snapshot_manager = Snapshot.from_context(self)
    destination_strategy = Snapshot.from_context(destination)

    # get destination dataset
    destination_dataset = destination.data.dataset

    # get latest snapshot from the destination
    latest_snapshot = destination_strategy.get_latest_snapshot(destination_dataset)
    # get local snapshots between, destination latest snapshot and the given snapshot
    all_snapshots = local_snapshot_manager.get_snapshots_between(self.data.dataset, latest_snapshot, until_snapshot)

    self.log.info("Sending incremental volume from %s dataset=%s and snapshot=%s", self.__class__.__name__, self.data.dataset, all_snapshots)

    current_snapshot = all_snapshots[0]
    all_snapshots = all_snapshots[1:]

    if latest_snapshot == None:
      # if there are no snapshots means we need to create the volume
      self._send_full_snahpshot(self.data.dataset, destination, current_snapshot)

    for remaining in all_snapshots:
      self._send_incremental_snapshot(self.data.dataset, destination,
                                      current_snapshot, remaining)
      current_snapshot = remaining
