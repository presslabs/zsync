import fnmatch

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

    # validate the until snapshot
    if self.validate(until_snapshot) == False:
      self.log.info("Ignoring snapshot %s@%s" % (self.data.dataset, until_snapshot))
      return

    """
    send to each implementation the task to send the ZFS volume with the last
    snapshot beeing _until_snapshot_
    """
    self._send_full_snahpshot(self.data.dataset, destination, until_snapshot)

  def validate(self, snapshot):
    if self.args.exclude != None:
      return not fnmatch.fnmatch(snapshot, self.args.exclude)
    elif self.args.include != None:
      return fnmatch.fnmatch(snapshot, self.args.include)

    return True

  def send(self, destination):
    """
    Send either full or incremental snapshot
    """
    local_snapshot_manager = Snapshot.from_context(self)

    # if no snapshot provided get the last one
    snapshot = self.data.snapshot
    if snapshot == None:
      snapshot = local_snapshot_manager.get_latest_snapshot(self.data.dataset)

    # if the full flag is provided than send full snapshot
    if self.args.full:
      self._send_full_volume(snapshot, destination)
    else:
      self._send_incremental_volume(snapshot, destination)

  def filter_to_valid(self, all_snapshots):
    results = []

    for snapshot in all_snapshots:
      if self.validate(snapshot):
        results.append(snapshot)

    return results

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

    all_snapshots = self.filter_to_valid(all_snapshots)

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
