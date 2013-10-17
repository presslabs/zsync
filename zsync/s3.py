import os
from cStringIO import StringIO
from collections import namedtuple

from zsync import Sync, Pipeable, Receivable

from zsync.uploader import Uploader
from zsync.downloader import Downloader
from zsync.snapshot import Snapshot

from multiprocessing import Process, Pipe

AWS_ACCESS_KEY = "AKIAJ6NBXHHV3P6IIXRA"
AWS_SECRET_KEY = "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"

class S3(Sync):
  """
  Sends and receives ZFS snapshots from S3
  """

  def __init__(self, data, log):
    super(S3, self).__init__(data, log)

  def downloader(self, bucket, key, fp):
    """
    Downloads to a stream
    """
    fp = os.fdopen(fp, 'w')
    Downloader(AWS_ACCESS_KEY, AWS_SECRET_KEY).download(fp, bucket, key)

  def _send_full_snahpshot(self, dataset, destination, first_snapshot):
    """
    Retrives a snapshot and passes it a pipe from which the zfs receive reads
    """
    key = "/".join(self.data.path)
    key = key[1:]
    key += "/" + self.data.dataset + "@" + first_snapshot

    self.log.info("Running send from bucket=%s, key=%s", self.data.bucket, key)

    pipeout, pipein = os.pipe()
    downloader_process = Process(target=self.downloader, args=(self.data.bucket, key, pipein))

    pipeout = os.fdopen(pipeout)
    downloader_process.start()

    FakePopen = namedtuple('FakePopen', ['stdout'])
    data = FakePopen(stdout=pipeout)
    destination.receive(data, dataset, first_snapshot)

    downloader_process.join()

  def _send_incremental_snapshot(self, dataset, destination, first_snapshot,
                                 second_snapshot):
    """
    There are no incremental snapshots from S3 so it fails back to the
    full snapshot.
    """
    self._send_full_snahpshot(dataset, destination, second_snapshot)

  def _send_full_volume(self, until_snapshot, destination):
    """
    Sends a full volume snapshot from S3 to a different destination.
    Captures all snapshots that need to be sent then it removes all of them
    but not the last one - mimics the effect of a proper zfs send
    """

    local_snapshot_manager = Snapshot.from_context(self)
    destination_strategy = Snapshot.from_context(destination)

    destination_dataset = destination.data.dataset

    # get latest snapshot from the destination
    latest_snapshot = destination_strategy.get_latest_snapshot(destination_dataset)
    # get local snapshots between, destination latest snapshot and the given snapshot
    all_snapshots = local_snapshot_manager.get_snapshots_between(self.data.dataset, latest_snapshot, until_snapshot)

    self._send_incremental_volume(until_snapshot, destination)

    if len(all_snapshots) > 1:
      all_snapshots = all_snapshots[:-1]

      for snapshot in all_snapshots:
        destination.destroy(snapshot)

  def receive(self, source, dataset, snapshot_name):
    """
    Puts a snapshot in S3 using Uploader
    """

    key = "/".join(self.data.path)
    key = key[1:]
    key += "/" + self.data.dataset + "@" + snapshot_name

    self.log.info("Receiving to s3 to bucket=%s, key=%s", self.data.bucket, key)

    Uploader(AWS_ACCESS_KEY, AWS_SECRET_KEY).upload(source.stdout, self.data.bucket, key)
