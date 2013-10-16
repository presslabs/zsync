import os
from cStringIO import StringIO
from collections import namedtuple

from zsync import Pipeable, Receivable

from zsync.uploader import Uploader
from zsync.downloader import Downloader
from zsync.snapshot import Snapshot

from multiprocessing import Process, Pipe

AWS_ACCESS_KEY = "AKIAJ6NBXHHV3P6IIXRA"
AWS_SECRET_KEY = "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"

class S3(Pipeable):

  def __init__(self, data):
    self.data = data

  def downloader(self, bucket, key, fp):
    fp = os.fdopen(fp, 'w')

    Downloader(AWS_ACCESS_KEY, AWS_SECRET_KEY).download(fp, bucket, key)

  def _send_full_snahpshot(self, dataset, destination, first_snapshot):
    key = "/".join(self.data.path)
    key = key[1:]
    key += "/" + self.data.dataset + "@" + first_snapshot

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
    self._send_full_snahpshot(dataset, destination, second_snapshot)

  def _send_incremental_volume(self, until_snapshot, destination):
    local_snapshot_manager = Snapshot.from_context(self)
    destination_strategy = Snapshot.from_context(destination)

    # get destination dataset
    destination_dataset = destination.data.dataset if hasattr(destination.data, "dataset") else destination.data.bucket

    # get latest snapshot from the destination
    latest_snapshot = destination_strategy.get_latest_snapshot(destination_dataset)
    # get local snapshots between, destination latest snapshot and the given snapshot
    all_snapshots = local_snapshot_manager.get_snapshots_between(self.data.dataset, latest_snapshot, until_snapshot)

    print self.data.dataset, latest_snapshot, until_snapshot
    print all_snapshots

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
    # HACK Circular dependency hack
    from zsync import Remote
    if isinstance(destination, S3) or isinstance(destination, Remote):
      raise NotImplementedError("Remote to Remote or Remote to S3 is not implemented")

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
    key = "/".join(self.data.path)
    key = key[1:]

    key += "/" + self.data.dataset + "@" + snapshot_name

    Uploader(AWS_ACCESS_KEY, AWS_SECRET_KEY).upload(source.stdout, self.data.bucket, key)
