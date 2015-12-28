# vim: ft=python:sw=2:ts=2:sts=2:et:fileencoding=utf8:nu:

import sys

from subprocess import PIPE, Popen, call

from snapshot import Snapshot
from zsync import Sync

class Local(Sync):

  def __init__(self, data, log):
    super(Local, self).__init__(data, log)

  def destroy(self, snapshot):
    cmd = "zfs destroy %s@%s" % (self.data.dataset, snapshot)

    if self.args.dryrun:
      sys.stdout.write("\n%s\n" % cmd)
    else:
      data = call(cmd, shell=True, stdout=PIPE)

  def _send_full_snahpshot(self, dataset, destination, first_snapshot):
    cmd = "zfs send %s@%s" % (dataset, first_snapshot)

    if self.args.dryrun:
      sys.stdout.write(cmd)
      destination.receive(None, dataset, first_snapshot)
    else:
      self.log.info("Running send as cmd=\"%s\"" % cmd)

      data = Popen(cmd, shell=True, stdout=PIPE)
      destination.receive(data, dataset, first_snapshot)

  def _send_incremental_snapshot(self, dataset, destination, first_snapshot,
                                 second_snapshot):

    cmd = "zfs send {send_type} {dataset}@{first_snapshot} {dataset}@{second_snapshot}".format(
      send_type="-i" if self.args.selective else "-I",
      dataset=dataset,
      first_snapshot=first_snapshot,
      second_snapshot=second_snapshot
    ) 

    if self.args.dryrun:
      sys.stdout.write(cmd)
      destination.receive(None, dataset, second_snapshot)
    else:
      self.log.info("Running send incremental as cmd=\"%s\"" % cmd)
      data = Popen(cmd, shell=True, stdout=PIPE)
      destination.receive(data, dataset, second_snapshot)

  def receive(self, source, dataset, snapshot_name):
    self.log.info("Receiving local to dataset=\"%s\" and snapshot=\"%s\"", self.data.dataset, snapshot_name)

    if self.args.dryrun:
      sys.stdout.write(" | zfs receive -F %s\n" % self.data.dataset)
    else:
      receive_endpoint = Popen(["zfs", "receive", "-F", self.data.dataset], stdin=source.stdout, stdout=PIPE)
      source.stdout.close()
      output = receive_endpoint.communicate()[0]
