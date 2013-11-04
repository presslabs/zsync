import sys
from subprocess import PIPE, Popen

from snapshot import Snapshot
from zsync import Sync

class Remote(Sync):

  def __init__(self, data, log):
    super(Remote, self).__init__(data, log)

  def _send_full_snahpshot(self, dataset, destination, first_snapshot):
    ssh_cmd = "%s %s zfs send %s@%s" % (self.args.e, self.data.host, dataset, first_snapshot)

    if self.args.dryrun:
      sys.stdout.write(ssh_cmd)
    else:
      self.log.info("Running send as cmd=\"%s\"" % ssh_cmd)

      data = Popen(ssh_cmd, shell=True, stdout=PIPE)
      destination.receive(data, dataset, first_snapshot)

  def _send_incremental_snapshot(self, dataset, destination, first_snapshot,
                                 second_snapshot):
    ssh_cmd = "%s %s zfs send -I %s@%s %s@%s" % (self.args.e, self.data.host, \
                                                dataset, first_snapshot, \
                                                dataset, second_snapshot )

    if self.args.dryrun:
      sys.stdout.write(ssh_cmd)
    else:
      self.log.info("Running send incremental as cmd=\"%s\"" % ssh_cmd)

      data = Popen(ssh_cmd, shell=True, stdout=PIPE)
      destination.receive(data, dataset, second_snapshot)

  def receive(self, source, dataset, snapshot_name):
    ssh_cmd = self.args.e + " " + self.data.host + " zfs receive -F %s" % self.data.dataset

    if self.args.dryrun:
      sys.stdout.write(" | " + ssh_cmd + "\n")
    else:
      self.log.info("Receiving local to dataset=\"%s\" and snapshot=\"%s\"", dataset, snapshot_name)

      receive_endpoint = Popen(ssh_cmd, stdin=source.stdout, stdout=PIPE, shell=True)
      source.stdout.close()
      output = receive_endpoint.communicate()[0]
