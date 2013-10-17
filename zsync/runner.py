"""
Entry point for the zsync command line tool. Sets up the CLI params and then
runs an instance of the ZSyncBase class.
"""

import cli.log

from zsync.zsync_base import ZSyncBase

@cli.log.LoggingApp
def zsync(app):
  """
  Sets up ZsyncBase with params and logging and the runs it
  """
  ZSyncBase(app.params, app.log).run()

zsync.add_param("source", help="The source volume that you want to synchronize from. Can be either local volume, remote (via ssh), or S3", default=False)
zsync.add_param("destination", help="The destination volume that you want to synchronize the data to. Can either be local, remote (via ssh) or S3", default=False)

zsync.add_param('--full', action='store_const', default=False, const=True,
                help='Makes a full backup out of the volume')

zsync.add_param('--dryrun', action='store_const', default=False, const=True,
                help="Doesn't do the actual synchronisation but only shows which \
                snapshots are going to be transfered")

zsync.add_param('-e', help='Remote shell user settings')

def execute_from_cli():
  """
  Entry point for zsync
  """
  zsync.run()
