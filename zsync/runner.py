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

zsync.add_param("--size", help="Chunk size when uploading. In MB", default=50)
zsync.add_param("-c", help="Concurrency of upload/download - a number between 1 and 100", default=100)
zsync.add_param("--storage-class", dest='storage_class', default='STANDARD',
                help="Specify the S3 storage class (STANDARD, STANDARD_IA, or REDUCED_REDUNDANCY).")


zsync.add_param("--exclude", help="Exclude snapshots matching this regex", default=None)
zsync.add_param("--include", help="Include snapshots matching this regex", default=None)

def execute_from_cli():
  """
  Entry point for zsync
  """
  zsync.run()
