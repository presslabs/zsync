import cli.app

from zsync.zsync_base import ZSyncBase

@cli.app.CommandLineApp
def zsync(app):
  ZSyncBase(app.params).run()

zsync.add_param("source", help="The source volume that you want to synchronize from. Can be either local volume, remote (via ssh), or S3", default=False)
zsync.add_param("destination", help="The destination volume that you want to synchronize the data to. Can either be local, remote (via ssh) or S3", default=False)

zsync.add_param('--full', action='store_const', default=False, const=True,
                help='Makes a full backup out of the volume')

zsync.add_param('--dryrun', action='store_const', default=False, const=True,
                help="Doesn't do the actual synchronisation but only shows which \
                snapshots are going to be transfered")

def execute_from_cli():
  zsync.run()
