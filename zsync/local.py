from subprocess import PIPE, Popen

from zsync import Pipeable, Receivable

"""
Local -> local:
  zfs send send_volume_name | zfs receive -F dest_volume_name

  zfs send -I send_volume_name@snap1 send_volume_name@span2 | zfs receive -F dest_volume_name
"""
class Local(Pipeable, Receivable):

  def __init__(self, path):
    self.data = path

  def pipe(self, destination):
    data = Popen(["zfs", "send", self.data.path], stdout=PIPE)
    destination.receive(data)

  def receive(self, source):
    receive_endpoint = Popen(["zfs", "receive", "-F", self.data.path], stdin=source.stdout, stdout=PIPE)
    source.stdout.close()
    output = receive_endpoint.communicate()[0]
