from abc import ABCMeta, abstractmethod

class Receivable(object):

  @abstractmethod
  def receive(self):
    pass
