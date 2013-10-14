from abc import ABCMeta, abstractmethod

class Receivable(object):

  __metaclass__ = ABCMeta

  @abstractmethod
  def receive(self):
    pass
