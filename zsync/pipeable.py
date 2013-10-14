from abc import ABCMeta, abstractmethod

class Pipeable(object):

  __metaclass__ = ABCMeta

  @abstractmethod
  def send(self, to):
    pass
