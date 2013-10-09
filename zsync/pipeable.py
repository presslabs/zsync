from abc import ABCMeta, abstractmethod

class Pipeable(object):

  @abstractmethod
  def pipe(self, to):
    pass
