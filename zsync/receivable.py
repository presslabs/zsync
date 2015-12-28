# vim: ft=python:sw=2:ts=2:sts=2:et:fileencoding=utf8:nu:

from abc import ABCMeta, abstractmethod

class Receivable(object):

  __metaclass__ = ABCMeta

  @abstractmethod
  def receive(self, data, dataset, destination):
    pass
