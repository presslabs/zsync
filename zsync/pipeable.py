# vim: ft=python:sw=2:ts=2:sts=2:et:fileencoding=utf8:nu:

from abc import ABCMeta, abstractmethod

class Pipeable(object):

  __metaclass__ = ABCMeta

  @abstractmethod
  def send(self, to):
    pass
