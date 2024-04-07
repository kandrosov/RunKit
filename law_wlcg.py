# coding: utf-8

"""
WLCG remote file system and targets.
"""

__all__ = ["WLCGFileSystem", "WLCGTarget", "WLCGFileTarget", "WLCGDirectoryTarget"]


import law
from law.target.remote import RemoteFileSystem, RemoteTarget, RemoteFileTarget, RemoteDirectoryTarget
from law.logger import get_logger

logger = get_logger(__name__)

from .law_gfal import GFALFileInterface
from .grid_tools import path_to_pfn


class WLCGFileSystem(RemoteFileSystem):
  def __init__(self, base):
    if type(base) is str:
      base = [base]
    base_pfns = [path_to_pfn(b) for b in base]
    file_interface = GFALFileInterface(base_pfns)
    super(WLCGFileSystem, self).__init__(file_interface)

class WLCGTarget(RemoteTarget):
  def __init__(self, path, fs, **kwargs):
    RemoteTarget.__init__(self, path, fs, **kwargs)

class WLCGFileTarget(WLCGTarget, RemoteFileTarget):
  pass

class WLCGDirectoryTarget(WLCGTarget, RemoteDirectoryTarget):
  pass


WLCGTarget.file_class = WLCGFileTarget
WLCGTarget.directory_class = WLCGDirectoryTarget