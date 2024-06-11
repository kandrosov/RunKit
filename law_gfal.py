import datetime
import os
from law.target.remote.interface import RemoteFileInterface
from .grid_tools import get_voms_proxy_info, GfalError, gfal_copy_safe, gfal_ls_safe, gfal_rm
from .run_tools import repeat_until_success


class LsCacheEntry:
  def __init__(self, path, entries, time_acquired, validity_period):
    self.path = path
    self.entries = entries
    self.time_acquired = time_acquired
    self.validity_period = validity_period

  def is_valid(self):
    now = datetime.datetime.now()
    delta_t = (now - self.time_acquired).total_seconds()
    return delta_t < self.validity_period

class DirLsCache:
  def __init__(self, validity_period):
    self.validity_period = validity_period
    self.cache = {}

  def add(self, path, entries):
    self.cache[path] = LsCacheEntry(path, entries, datetime.datetime.now(), self.validity_period)

  def get(self, path):
    if path in self.cache:
      entry = self.cache[path]
      if entry.is_valid():
        return entry.entries
      else:
        del self.cache[path]
    return None

  def invalidate(self, path):
    to_invalidate = []
    for key in self.cache:
      if path.startswith(key):
        to_invalidate.append(key)
    for key in to_invalidate:
      del self.cache[key]

class GFALFileInterface(RemoteFileInterface):
  local_prefix = 'file://'

  def __init__(self, base, ls_cache_validity_period=60):
    self.voms_token = get_voms_proxy_info()['path']
    self.ls_cache = DirLsCache(ls_cache_validity_period)
    super(GFALFileInterface, self).__init__(base=base)

  def is_local(self, path):
    return path.startswith(GFALFileInterface.local_prefix)

  def exists(self, path, base=None, **kwargs):
    path_dir, path_name = os.path.split(path)
    dir_entries = self.listdir(path_dir, base=base, silent=True)
    return path_name in dir_entries

  def remove(self, path, base=None, silent=True, **kwargs):
    path_dir, _ = os.path.split(path)
    self.ls_cache.invalidate(path_dir)
    path_uri = self.uri(path, base=base)
    try:
      gfal_rm(path_uri, voms_token=self.voms_token, recursive=True)
      return True
    except GfalError as e:
      if not silent:
        raise e
    return False

  def filecopy(self, src, dst, base=None, **kwargs):
    src_local = self.is_local(src)
    dst_local = self.is_local(dst)
    if src_local and not dst_local:
      dst_uris = self.uri(dst, base=base, return_all=True)
      src_uri = src
      path_dir, _ = os.path.split(dst)
      self.ls_cache.invalidate(path_dir)
      for dst_uri in dst_uris:
        gfal_copy_safe(src_uri, dst_uri, voms_token=self.voms_token, verbose=0)
      return src_uri, dst_uris
    elif dst_local and not src_local:
      dst_uri = dst
      src_uris = self.uri(src, base=base, return_all=True)
      path_dir, _ = os.path.split(src)
      self.ls_cache.invalidate(path_dir)
      opt_list = [ [uri,] for uri in src_uris ]
      successful_src_uri = None
      def copy(src_uri):
        nonlocal successful_src_uri
        gfal_copy_safe(src_uri, dst_uri, voms_token=self.voms_token, n_retries=1, verbose=0)
        successful_src_uri = src_uri
      repeat_until_success(copy, opt_list=opt_list,
                           exception=GfalError(f"GFALFileInterface: failed to copy {src} to {dst}"))
      return successful_src_uri, dst_uri
    raise RuntimeError(f'GFALFileInterface: unable to copy {src} -> {dst}. Either source or destination must be local')

  def listdir(self, path, base=None, silent=False, **kwargs):
    entries = self.ls_cache.get(path)
    if entries is None:
      path_uri = self.uri(path, base=base)
      entries = gfal_ls_safe(path_uri, voms_token=self.voms_token, catch_stderr=True, verbose=0)
      if entries is None:
        if not silent:
          raise GfalError(f'GFALFileInterface: failed to list directory {path}')
        entries = []
      self.ls_cache.add(path, entries)
    return [ entry.name for entry in entries ]

  @staticmethod
  def _raise_not_implemented(method_name):
    raise NotImplementedError(f'{method_name} is not supported by the GFAL interface')

  def chmod(self, file, perm, **kwargs):
    return True

  def isdir(self, path, **kwargs):
    return True

  def isfile(self):
    self._raise_not_implemented('isfile')

  def mkdir(self):
    self._raise_not_implemented('mkdir')

  def mkdir_rec(self):
    self._raise_not_implemented('mkdir_rec')

  def rmdir(self):
    self._raise_not_implemented('rmdir')

  def stat(self):
    self._raise_not_implemented('stat')

  def unlink(self):
    self._raise_not_implemented('unlink')
