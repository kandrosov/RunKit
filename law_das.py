import datetime
import os
from law.target.remote.interface import RemoteFileInterface
from .grid_tools import get_voms_proxy_info, copy_remote_file
from .run_tools import repeat_until_success
from .getFileRunLumi import getFileRunLumi

class DASFileInterface(RemoteFileInterface):
  local_prefix = 'file://'

  def __init__(self, *, ls_cache_validity_period=60):
    self.voms_token = get_voms_proxy_info()['path']
    self.dataset_files = {}
    super(DASFileInterface, self).__init__(base=["/"])

  def is_local(self, path):
    return path.startswith(DASFileInterface.local_prefix)

  def exists(self, path, base=None, **kwargs):
    self._raise_not_implemented('exists')

  def remove(self, path, base=None, silent=True, **kwargs):
    self._raise_not_implemented('remove')

  def filecopy(self, src, dst, base=None, **kwargs):
    src_local = self.is_local(src)
    dst_local = self.is_local(dst)
    if not (not src_local and dst_local):
      raise RuntimeError("DASFileInterface: only copy from remote to local is supported")
    dst_path = dst[len(DASFileInterface.local_prefix):]
    copy_remote_file(src, dst_path, voms_token=self.voms_token)
    return src, dst

  def listdir(self, path, base=None, silent=False, **kwargs):
    if path not in self.dataset_files:
      self.dataset_files[path] = getFileRunLumi(path)
    return list(self.dataset_files[path].keys())

  @staticmethod
  def _raise_not_implemented(method_name):
    raise NotImplementedError(f'{method_name} is not supported by the DAS interface')

  def chmod(self, file, perm, **kwargs):
    self._raise_not_implemented('chmod')

  def isdir(self, path, **kwargs):
    self._raise_not_implemented('isdir')

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
