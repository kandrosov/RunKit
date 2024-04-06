from law.target.remote.interface import RemoteFileInterface
from .grid_tools import get_voms_proxy_info, gfal_copy_safe, gfal_exists, gfal_rm

class GFALFileInterface(RemoteFileInterface):
  def __init__(self, base):
    self.voms_token = get_voms_proxy_info()['path']
    super(GFALFileInterface, self).__init__(base=base)

  def exists(self, path, **kwargs):
    path_uri = self.get_uri(path)
    return gfal_exists(path_uri, voms_token=self.voms_token)

  def remove(self, path, **kwargs):
    gfal_rm(path, voms_token=self.voms_token, recursive=False)

  def get_uri(self, path):
    local_prefix = 'file://'
    is_local = path.startswith(local_prefix)
    return path[len(local_prefix):] if is_local else self.base[0] + path

  def filecopy(self, src, dst, **kwargs):
    src_uri = self.get_uri(src)
    dst_uri = self.get_uri(dst)
    gfal_copy_safe(src_uri, dst_uri, voms_token=self.voms_token, verbose=0)
    return src_uri, dst_uri

  @staticmethod
  def _raise_not_implemented(method_name):
    raise NotImplementedError(f'{method_name} is not supported by the GFAL interface')

  def chmod(self, file, perm, silent=False, **kwargs):
    return True

  def isdir(self, path, **kwargs):
    return True

  def isfile(self):
    self._raise_not_implemented('isfile')

  def listdir(self):
    self._raise_not_implemented('listdir')

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
