from law.target.remote.interface import RemoteFileInterface
from .grid_tools import get_voms_proxy_info, GfalError, gfal_copy_safe, gfal_exists, gfal_rm
from .run_tools import repeat_until_success

class GFALFileInterface(RemoteFileInterface):
  local_prefix = 'file://'

  def __init__(self, base):
    self.voms_token = get_voms_proxy_info()['path']
    super(GFALFileInterface, self).__init__(base=base)

  def is_local(self, path):
    return path.startswith(GFALFileInterface.local_prefix)

  def exists(self, path, base=None, **kwargs):
    path_uri = self.uri(path, base=base)
    return gfal_exists(path_uri, voms_token=self.voms_token)

  def remove(self, path, base=None, silent=True, **kwargs):
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
      for dst_uri in dst_uris:
        gfal_copy_safe(src_uri, dst_uri, voms_token=self.voms_token, verbose=0)
      return src_uri, dst_uris
    elif dst_local and not src_local:
      dst_uri = dst
      src_uris = self.uri(src, base=base, return_all=True)
      opt_list = [ [uri,] for uri in src_uris ]
      successful_src_uri = None
      def copy(src_uri):
        gfal_copy_safe(src_uri, dst_uri, voms_token=self.voms_token, n_retries=1, verbose=0)
        successful_src_uri = src_uri
      repeat_until_success(copy, opt_list=opt_list,
                           exception=GfalError(f"GFALFileInterface: failed to copy {src} to {dst}"))
      return successful_src_uri, dst_uri
    raise RuntimeError(f'GFALFileInterface: unable to copy {src} -> {dst}. Either source or destination must be local')

  @staticmethod
  def _raise_not_implemented(method_name):
    raise NotImplementedError(f'{method_name} is not supported by the GFAL interface')

  def chmod(self, file, perm, **kwargs):
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
