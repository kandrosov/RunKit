import json
import os
import re
import shutil
import sys
import time
import tempfile
import ROOT

if __name__ == "__main__":
  file_dir = os.path.dirname(os.path.abspath(__file__))
  base_dir = os.path.dirname(file_dir)
  if base_dir not in sys.path:
    sys.path.append(base_dir)
  __package__ = os.path.split(file_dir)[-1]

from .run_tools import PsCallError, ps_call, adler32sum

class LocalIO:
  def ls(self, path, recursive=False, not_exists_ok=False):
    if not os.path.exists(path):
      if not_exists_ok:
        return []
      raise RuntimeError(f'Path "{path}" does not exists.')
    all_files = []
    if recursive:
      for root, dirs, files in os.walk(path):
        for file in files:
            file_path = os.path.join(root, file)
            file_size = os.path.getsize(file_path)
            all_files.append((file_path, file_size))
    else:
      for file in os.lsdir(path):
        file_path = os.path.join(path, file)
        file_size = os.path.getsize(file_path)
        all_files.append((file_path, file_size))
    return all_files

  def rm(self, path):
    shutil.rmtree(path)

  def copy(self, src, dst):
    dst_dir = os.path.dirname(dst)
    os.makedirs(dst_dir, exist_ok=True)
    shutil.copy(src, dst)

  def move(self, src, dst):
    dst_dir = os.path.dirname(dst)
    os.makedirs(dst_dir, exist_ok=True)
    shutil.move(src, dst)

  def copy_local(self, files, out_dir):
    return None

class RemoteIO:
  def __init__(self):
    from .grid_tools import get_voms_proxy_info, gfal_ls, gfal_ls_recursive, gfal_copy_safe, gfal_rm, \
                            gfal_rename, gfal_exists
    self.gfal_ls = gfal_ls
    self.gfal_ls_recursive = gfal_ls_recursive
    self.gfal_copy_safe = gfal_copy_safe
    self.gfal_rm = gfal_rm
    self.gfal_rename = gfal_rename
    self.gfal_exists = gfal_exists
    self.voms_token = get_voms_proxy_info()['path']

  def ls(self, path, recursive=False, not_exists_ok=False):
    if not self.gfal_exists(path, voms_token=self.voms_token):
      if not_exists_ok:
        return []
      raise RuntimeError(f'Path "{path}" does not exists.')
    files = []
    if recursive:
      ls_result = self.gfal_ls_recursive(path, voms_token=self.voms_token)
    else:
      ls_result = self.gfal_ls(path, voms_token=self.voms_token)
    for file in ls_result:
      files.append((file.full_name, file.size))
    return files

  def rm(self, path):
    self.gfal_rm(path, recursive=True, voms_token=self.voms_token)

  def copy(self, src, dst):
    self.gfal_copy_safe(src, dst, voms_token=self.voms_token)

  def move(self, src, dst):
    self.gfal_rename(src, dst, voms_token=self.voms_token)

  def copy_local(self, files, out_dir):
    local_files = []
    for file in files:
      file_base = os.path.basename(file)
      file_name, file_ext = os.path.splitext(file_base)
      idx = 0
      while True:
        local_name = file_base if idx == 0 else f'{file_name}_{idx}{file_ext}'
        local_file = os.path.join(out_dir, local_name)
        if local_file not in local_files:
          break
        idx += 1
      self.copy(file, local_file)
      local_files.append(local_file)
    return local_files

def toMiB(size):
  return float(size) / (1024 * 1024)

def fromMiB(size):
  return int(size * 1024 * 1024)

class InputFile:
  def __init__(self, name, size):
    self.name = name
    self.size = size

class InputBlock:
  def __init__(self):
    self.files = set()
    self.run_lumi = {}

  @property
  def size(self):
    return sum([ f.size for f in self.files ])

  def has_overlap(self, other):
    for run, lumis in other.run_lumi.items():
      if run not in self.run_lumi: continue
      if len(lumis & self.run_lumi[run]) > 0:
        return True
    return False

  @staticmethod
  def create(input_files, file_run_lumi):
    blocks = []
    for file in input_files:
      block = InputBlock()
      block.add(file, file_run_lumi)
      blocks.append(block)
    had_merge = True
    while had_merge:
      new_blocks = []
      processed_indices = set()
      had_merge = False
      for block_idx, block in enumerate(blocks):
        if block_idx in processed_indices: continue
        overlap_idx = []
        overlap_blocks = []
        for other_idx in range(block_idx+1, len(blocks)):
          if other_idx in processed_indices: continue
          other = blocks[other_idx]
          if block.has_overlap(other):
            overlap_blocks.append(other)
            overlap_idx.append(other_idx)
        processed_indices.add(block_idx)
        if len(overlap_blocks) == 0:
          new_blocks.append(block)
        else:
          merged_block = InputBlock.merge(block, *overlap_blocks)
          new_blocks.append(merged_block)
          processed_indices.update(overlap_idx)
          had_merge = True
      blocks = new_blocks
    return blocks

  @staticmethod
  def merge(*blocks):
    merged_block = InputBlock()
    for block in blocks:
      merged_block.files.update(block.files)
      for run, lumis in block.run_lumi.items():
        if run not in merged_block.run_lumi:
          merged_block.run_lumi[run] = set()
        merged_block.run_lumi[run].update(lumis)
    return merged_block

  def add(self, input_file, file_run_lumi):
    self.files.add(input_file)
    if file_run_lumi is not None:
      run_lumi = file_run_lumi[input_file.name]
      for run, lumis in run_lumi.items():
        if run not in self.run_lumi:
          self.run_lumi[run] = set()
        self.run_lumi[run].update(lumis)

class OutputFile:
  def __init__(self):
    self.name = None
    self.expected_size = 0.
    self.input_files = []
    self.input_files_local = None

  def try_add(self, block, max_size):
    if len(self.input_files) > 0 and self.expected_size + block.size > max_size:
      return False
    self.expected_size += block.size
    self.input_files.extend(block.files)
    return True

  def try_merge(self, input_names):
    try:
      if os.path.exists(self.out_path):
        os.remove(self.out_path)
      haddnano_path = os.path.join(os.path.dirname(__file__), 'haddnano.py')
      cmd = ['python3', '-u', haddnano_path, self.out_path ] + input_names
      ps_call(cmd, verbose=1)
      return True, None
    except (PsCallError, OSError, FileNotFoundError) as e:
      return False, e

  def merge(self, out_dir, max_n_retries, retry_interval):
    n_retries = 0
    self.out_path = os.path.join(out_dir, self.name)
    input_names = [ f.name for f in self.input_files ] if self.input_files_local is None else self.input_files_local
    while True:
      merged, error = self.try_merge(input_names)
      if merged: return
      n_retries += 1
      if n_retries >= max_n_retries:
        raise error
      print(f"Merge failed. {error}\nWaiting {retry_interval} seconds before the next attempt...")
      time.sleep(retry_interval)

  def filter_duplicates(self):
    skim_tree_path = os.path.join(os.path.dirname(__file__), 'skim_tree.py')
    filter_duplicates_path = os.path.join(os.path.dirname(__file__), 'filter_duplicates.py')
    self.out_filtered_path = self.out_path.replace('.root', '_filtered.root')
    cmd = [ 'python3', '-u', skim_tree_path, '--input', self.out_path, '--output', self.out_filtered_path,
            '--input-tree', 'Events,EventsNotSelected', '--ignore-absent',
            '--processing-module', f'{filter_duplicates_path}:filter',
            '--other-trees', 'LuminosityBlocks,Runs', '--verbose', '1' ]
    ps_call(cmd, verbose=1)
    self.size = os.path.getsize(self.out_filtered_path)

def loadEventStats(file_name):
  stats = {}
  root_file = ROOT.TFile.Open(file_name, 'READ')
  for tree_name, stat_name in [ ('Events', 'n_selected'), ('EventsNotSelected', 'n_not_selected') ]:
    tree = root_file.Get(tree_name)
    if not (tree == None):
      df = ROOT.RDataFrame(tree)
      stat_value = df.Count().GetValue()
    else:
      stat_value = 0
    stats[stat_name] = stat_value
  root_file.Close()
  return stats


def mergeFiles(output_files, output_dir, output_name_base, work_dir, io_provider, max_n_retries, retry_interval):
  merge_dir = os.path.join(work_dir, 'merged')
  os.makedirs(merge_dir, exist_ok=True)
  input_dir = os.path.join(work_dir, 'input')
  os.makedirs(merge_dir, exist_ok=True)
  output_tmp = os.path.join(output_dir, output_name_base + '.tmp')

  for file in output_files:
    print(f'Merging {len(file.input_files)} input files into {file.name}...')
    file.input_files_local = io_provider.copy_local([ f.name for f in file.input_files ], input_dir)
    file.merge(merge_dir, max_n_retries, retry_interval)
    file.filter_duplicates()
    output_stats = loadEventStats(file.out_path)
    output_filtered_stats = loadEventStats(file.out_filtered_path)
    file.n_selected = output_filtered_stats['n_selected']
    file.n_not_selected = output_filtered_stats['n_not_selected']
    file.size = os.path.getsize(file.out_filtered_path)
    file.n_selected_original = output_stats['n_selected']
    file.n_not_selected_original = output_stats['n_not_selected']
    file.size_original = os.path.getsize(file.out_path)
    file.adler32sum = adler32sum(file.out_filtered_path)
    file.remote_tmp_path = os.path.join(output_tmp, file.name)
    io_provider.copy(file.out_filtered_path, file.remote_tmp_path)
    os.remove(file.out_path)
    os.remove(file.out_filtered_path)
    if file.input_files_local is not None:
      for file_name in file.input_files_local:
        os.remove(file_name)
    print(f'Done. Expected size = {toMiB(file.expected_size):.1f} MiB, actual size = {toMiB(file.size):.1f} MiB.')
  print("Moving merged files into the final location and removing temporary files.")
  for file in output_files:
    io_provider.move(file.remote_tmp_path, os.path.join(output_dir, file.name))
  io_provider.rm(output_tmp)
  print('All inputs have been merged.')

def getWorkDir(work_dir):
  if work_dir is not None:
    return work_dir
  tmp_base= os.environ.get('TMPDIR', '.')
  os.makedirs(tmp_base, exist_ok=True)
  return tempfile.mkdtemp(dir=tmp_base)

def getInputFiles(input_dirs, file_list, io_provider):
  input_files = []
  for input_dir in input_dirs:
    for file_name, file_size in io_provider.ls(input_dir, recursive=True, not_exists_ok=False):
      if file_name.endswith('.root'):
        input_files.append(InputFile(file_name, file_size))
  if file_list is not None:
    with open(file_list, 'r') as f:
      lines = [ l for l in f.read().splitlines() if len(l) > 0 ]
    for line in lines:
      files = io_provider.ls(line, recursive=False, not_exists_ok=False)
      if len(files) != 1:
        raise RuntimeError(f'File "{line}" not found.')
      file_name, file_size = files[0]
      input_files.append(InputFile(file_name, file_size))
  return input_files

def createOutputPlan(input_files, target_size, output_name_base, file_run_lumi):
  input_blocks = InputBlock.create(input_files, file_run_lumi)
  input_blocks = sorted(input_blocks, key=lambda b: -b.size)
  processed_blocks = set()
  output_files = []
  while len(processed_blocks) < len(input_blocks):
    output_file = OutputFile()
    for block in input_blocks:
      if block not in processed_blocks and output_file.try_add(block, target_size):
        processed_blocks.add(block)
    output_files.append(output_file)
  for idx, file in enumerate(output_files):
    file.name = output_name_base + f'_{idx}.root'
  processed_files = set()
  for output_file in output_files:
    for input_file in file.input_files:
      if input_file in processed_files:
        raise RuntimeError(f'File "{input_file.name}" is duplicated.')
      processed_files.add(input_file)
  if len(processed_files) != len(input_files):
    raise RuntimeError("Some input files were not scheduled for processing.")
  return output_files

def cleanOutput(output_dir, output_name_base, io_provider):
  name_pattern = re.compile(f'^{output_name_base}(|_[0-9]+)\.(root|tmp)$')
  files = io_provider.ls(output_dir, recursive=False, not_exists_ok=True)
  for file, file_size in files:
    if name_pattern.match(file):
      io_provider.rm(file)

def haddnanoEx(input_dirs, file_list, output_dir, output_name, work_dir, target_size, use_remote_io, max_n_retries,
               retry_interval, merge_report_path, file_run_lumi_path):
  work_dir = getWorkDir(work_dir)
  io_provider = RemoteIO() if use_remote_io else LocalIO()
  input_files = getInputFiles(input_dirs, file_list, io_provider)
  if len(input_files) == 0:
    raise RuntimeError("No input files were found.")
  output_name_base, output_name_ext = os.path.splitext(output_name)
  if output_name_ext != '.root':
    raise RuntimeError(f'Unsupported output file format "{output_name_ext}"')

  if file_run_lumi_path is not None:
    with open(file_run_lumi_path, 'r') as f:
      file_run_lumi = json.load(f)
  else:
    file_run_lumi = None

  output_files = createOutputPlan(input_files, target_size, output_name_base, file_run_lumi)
  cleanOutput(output_dir, output_name_base, io_provider)
  mergeFiles(output_files, output_dir, output_name_base, work_dir, io_provider, max_n_retries, retry_interval)

  if merge_report_path is not None:
    merge_report = {}
    for output in output_files:
      merge_report[output.name] = {
        'adler32sum': output.adler32sum,
        'n_selected': output.n_selected,
        'n_not_selected': output.n_not_selected,
        'size': output.size,
        'n_selected_original': output.n_selected_original,
        'n_not_selected_original': output.n_not_selected_original,
        'size_original': output.size_original,
        'inputs': [ f.name for f in output.input_files ]
      }
    with open(merge_report_path, 'w') as f:
      json.dump(merge_report, f, indent=2)

if __name__ == "__main__":
  import argparse
  parser = argparse.ArgumentParser(description='hadd nano files.')
  parser.add_argument('--output-dir', required=True, type=str, help="Output where merged files will be stored.")
  parser.add_argument('--output-name', required=False, type=str, default='nano.root',
                      help="Name of the output files. _1, _2, etc. suffices will be added.")
  parser.add_argument('--work-dir', required=False, default=None, type=str, help="Work directory for temporary files.")
  parser.add_argument('--target-size', required=False, type=float, default=1024.,
                      help="target output file size in MiB")
  parser.add_argument('--remote-io', action='store_true', help='use remote I/O')
  parser.add_argument('--n-retries', required=False, type=int, default=1,
                      help="maximal number of retries in case if hadd fails. " + \
                           "The retry counter is reset to 0 after each successful hadd.")
  parser.add_argument('--retry-interval', required=False, type=int, default=60,
                      help="interval in seconds between retry attempts.")
  parser.add_argument('--merge-report', required=False, default=None, type=str,
                      help="File where to store merge report.")
  parser.add_argument('--file-list', required=False, type=str, default=None,
                      help="txt file with the list of input files to merge")
  parser.add_argument('--file-run-lumi', required=False, type=str, default=None,
                      help="json file with file -> (run, lumi) correspondance. "
                           "If provided, potential duplicates will be removed.")
  parser.add_argument('input_dir', type=str, nargs='*', help="input directories")
  args = parser.parse_args()

  haddnanoEx(args.input_dir, args.file_list, args.output_dir, args.output_name, args.work_dir,
             fromMiB(args.target_size), args.remote_io, args.n_retries, args.retry_interval, args.merge_report,
             args.file_run_lumi)
