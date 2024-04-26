from enum import Enum
import json
import os
import yaml
from run_tools import ps_call

class Status(Enum):
  OK = 0
  MISSING = 1
  MULTIPLE_INFO = 2
  INCONSISTENT_STATUS = 3
  NOT_VALID = 4
  QUERY_FAILED = 5

class DasInerface:
  def __init__(self, cache_file=None):
    self.cache_file = cache_file
    if cache_file is not None and os.path.isfile(cache_file):
      with open(cache_file, 'r') as f:
        self.cache = json.load(f)
    else:
      self.cache = {}

  def get_status(self, dataset):
    if dataset in self.cache:
      return Status[self.cache[dataset]]
    status = self.query_status(dataset)
    self.cache[dataset] = status.name
    if self.cache_file is not None:
      with open(self.cache_file, 'w') as f:
        json.dump(self.cache, f)
    return status

  def query_status(self, dataset):
    try:
      query = f'dataset dataset={dataset}'
      if dataset.endswith("USER"): # Embedded samples
        query += ' instance=prod/phys03'

      _, output, _ = ps_call(['dasgoclient', '--json', '--query', query], catch_stdout=True)
      entries = json.loads(output)
      ds_infos = []
      for entry in entries:
        if "dbs3:dataset_info" in entry['das']['services']:
          ds_infos.append(entry['dataset'])

      if len(ds_infos) == 0:
        return Status.MISSING
      status = None
      for ds_info in ds_infos:
        if len(ds_info) != 1:
          return Status.MULTIPLE_INFO
        if status is not None and status != ds_info[0]['status']:
          return Status.INCONSISTENT_STATUS
        status = ds_info[0]['status']
      if status != "VALID":
        return Status.NOT_VALID
    except:
      return Status.QUERY_FAILED
    return Status.OK

if __name__ == '__main__':
  import argparse
  parser = argparse.ArgumentParser(description='Check consistency of tasks configurations for crabOverseer.')
  parser.add_argument('--cache', type=str, required=False, default=None, help='File with cached results.')
  parser.add_argument('task_file', type=str, nargs='+', help="file(s) with task descriptions")
  args = parser.parse_args()

  datasets = []
  all_ok = True
  for entry in args.task_file:
    if entry.endswith('.yaml'):
      with open(entry, 'r') as f:
        cfg = yaml.safe_load(f)
      for task_name, task_desc in cfg.items():
        if task_name == 'config': continue
        customTask = type(task_desc) == dict
        if customTask:
          if 'inputDataset' not in task_desc:
            print(f'ERROR: "{entry}" task "{task_name}" does not have "inputDataset" field.')
            inputDataset = None
            all_ok = False
          else:
            inputDataset = task_desc['inputDataset']
        else:
          inputDataset = task_desc
        if inputDataset is None:
          raise ValueError(f'ERROR: "{entry}" task "{task_name}" does not have "inputDataset" field.')
        datasets.append(inputDataset)
    else:
      datasets.append(entry)

  print(f'Checking {len(datasets)} datasets from {len(args.task_file)} config files.')

  das_interface = DasInerface(args.cache)

  for dataset in datasets:
    status = das_interface.get_status(dataset)
    if status == Status.OK: continue
    all_ok = False
    print(f'{status}: {dataset}')
  if all_ok:
    print("All datasets exist.")

