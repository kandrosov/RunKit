import fnmatch
import os
import re
import sys
import yaml

from yaml import Loader
from yaml.constructor import ConstructorError

def no_duplicates_constructor(loader, node, deep=False):
    """Check for duplicate keys."""

    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        value = loader.construct_object(value_node, deep=deep)
        if key in mapping:
            raise ConstructorError("while constructing a mapping", node.start_mark,
                                   "found duplicate key (%s)" % key, key_node.start_mark)
        mapping[key] = value

    return loader.construct_mapping(node, deep)

yaml.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, no_duplicates_constructor)

if __name__ == "__main__":
  file_dir = os.path.dirname(os.path.abspath(__file__))
  sys.path.append(os.path.dirname(file_dir))
  __package__ = 'RunKit'

from .grid_tools import run_dasgoclient

class CheckResult:
  def __init__(self, all_ok, tasks_by_name, tasks_by_dataset):
    self.all_ok = all_ok
    self.tasks_by_name = tasks_by_name
    self.tasks_by_dataset = tasks_by_dataset

def check_consistency_era(task_cfg_files, dataset_name_mask_mc=None, dataset_name_mask_data=None):
  tasks_by_name = {}
  tasks_by_dataset = {}
  all_ok = True
  for task_cfg_file in task_cfg_files:
    if not os.path.isfile(task_cfg_file):
      print(f'ERROR: "{task_cfg_file}" does not exist.')
      all_ok = False
      continue
    try:
      with open(task_cfg_file) as f:
        cfg = yaml.load(f, Loader=Loader)
    except Exception as e:
      print(f'ERROR: "{task_cfg_file}" unable to parse yaml.')
      print(e)
      all_ok = False
      continue
    if type(cfg) != dict:
      print(f'ERROR: "{task_cfg_file}" contains {type(cfg)}, while dict is expected.')
      all_ok = False
      continue
    is_data = cfg.get('config', {}).get('params', {}).get('sampleType', '') == 'data'
    for task_name, task_desc in cfg.items():
      if task_name == 'config': continue
      customTask = type(task_desc) == dict
      if customTask:
        if 'inputDataset' not in task_desc:
          print(f'ERROR: "{task_cfg_file}" task "{task_name}" does not have "inputDataset" field.')
          all_ok = False
          continue
        if 'ignoreFiles' in task_desc:
          print(f'WARNING: "{task_cfg_file}" task "{task_name}" has "ignoreFiles" field.')
        inputDataset = task_desc['inputDataset']
      else:
        inputDataset = task_desc
      task_entry = {
        'name': task_name,
        'inputDataset': inputDataset,
        'file': task_cfg_file,
        'isData': is_data,
      }
      if task_name not in tasks_by_name:
        tasks_by_name[task_name] = []
      tasks_by_name[task_name].append(task_entry)
      if inputDataset not in tasks_by_dataset:
        tasks_by_dataset[inputDataset] = []
      tasks_by_dataset[inputDataset].append(task_entry)
  for task_name, task_list in tasks_by_name.items():
    if len(task_list) > 1:
      print(f'ERROR: task "{task_name}" is defined in multiple files:')
      for task_entry in task_list:
        print(f'  file={task_entry["file"]} dataset={task_entry["inputDataset"]}')
      all_ok = False
  for inputDataset, task_list in tasks_by_dataset.items():
    if len(task_list) > 1:
      print(f'ERROR: input dataset "{inputDataset}" is defined in multiple tasks:')
      for task_entry in task_list:
        print(f'  file={task_entry["file"]} task={task_entry["name"]}')
      all_ok = False
    is_data = task_list[0]['isData']
    name_mask = dataset_name_mask_data if is_data else dataset_name_mask_mc
    if name_mask is not None and len(name_mask) > 0:
      if name_mask[0] == '^':
        mask_matched = re.match(name_mask, inputDataset)
      else:
        mask_matched = fnmatch.fnmatch(inputDataset, name_mask)
      if not mask_matched:
        print(f'ERROR: input dataset "{inputDataset}" does not matches the expected mask "{name_mask}"')
        all_ok = False

  return CheckResult(all_ok, tasks_by_name, tasks_by_dataset)


class ExceptionMatcher:
  def __init__(self, exceptions):
    self.exceptions = exceptions
    self.used_patterns = set()

  def get_known_exceptions(self, task_name):
    matched_eras = set()
    era_to_pattern_list = {}
    for task_pattern, eras in exceptions.items():
      if (task_pattern[0] == '^' and re.match(task_pattern, task_name)) or task_pattern == task_name:
        for era in eras:
          matched_eras.add(era)
          if era not in era_to_pattern_list:
            era_to_pattern_list[era] = []
          era_to_pattern_list[era].append(task_pattern)
        self.used_patterns.add(task_pattern)
    all_ok = True
    era_to_pattern = {}
    for era, patterns in era_to_pattern_list.items():
      if len(patterns) > 1:
        all_ok = False
        patterns_str = ', '.join(patterns)
        print(f'{task_name} is matched by multiple exception patterns that include {era}: {patterns_str}')
      era_to_pattern[era] = patterns[0]
    return all_ok, matched_eras, era_to_pattern_list

  def get_unused_patterns(self):
    return set(self.exceptions.keys()) - self.used_patterns

def check_task_consistency(task_name, eras, all_eras, exception_matcher, era_results, dataset_nameing_rules,
                           show_only_missing_with_candidates, search_for_missing_candidates):
  allowed_name_pattern = dataset_nameing_rules.get('allowed_name_pattern', None)
  if allowed_name_pattern is not None and not re.match(allowed_name_pattern, task_name):
    print(f'{task_name} contains invalid characters')
    return False
  known_name_variants = dataset_nameing_rules.get('known_name_variants', {})
  for variant, replacement in known_name_variants.items():
    if re.match(f'.*{variant}.*', task_name):
      print(f'{task_name} contains "{variant}" in the name, use "{replacement}" instead')
      return False
  n_eras = len(all_eras)
  is_data = era_results[eras[0]].tasks_by_name[task_name][0]['isData']
  exception_match_ok, known_exceptions, known_exception_to_pattern = exception_matcher.get_known_exceptions(task_name)
  if not exception_match_ok:
    return False
  missing_eras = all_eras - set(eras) - known_exceptions
  redundant_exceptions = known_exceptions & set(eras)
  if len(redundant_exceptions) > 0:
    known_exceptions_str = ', '.join(known_exceptions)
    redundant_exceptions_str = ', '.join(redundant_exceptions)
    known_exception_patterns_str = ', '.join(set([ k for v in known_exception_to_pattern.values() for k in v ]))
    print(f'{task_name} is listed as exception for [{known_exceptions_str}] in [{known_exception_patterns_str}]'
          f', but it exists for [{redundant_exceptions_str}]')
    return False
  if len(eras) != n_eras and not is_data and len(missing_eras) > 0:
    missing_eras_str = ', '.join(missing_eras)
    missing_prints = [ f'{task_name} is not available in: {missing_eras_str}' ]
    print_missing = not show_only_missing_with_candidates
    dataset_names = set()
    dataset_name_masks = dataset_nameing_rules.get('full_name_masks', {})
    for era in eras:
      for task in era_results[era].tasks_by_name[task_name]:
        missing_prints.append(f'  era={era} file={task["file"]} dataset={task["inputDataset"]}')
        dataset_name = re.match(dataset_name_masks[era]['mc'], task["inputDataset"]).group(1)
        if dataset_name is None or len(dataset_name) == 0:
          raise RuntimeError(f'Unable to extract dataset name from {task["inputDataset"]} using mask {dataset_name_masks[era]["mc"]}')
        dataset_names.add(dataset_name)
    if search_for_missing_candidates:
      for missing_era in missing_eras:
        das_candidates = set()
        for dataset_name in dataset_names:
          full_dataset_name = dataset_name_masks[missing_era]['mc_das'].format(dataset_name)
          new_das_candidates = run_dasgoclient(f'dataset dataset={full_dataset_name}', verbose=0)
          for candidate in new_das_candidates:
            if re.match(dataset_name_masks[missing_era]['mc'], candidate):
              add_candidate = True
              ext_match = re.match('.*_(ext[0-9]+)', task_name)
              if ext_match:
                ext_str = ext_match.group(1)
                if not re.match(f'/.+/.+[_-]{ext_str}[_-]v[0-9]+/.+', candidate):
                  add_candidate = False
              if add_candidate:
                das_candidates.add(candidate)
        if len(das_candidates) > 0:
          missing_prints.append(f'  {missing_era} potential candidates from DAS:')
          print_missing = True
          for candidate in das_candidates:
            missing_prints.append(f'    {candidate}')
        else:
          missing_prints.append(f' {missing_era} no candidates from DAS')
    if print_missing:
      for line in missing_prints:
        print(line)
    return False
  file_names = {}
  datasets = {}
  name_matchings = dataset_nameing_rules.get('name_matchings', [])
  for era in eras:
    for task in era_results[era].tasks_by_name[task_name]:
      file_name = os.path.split(task['file'])[1]
      if file_name not in file_names:
        file_names[file_name] = []
      file_names[file_name].append(era)
      dataset = task['inputDataset']
      dataset_name = dataset.split('/')[1]
      dataset_name_ref = dataset_name.lower()
      for pattern, replacement in name_matchings:
        dataset_name_ref = re.sub(pattern.lower(), replacement.lower(), dataset_name_ref)
      if dataset_name_ref not in datasets:
        datasets[dataset_name_ref] = []
      datasets[dataset_name_ref].append((era, dataset, dataset_name))
  if len(file_names) > 1:
    print(f'{task_name} is defined in multiple files:')
    for file_name, eras in file_names.items():
      print(f'  {file_name} in {", ".join(eras)}')
    return False
  if len(name_matchings) > 0:
    if len(datasets) > 1:
      print(f'{task_name} is used to refer different datasets:')
      for ref_name, era_list in datasets.items():
        for era, dataset, dataset_name in era_list:
          print(f'  era={era} dataset={dataset} ref={ref_name}')
      return False
  return True

def check_consistency(era_files_dict, exceptions, dataset_naming_rules, show_only_missing_with_candidates,
                      search_for_missing_candidates):
  era_results = {}
  tasks_by_name = {}
  all_ok = True
  dataset_name_masks = dataset_naming_rules.get('full_name_masks', None)
  for era, files in era_files_dict.items():
    if dataset_name_masks is None:
      dataset_name_mask_mc = None
      dataset_name_mask_data = None
    else:
      if era not in dataset_name_masks:
        raise RuntimeError(f'Era {era} is not found in dataset_name_masks')
      dataset_name_mask_mc = dataset_name_masks[era]['mc']
      dataset_name_mask_data = dataset_name_masks[era]['data']
    era_results[era] = check_consistency_era(files, dataset_name_mask_mc, dataset_name_mask_data)
    all_ok = all_ok and era_results[era].all_ok
    for task_name in era_results[era].tasks_by_name.keys():
      if task_name not in tasks_by_name:
        tasks_by_name[task_name] = []
      tasks_by_name[task_name].append(era)

  exception_matcher = ExceptionMatcher(exceptions)
  all_eras = set(era_files_dict.keys())
  for task_name, eras in tasks_by_name.items():
    task_consistent = check_task_consistency(task_name, eras, all_eras, exception_matcher, era_results,
                                             dataset_naming_rules, show_only_missing_with_candidates,
                                             search_for_missing_candidates)

    all_ok = all_ok and task_consistent
  unused_patterns = exception_matcher.get_unused_patterns()
  if len(unused_patterns) > 0:
    print(f'WARNING: unused entries in exceptions: {", ".join(unused_patterns)}')
  return all_ok

if __name__ == "__main__":
  import argparse
  parser = argparse.ArgumentParser(description='Check consistency of tasks configurations for crabOverseer.')
  parser.add_argument('--cross-eras', action='store_true', help='Check consistency of tasks across different eras.')
  parser.add_argument('--era', type=str, required=False, default=None, help='Era')
  parser.add_argument('--exceptions', type=str, required=False, default=None,
                      help='File with exceptions for the checks.')
  parser.add_argument('--dataset-naming-rules', type=str, required=False, default=None,
                      help='File with naming rules')
  parser.add_argument('--show-only-missing-with-candidates', action='store_true',
                      help='Only show missing samples that have potential candidates in DAS.')
  parser.add_argument('--search-for-missing-candidates', action='store_true',
                      help='Search for missing candidates in DAS.')
  parser.add_argument('task_file', type=str, nargs='+', help="file(s) with task descriptions")
  args = parser.parse_args()

  era_files_dict = {}
  if args.cross_eras:
    for era_dir in args.task_file:
      era_name = os.path.basename(era_dir)
      era_files_dict[era_name] = []
      for root, dirs, files in os.walk(era_dir):
        task_files = [os.path.join(root, f) for f in files if f.endswith('.yaml')]
        era_files_dict[era_name].extend(task_files)
  else:
    if args.era is None:
      raise RuntimeError("Era is not specified.")
    era_files_dict[args.era] = args.task_file

  dataset_naming_rules = {}
  if args.dataset_naming_rules:
    with open(args.dataset_naming_rules) as f:
      dataset_naming_rules = yaml.safe_load(f)

  exceptions = {}
  if args.exceptions:
    with open(args.exceptions) as f:
      exceptions = yaml.safe_load(f)


  all_ok = check_consistency(era_files_dict, exceptions, dataset_naming_rules,
                             args.show_only_missing_with_candidates, args.search_for_missing_candidates)

  if all_ok:
    print("All checks are successfully passed.")
    exit_code = 0
  else:
    print("Some checks are failed.")
    exit_code = 1
  sys.exit(exit_code)
