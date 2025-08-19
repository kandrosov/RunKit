import glob
import json
import os
import re
import sys
import tempfile
import yaml

if __name__ == "__main__":
  file_dir = os.path.dirname(os.path.abspath(__file__))
  sys.path.append(os.path.dirname(file_dir))
  __package__ = 'RunKit'

from .run_tools import print_ts, ps_call, natural_sort
from .grid_tools import get_voms_proxy_info, gfal_copy_safe, lfn_to_pfn, gfal_exists, gfal_rm

def load_config(cfg_file, era):
  with open(cfg_file) as f:
    full_cfg = yaml.safe_load(f)
  raw_cfg = {}
  for key in ['common', era]:
    if key in full_cfg:
      raw_cfg.update(full_cfg[key])
  cfg = {}
  os.environ['ERA'] = era

  def process_value(value):
    output = []
    value = os.path.expandvars(value)
    if value.startswith('/') and '*' in value:
      for sub_value in glob.glob(value):
        output.append(sub_value)
    elif len(value) > 0:
      if value.startswith('T'):
        server, lfn = value.split(':')
        value = lfn_to_pfn(server, lfn)
      output.append(value)
    return output

  for key in [ 'task_files', 'outputs', 'config_files', 'files_to_ignore' ]:
    values = set()
    for value in raw_cfg.get(key, []):
      values.update(process_value(value))
    cfg[key] = sorted(values)
  for key in [ 'storage', 'info', 'title' ]:
    if key not in raw_cfg:
      raise RuntimeError(f'"{key}" not found in "{cfg_file}" for era="{era}".')
    value = process_value(raw_cfg[key])
    if len(value) != 1:
      raise RuntimeError(f'Unable to extract "{key}" from "{cfg_file}" for era="{era}".')
    cfg[key] = value[0]
  for key in [ 'prod_flavours' ]:
    if key not in raw_cfg:
      raise RuntimeError(f'"{key}" not found in "{cfg_file}" for era="{era}".')
    cfg[key] = raw_cfg[key]

  tasks = {}
  datasets = {}
  for task_file in cfg['task_files']:
    file_name = os.path.split(task_file)[1]
    if file_name in cfg['files_to_ignore']: continue
    with open(task_file) as f:
      task_yaml = yaml.safe_load(f)
    for task_name, task_desc in task_yaml.items():
      if task_name == 'config': continue
      customTask = type(task_desc) == dict
      if customTask:
        if 'inputDataset' not in task_desc:
          raise RuntimeError(f'Task "{task_name}" in {task_file} does not have "inputDataset" field.')
        inputDataset = task_desc['inputDataset']
      else:
        inputDataset = task_desc
      if task_name not in tasks:
        tasks[task_name] = []
      tasks[task_name].append({ 'file': task_file, 'dataset': inputDataset })
      if inputDataset not in datasets:
        datasets[inputDataset] = []
      datasets[inputDataset].append({ 'file': task_file, 'name': task_name })
  all_ok = True
  for task_name, task_entries in tasks.items():
    if len(task_entries) != 1:
      print(f'ERROR: task "{task_name}" is defined multiple files:')
      for entry in task_entries:
        print(f'  file={entry["file"]} dataset={entry["dataset"]}')
      all_ok = False
  for dataset, dataset_entries in datasets.items():
    if len(dataset_entries) != 1:
      print(f'ERROR: input dataset "{dataset}" is defined in multiple tasks:')
      for entry in dataset_entries:
        print(f'  file={entry["file"]} task={entry["name"]}')
      all_ok = False

  if not all_ok:
    raise RuntimeError('Production configuration is not consistent')

  cfg['datasets'] = { dataset: dataset_entries[0]['name'] for dataset, dataset_entries in datasets.items() }
  cfg['tasks'] = {}
  for task_name, task_entries in tasks.items():
    task_entry = { 'dataset': task_entries[0]['dataset'], 'flavours': {} }
    for flavour_entry in cfg['prod_flavours']:
      if re.match(flavour_entry['task_name_pattern'], task_name):
        for flavour, prod_report in flavour_entry['flavours'].items():
          task_entry['flavours'][flavour] = { 'prod_report': prod_report }
        break
    if len(task_entry['flavours']) == 0:
      raise RuntimeError(f'No production flavours found for task "{task_name}".')
    for flavour in task_entry['flavours'].keys():
      if len(task_entry['flavours']) == 1:
        full_name = task_name
      else:
        full_name = f'{task_name}-{flavour}'
      task_entry['flavours'][flavour]['full_name'] = full_name
    cfg['tasks'][task_name] = task_entry

  return cfg

def copy_info_files(info_path, files_to_copy, voms_token):
  for entry in files_to_copy:
    if type(entry) == list:
      file_in, file_out = entry
    else:
      file_in = entry
      file_out = entry
    if file_in.startswith('/'):
      file_out = os.path.split(file_in)[1]
    else:
      file_in = os.path.join(os.environ['ANALYSIS_PATH'], 'RunKit', 'html', file_in)
    file_out = os.path.join(info_path, file_out)
    print(f'{file_in} -> {file_out}')
    gfal_copy_safe(file_in, file_out, voms_token=voms_token, verbose=0)

def update_eras_info(cfg, era, tmp_dir, voms_token, dry_run):
  eras_json_path = os.path.join(cfg['info'], 'eras.json')
  eras_tmp = os.path.join(tmp_dir, 'eras.json')
  datasets_tmp = os.path.join(tmp_dir, 'datasets.json')
  has_updates = False
  if gfal_exists(eras_json_path, voms_token=voms_token):
    print('Loading existing eras info...')
    gfal_copy_safe(eras_json_path, eras_tmp, voms_token=voms_token, verbose=0)
    with open(eras_tmp) as f:
      eras_info = json.load(f)
  else:
    config_file_names = [ os.path.split(f)[1] for f in cfg['config_files'] ]
    eras_info = { 'title': cfg['title'], 'eras': [], 'config_files': config_file_names }
    has_updates = True
  era_found = False
  for era_info in eras_info['eras']:
    if era_info['era'] == era:
      era_found = True
      break
  if not era_found:
    print(f'Adding new era {era}...')
    eras_info['eras'].append({
      'era': era,
      'location': os.path.join(cfg['storage'], era),
      'info': os.path.join(era, 'index.html'),
    })
    eras_info['eras'] = sorted(eras_info['eras'], key=lambda x: x['era'])
    has_updates = True
  if has_updates and not dry_run:
    with open(eras_tmp, 'w') as f:
      json.dump(eras_info, f, indent=2)
    files_to_copy = [ [ 'index_eras.html', 'index.html'], 'jquery.min.js', 'jsgrid.css',
                      'jsgrid.min.js', 'jsgrid-theme.css' ]
    files_to_copy.extend(cfg['config_files'])
    files_to_copy.append(eras_tmp)
    copy_info_files(cfg['info'], files_to_copy, voms_token)

    era_datasets = {
      'title': f'{cfg["title"]}: {era} datasets',
      'datasets': [],
    }
    with open(datasets_tmp, 'w') as f:
      json.dump(era_datasets, f, indent=2)
    files_to_copy = [ [ 'index_era.html', 'index.html'], datasets_tmp ]
    copy_info_files(os.path.join(cfg['info'], era), files_to_copy, voms_token)

def find_dataset_report(cfg, era, task_name, prod_report_file, voms_token):
  task_report_path = os.path.join(cfg['storage'], era, task_name, prod_report_file)
  if gfal_exists(task_report_path, voms_token=voms_token):
    return task_report_path, True, None
  for output in cfg['outputs']:
    task_report_path = os.path.join(output, task_name, prod_report_file)
    if gfal_exists(task_report_path, voms_token=voms_token):
      return task_report_path, False, output
  return None, False, None

def check_consistency(cfg, datasets_info, not_defined_datasets):
  all_ok = True
  datasets_by_name = {}
  datasets_by_path = {}
  for dataset in datasets_info['datasets']:
    name = dataset['name']
    flavour = dataset.get('flavour', '')
    path = dataset['dataset']
    if name not in datasets_by_name:
      datasets_by_name[name] = set()
    datasets_by_name[name].add(path)
    if path not in datasets_by_path:
      datasets_by_path[path] = {}
    if name not in datasets_by_path[path]:
      datasets_by_path[path][name] = []
    datasets_by_path[path][name].append(flavour)
  for dataset_name, paths in datasets_by_name.items():
    if len(paths) != 1:
      paths_str = ', '.join(paths)
      print(f'ERROR: dataset "{dataset_name}" is used in multiple paths: {paths_str}')
      all_ok = False
  for dataset_path, names in datasets_by_path.items():
    if len(names) != 1:
      names_str = ', '.join(names.keys())
      print(f'ERROR: path "{dataset_path}" is refered by multiple names: {names_str}')
      all_ok = False
  if not all_ok:
    return False

  datasets_by_name = { name: next(iter(paths)) for name, paths in datasets_by_name.items() }
  for name, dataset in datasets_by_name.items():
    name_found = name in cfg['tasks']
    if name_found and dataset == cfg['tasks'][name]['dataset']:
      continue
    all_ok = False
    dataset_found = dataset in cfg['datasets']
    if dataset_found:
      print(f'ERROR: deployed name != production name for dataset={dataset}:')
      print(f' {name} != {cfg["datasets"][dataset]}')
    if name_found:
      print(f'ERROR: deployed dataset != production dataset for name={name}:')
      print(f' {dataset} != {cfg["tasks"][name]["dataset"]}')
    if not name_found and not dataset_found:
      print(f'ERROR: deployed name={name} dataset={dataset}" is not defined in the production configuration.')
      not_defined_datasets.add(name)

  return all_ok

def deploy_prod_results(cfg_file, era, dry_run=False, check_only=False, output_missing=None, remove_not_defined=False):
  cfg = load_config(cfg_file, era)
  if len(cfg['datasets']) == 0:
    raise RuntimeError(f'No datasets are found for era="{era}".')

  voms_token = get_voms_proxy_info()['path']
  tmp_dir = tempfile.mkdtemp(dir=os.environ['TMPDIR'])
  update_eras_info(cfg, era, tmp_dir, voms_token, dry_run)

  datasets_json_path = os.path.join(cfg['info'], era, 'datasets.json')
  datasets_tmp = os.path.join(tmp_dir, 'datasets.json')
  if not gfal_exists(datasets_json_path, voms_token=voms_token):
    raise RuntimeError(f'File {datasets_json_path} does not exist.')
  print('Loading existing datasets info...')
  gfal_copy_safe(datasets_json_path, datasets_tmp, voms_token=voms_token, verbose=0)
  with open(datasets_tmp) as f:
    datasets_info = json.load(f)

  not_defined_datasets = set()
  if not check_consistency(cfg, datasets_info, not_defined_datasets):
    if remove_not_defined and len(not_defined_datasets) > 0:
      dry_run_str = ' (dry run)' if dry_run else ''

      print(f'Removing {len(not_defined_datasets)} not defined datasets {dry_run_str}:'
            f'{" ".join(not_defined_datasets)}')
      files_to_remove = []
      new_datasets = []
      for dataset in datasets_info['datasets']:
        if dataset['name'] in not_defined_datasets:
          full_name = dataset['full_name']
          files_to_remove.extend([ f'{full_name}_size.html', f'{full_name}_doc.html', f'{full_name}_report.json' ])
        else:
          new_datasets.append(dataset)
      print(f'Replacing dataset info{dry_run_str}: old datasets count = {len(datasets_info["datasets"])},'
            f' new datasets count = {len(new_datasets)}')
      datasets_info['datasets'] = new_datasets
      if not dry_run:
        with open(datasets_tmp, 'w') as f:
          json.dump(datasets_info, f, indent=2)
        copy_info_files(os.path.join(cfg['info'], era), [ datasets_tmp ], voms_token)
      for file in files_to_remove:
        file_path = os.path.join(cfg['info'], era, file)
        if gfal_exists(file_path, voms_token=voms_token):
          print(f'Removing {file_path}{dry_run_str}...')
          if not dry_run:
            gfal_rm(file_path, voms_token=voms_token, verbose=0)
        else:
          print(f'File {file_path} does not exist, skipping removal.')
    else:
      raise RuntimeError(f'Inconsistent datasets info in {datasets_json_path}.')
    return
  if check_only:
    return

  missing_tasks = set()
  task_names = natural_sort(cfg['tasks'].keys())
  print('Checking datasets availability...')
  for task_name in task_names:
    task_dataset = cfg['tasks'][task_name]['dataset']
    for flavour, flavour_entry in cfg['tasks'][task_name]['flavours'].items():
      full_name = flavour_entry['full_name']
      dataset_exists = False
      for dataset in datasets_info['datasets']:
        if dataset['full_name'] == full_name:
          dataset_exists = True
          break
      if dataset_exists:
        continue
      task_report_path, from_storage, output_node = find_dataset_report(cfg, era, task_name,
                                                                        flavour_entry['prod_report'], voms_token)
      if task_report_path is None:
        missing_tasks.add(task_name)
        continue
      dry_run_str = ' (dry run)' if dry_run else ''
      print_ts(f'Adding new task {task_name} (flavour={flavour}) {dry_run_str}...')
      if dry_run:
        continue

      report_tmp = os.path.join(tmp_dir, 'report.json')
      gfal_copy_safe(task_report_path, report_tmp, voms_token=voms_token, verbose=0)
      with open(report_tmp) as f:
        report = json.load(f)

      if report['inputDataset'] != task_dataset:
        raise RuntimeError(f'Inconsistent dataset definition for {task_name}: {report["dataset"]} != {task_dataset}')

      if not from_storage:
        for output_file in list(report['outputs'].keys()) + [ flavour_entry['prod_report'] ]:
          input_path = os.path.join(output_node, task_name, output_file)
          output_path = os.path.join(cfg['storage'], era, task_name, output_file)
          print_ts(f'{input_path} -> {output_path}')
          gfal_copy_safe(input_path, output_path, voms_token=voms_token, verbose=0)

      stats = { key: 0 for key in [ 'size', 'n_selected', 'n_not_selected', 'n_selected_original',
                                    'n_not_selected_original', 'size_original' ] }
      output_files = []
      for output_file, output_desc in report['outputs'].items():
        output_path = os.path.join(cfg['storage'], era, task_name, output_file)
        output_files.append(output_path)
        for stat_key in stats.keys():
          stats[stat_key] += output_desc[stat_key]

      size_report_tmp = os.path.join(tmp_dir, f'{full_name}_size.html')
      doc_report_tmp = os.path.join(tmp_dir, f'{full_name}_doc.html')
      json_report_tmp = os.path.join(tmp_dir, f'{full_name}_report.json')

      root_tmp = os.path.join(tmp_dir, f'{full_name}.root')
      print_ts(f'{output_files[0]} -> {root_tmp}')
      gfal_copy_safe(output_files[0], root_tmp, voms_token=voms_token, verbose=0)
      cmd= [ 'python', os.path.join(os.environ['ANALYSIS_PATH'], 'RunKit', 'inspectNanoFile.py'),
            '-j', json_report_tmp, '-s', size_report_tmp, '-d', doc_report_tmp, root_tmp ]
      ps_call(cmd, verbose=1)
      if os.path.exists(root_tmp):
        os.remove(root_tmp)

      dataset = { 'name': task_name, 'flavour': flavour, 'full_name': full_name, 'dataset': report['inputDataset'],
                  'n_files': len(output_files) }
      dataset.update(stats)

      print(json.dumps(dataset, indent=2))
      datasets_info['datasets'].append(dataset)
      datasets_info['datasets'] = sorted(datasets_info['datasets'], key=lambda x: x['full_name'])
      with open(datasets_tmp, 'w') as f:
        json.dump(datasets_info, f, indent=2)

      files_to_copy = [ size_report_tmp, doc_report_tmp, json_report_tmp, datasets_tmp ]
      copy_info_files(os.path.join(cfg['info'], era), files_to_copy, voms_token)
      print_ts(f'{task_name} added.')

  print(f'Total number of tasks: {len(cfg["tasks"])}')
  missing_tasks = natural_sort(missing_tasks)
  print(f'Missing {len(missing_tasks)} tasks: {", ".join(missing_tasks)}')
  if output_missing is not None and len(missing_tasks) > 0:
    with open(output_missing, 'w') as f:
      json.dump(missing_tasks, f, indent=2)

if __name__ == "__main__":
  import argparse
  parser = argparse.ArgumentParser(description='Deploy produced files in to the final destination.')
  parser.add_argument('--cfg', required=True, type=str, help="configuration file")
  parser.add_argument('--era', required=True, type=str, help="era to deploy")
  parser.add_argument('--output-missing', required=False, type=str, default=None,
                      help="file to store the list of missing datasets")
  parser.add_argument('--dry-run', action="store_true", help="Do not perform actions.")
  parser.add_argument('--check-only', action="store_true", help="Run only consistency checks.")
  parser.add_argument('--remove-not-defined', action="store_true",
                      help="Remove datasets that are not defined in the production configuration.")
  args = parser.parse_args()

  deploy_prod_results(args.cfg, args.era, dry_run=args.dry_run, check_only=args.check_only,
                      output_missing=args.output_missing, remove_not_defined=args.remove_not_defined)
