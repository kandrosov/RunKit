import datetime
import importlib.util
import json
import os
import shutil
import sys
import traceback

if __name__ == "__main__":
  file_dir = os.path.dirname(os.path.abspath(__file__))
  base_dir = os.path.dirname(file_dir)
  if base_dir not in sys.path:
    sys.path.append(base_dir)
  __package__ = os.path.split(file_dir)[-1]

from .run_tools import PsCallError
from .grid_tools import gfal_exists, gfal_copy_safe, gfal_check_write, copy_remote_file, get_voms_proxy_token, GfalError

_error_msg_fmt = '''
<FrameworkError ExitStatus="{}" Type="Fatal error" >
<![CDATA[
{}
]]>
</FrameworkError>'''

_job_report_fmt = '''
<FrameworkJobReport>
<ReadBranches>
</ReadBranches>
<PerformanceReport>
  <PerformanceSummary Metric="StorageStatistics">
    <Metric Name="Parameter-untracked-bool-enabled" Value="true"/>
    <Metric Name="Parameter-untracked-bool-stats" Value="true"/>
    <Metric Name="Parameter-untracked-string-cacheHint" Value="application-only"/>
    <Metric Name="Parameter-untracked-string-readHint" Value="auto-detect"/>
    <Metric Name="ROOT-tfile-read-totalMegabytes" Value="0"/>
    <Metric Name="ROOT-tfile-write-totalMegabytes" Value="0"/>
  </PerformanceSummary>
</PerformanceReport>

<GeneratorInfo>
</GeneratorInfo>
{}
</FrameworkJobReport>
'''

_cmssw_report_name = 'cmssw_report'
_cmssw_report_ext = '.xml'
_cmssw_report = _cmssw_report_name + _cmssw_report_ext
_final_report = 'FrameworkJobReport.xml'
_tmp_report = _final_report + '.tmp'

def make_job_report(exit_code, exit_message=''):
  if exit_code == 0:
    error_msg = ''
  else:
    error_msg = _error_msg_fmt.format(exit_code, exit_message)
  report_str = _job_report_fmt.format(error_msg)
  with open(_tmp_report, 'w') as f:
    f.write(report_str)
  shutil.move(_tmp_report, _final_report)

def exit(exit_code, exit_message=''):
  if exit_code == 0 and os.path.exists(_cmssw_report):
    shutil.move(_cmssw_report, _final_report)
  else:
    make_job_report(exit_code, exit_message)
  if exit_code != 0:
    sys_exit = exit_code if exit_code >= 0 and exit_code <= 255 else 1
    sys.exit(sys_exit)

def getFilePath(file):
  if os.path.exists(file):
    return file
  file_name = os.path.basename(file)
  if os.path.exists(file_name):
    return file_name
  raise RuntimeError(f"Unable to find {file}")

def load(module_file):
  module_path = module_file
  if not os.path.exists(module_path):
    module_path = os.path.join(os.path.dirname(__file__), module_file)
    if not os.path.exists(module_path):
      module_path = os.path.join(os.getenv("CMSSW_BASE"), 'src', module_file)
      if not os.path.exists(module_path):
        raise RuntimeError(f"Cannot find path to {module_file}.")

  module_name, module_ext = os.path.splitext(module_file)
  spec = importlib.util.spec_from_file_location(module_name, module_path)
  module = importlib.util.module_from_spec(spec)
  sys.modules[module_name] = module
  spec.loader.exec_module(module)
  return module

def convertParams(cfg_params):
  class Params: pass
  params = Params()
  for param_name, param_value in cfg_params.parameters_().items():
    setattr(params, param_name, param_value.value())
  return params

def processFile(jobModule, file_id, input_file, outputs, cmd_line_args, params, voms_token):
  cmssw_report = f'{_cmssw_report_name}_{file_id}{_cmssw_report_ext}'
  result = False
  unable_to_write_output = False
  tmp_files = []
  exception = None
  need_to_run = False
  for output in outputs:
    if 'remote_path' in output:
      if not gfal_exists(output['remote_path'], voms_token):
        need_to_run = True
        can_write, exception = gfal_check_write(output['remote_path'], return_exception=True,
                                                voms_token=voms_token, verbose=1)
        if not can_write:
          print(f"Check write failed for {input_file} into {output['remote_path']}.")
          return False, True, exception
    else:
      need_to_run = True

  if not need_to_run:
    print(f'All outputs already exist for {input_file}. Skip processing.')
    return True, False, None

  try:
    if input_file.startswith('file:') or not params.copyInputsToLocal:
      module_input_file = input_file
      local_file = None
    else:
      local_file = f'input_{file_id}.root'
      tmp_files.append(local_file)
      copy_remote_file(input_file, local_file, inputDBS=params.inputDBS, custom_pfns_prefix=params.inputPFNSprefix, verbose=1)
      module_input_file = f'file:{local_file}'
    jobModule.processFile(module_input_file, outputs, tmp_files, cmssw_report, cmd_line_args, params)
    unable_to_write_output = True
    for output in outputs:
      if 'remote_path' in output:
        gfal_copy_safe(output['file_name'], output['remote_path'], voms_token=voms_token, verbose=1)
    unable_to_write_output = False
    result = True
  except (GfalError, PsCallError, Exception) as e:
    print(traceback.format_exc())
    exception = e
  if os.path.exists(cmssw_report) and (not os.path.exists(_cmssw_report) or result):
    shutil.move(cmssw_report, _cmssw_report)
  if not hasattr(params, 'keepIntermediateFiles') or not params.keepIntermediateFiles:
    for file in tmp_files:
      if os.path.exists(file):
        os.remove(file)
  for output in outputs:
    if len(output['output_pfn']) > 0 and os.path.exists(output['file_name']):
      os.remove(output['file_name'])
  return result, unable_to_write_output, exception

def runJob(cmd_line_args):
  pset_path = 'PSet.py'
  if not os.path.exists(pset_path):
    pset_path = os.path.join(os.getenv("CMSSW_BASE"), 'src', 'PSet.py')
  if not os.path.exists(pset_path):
    raise RuntimeError("Cannot find path to PSet.py.")

  PSet = load(pset_path)
  cfg_params = convertParams(PSet.process.exParams)
  cfg_params.maxEvents = PSet.process.maxEvents.input.value()
  jobModule = load(cfg_params.jobModule)
  recoveryIndex = cfg_params.recoveryIndex

  outputs = []
  for output in cfg_params.output:
    output = output.split(';')
    if len(output) not in [1, 2, 4, 5]:
      raise RuntimeError(f'Invalid output format: {output}')
    while len(output) < 5:
      output.append('')
    output_desc = {}
    for i, key in enumerate(['file', 'output_pfn', 'skim_cfg', 'skim_setup', 'skim_setup_failed']):
      if len(output) >= i + 1 and len(output[i]) > 0:
        output_desc[key] = output[i]
    if 'file' not in output_desc:
      raise RuntimeError(f'Empty output file name.')
    outputs.append(output_desc)

  voms_token = get_voms_proxy_token()

  if len(cfg_params.datasetFiles) > 0:
    with open(cfg_params.datasetFiles, 'r') as f:
      datasetFiles = json.load(f)
  else:
    datasetFiles = None

  print(f'datasetFiles: {cfg_params.datasetFiles}')
  has_at_least_one_success = False
  for file_index, file in enumerate(list(PSet.process.source.fileNames)):
    if cfg_params.maxFiles > 0 and file_index >= cfg_params.maxFiles:
      break
    file_id = datasetFiles[file] if datasetFiles else file_index
    for output in outputs:
      outputFileBase, outputExt = os.path.splitext(output['file'])
      if recoveryIndex < 0:
        output['file_name'] = f'{outputFileBase}_{file_id}{outputExt}'
      else:
        output['file_name'] = f'{outputFileBase}_{file_id}_{recoveryIndex}{outputExt}'
      if len(output['output_pfn']) > 0:
        output['remote_path'] = os.path.join(output['output_pfn'], output['file_name'])
    result, unable_to_write_output, exception = processFile(jobModule, file_id, file, outputs, cmd_line_args,
                                                            cfg_params, voms_token)
    if result:
      has_at_least_one_success = True
    else:
      print(f"Failed to process {file}")
      if cfg_params.mustProcessAllInputs:
        raise exception
    if unable_to_write_output:
      print(f"Unable to write output for {file}. Stop processing.")
      raise exception

  if not has_at_least_one_success:
    raise RuntimeError("Processing has failed for all input files.")

if __name__ == "__main__":
  try:
    runJob(sys.argv[1:])
    exit(0)
  except PsCallError as e:
    print(traceback.format_exc())
    exit(e.return_code, str(e))
  except Exception as e:
    print(traceback.format_exc())
    exit(666, str(e))
  except:
    print(traceback.format_exc())
    exit(666, 'Unexpected error')
