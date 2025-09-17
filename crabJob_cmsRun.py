import os
import shutil
import sys

if len(__package__) == 0:
  file_dir = os.path.dirname(os.path.abspath(__file__))
  base_dir = os.path.dirname(file_dir)
  if base_dir not in sys.path:
    sys.path.append(base_dir)
  __package__ = os.path.split(file_dir)[-1]

from .run_tools import ps_call

def processFile(input_file, outputs, tmp_files, cmssw_report, cmd_line_args, cfg_params):
  run_cmsRun = True
  debug = len(cmd_line_args) > 0 and cmd_line_args[0] == 'DEBUG'
  if debug:
    if len(cmd_line_args) > 1:
      run_cmsRun = cmd_line_args[1] == 'True'
  assert(len(outputs) > 0)

  cmsRun_out = 'cmsRun_out.root'

  if run_cmsRun:
    cmsRunCfg = cfg_params.cmsRunCfg
    cmsRunOptions = cfg_params.cmsRunOptions.split(',') if len(cfg_params.cmsRunOptions) > 0 else []

    customise_commands = cfg_params.customisationCommands
    cfg_name = cmsRunCfg
    if len(customise_commands) > 0:
      cfg_name = f'{cmsRunCfg}_cmsRun.py'
      shutil.copy(cmsRunCfg, cfg_name)
      tmp_files.append(cfg_name)
      with open(cfg_name, 'a') as f:
        f.write('\n' + customise_commands)

    cmssw_cmd = [ 'cmsRun',  '-j', cmssw_report, cfg_name, f'inputFiles={input_file}', f'output={cmsRun_out}',
                  f'maxEvents={cfg_params.maxEvents}' ]
    cmssw_cmd.extend(cmsRunOptions)
    ps_call(cmssw_cmd, verbose=1)

  skim_tree_path = os.path.join(os.path.dirname(__file__), 'skim_tree.py')
  for output in outputs:
    if len(output.get('skim_cfg', '')) > 0:
      cmd_line = ['python3', '-u', skim_tree_path, '--input', cmsRun_out, '--output', output['file_name'],
                '--config', output['skim_cfg'], '--setup', output['skim_setup'], '--skip-empty', '--verbose', '1']
      ps_call(cmd_line, verbose=1)

      if len(output.get('skim_setup_failed', '')) > 0:
        cmd_line = ['python3', '-u', skim_tree_path, '--input', cmsRun_out, '--output', output['file_name'],
                    '--config', output['skim_cfg'], '--setup', output['skim_setup_failed'],
                    '--skip-empty', '--update-output', '--verbose', '1']
        ps_call(cmd_line, verbose=1)
    else:
      shutil.copy(cmsRun_out, output['file_name'])
