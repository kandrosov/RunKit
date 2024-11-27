import copy
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

def getLumiList(file):
  from DataFormats.FWLite import Lumis
  from FWCore.PythonUtilities.LumiList import LumiList

  runsLumisDict = {}
  lumis = Lumis(file)
  for lumi in lumis:
    run = lumi.aux().run()
    if run not in runsLumisDict:
      runsLumisDict[run] = []
    runsLumisDict[run].append(lumi.aux().id().luminosityBlock())
  return LumiList(runsAndLumis=runsLumisDict)

def createLumiMasksPerRun(input, output):
  lumi_list = getLumiList(input)
  mask_dict = {}
  for run in lumi_list.getCompactList().keys():
    lumi_list_per_run = copy.deepcopy(lumi_list)
    lumi_list_per_run.selectRuns([run])
    output_run = f'{output}_{run}.json'
    mask_dict[run] = output_run
    lumi_list_per_run.writeJSON(output_run)
  return mask_dict

def runCmssw(input_file, cmsRun_out, cfg_params, cmssw_report, cmsDriver_py, lumi_mask=None, run_cmsDriver=True):
  if run_cmsDriver:
    n_threads = 1
    cmsDrive_cmd = [
      'cmsDriver.py', 'nano', '--filein', input_file, '--fileout', f'file:{cmsRun_out}',
      '--eventcontent', 'NANOAODSIM', '--datatier', 'NANOAODSIM', '--step', 'NANO', '--nThreads', f'{n_threads}',
      f'--{cfg_params.sampleType}', '--conditions', cfg_params.cond,
      '--era', f"{cfg_params.era}", '-n', f'{cfg_params.maxEvents}', '--no_exec',
    ]

    if lumi_mask is not None:
      cmsDrive_cmd.extend(['--lumiToProcess', lumi_mask])

    customise = cfg_params.customisationFunction
    if len(customise) > 0:
      print(f'Using customisation function "{customise}"')
      customise_path, customise_fn = customise.split('.')
      customise_path = customise_path.split('/')
      if len(customise_path) == 3:
        customise_dir = os.path.join(os.environ['CMSSW_BASE'], 'src', customise_path[0], customise_path[1], 'python')
        customise_file = customise_path[2] + '.py'
        customise_file_path = os.path.join(customise_dir, customise_file)
        if not os.path.exists(customise_file_path):
          sandbox_file = os.path.join(os.path.dirname(__file__), customise_path[2] + '.py')
          if os.path.exists(sandbox_file):
            os.makedirs(customise_dir, exist_ok=True)
            shutil.copy(sandbox_file, customise_file_path)
      cmsDrive_cmd.extend(['--customise', customise])

  customise_commands = cfg_params.customisationCommands
  if len(customise_commands) > 0:
    cmsDrive_cmd.extend(['--customise_commands', customise_commands])

  cmssw_cmd = [ 'cmsRun',  '-j', cmssw_report, cmsDriver_py ]

  ps_call(cmsDrive_cmd, verbose=1)
  ps_call(cmssw_cmd, verbose=1)

def processFile(input_file, outputs, tmp_files, cmssw_report, cmd_line_args, cfg_params):
  run_cmsDriver = True
  debug = len(cmd_line_args) > 0 and cmd_line_args[0] == 'DEBUG'
  if debug:
    if len(cmd_line_args) > 1:
      run_cmsDriver = cmd_line_args[1] == 'True'
  assert(len(outputs) > 0)

  cmsRun_out = 'cmsRun_out.root'
  cmsDriver_py = 'nano_NANO.py'
  tmp_files.append(cmsDriver_py)
  if os.path.exists(cmsRun_out):
    os.remove(cmsRun_out)

  processEachRunSeparately = cfg_params.processEachRunSeparately
  if processEachRunSeparately:
    lumi_masks = createLumiMasksPerRun(input_file, 'lumi_mask')
    for mask in lumi_masks.values():
      tmp_files.append(mask)
    if len(lumi_masks) <= 1:
      processEachRunSeparately = False

  if processEachRunSeparately:
    cmsRun_outputs = []
    for run, mask in sorted(lumi_masks.items()):
      cmsRun_out_run = f'cmsRun_out_{run}.root'
      tmp_files.append(cmsRun_out_run)
      cmsRun_outputs.append(cmsRun_out_run)
      runCmssw(input_file, cmsRun_out_run, cfg_params, cmssw_report, cmsDriver_py, run_cmsDriver=run_cmsDriver,
               lumi_mask=mask)
    haddnano_path = os.path.join(os.path.dirname(__file__), 'haddnano.py')
    hadd_cmd = [ 'python3', '-u', haddnano_path, cmsRun_out ] + cmsRun_outputs
    ps_call(hadd_cmd, verbose=1)
  else:
    runCmssw(input_file, cmsRun_out, cfg_params, cmssw_report, cmsDriver_py, run_cmsDriver=run_cmsDriver)

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
