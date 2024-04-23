# Crab wrapper.

import os
import sys
import yaml
import FWCore.ParameterSet.Config as cms
from FWCore.ParameterSet.VarParsing import VarParsing

print("Running nanoProdWrapper.py", file=sys.stderr)
print("Arguments: ", sys.argv, file=sys.stderr)

options = VarParsing('analysis')
options.register('sampleType', '', VarParsing.multiplicity.singleton, VarParsing.varType.string,
                 "Indicates the sample type: data or mc")
options.register('era', '', VarParsing.multiplicity.singleton, VarParsing.varType.string,
                 "Indicates era: Run2_2016_HIPM, Run2_2016, Run2_2017, Run2_2018, Run3_2022, Run3_2022EE, Run3_2023, Run3_2023BPix")
options.register('mustProcessAllInputs', False, VarParsing.multiplicity.singleton, VarParsing.varType.bool,
                 "To sucessfully finish, all inputs must be processed.")
options.register('maxRuntime', 20, VarParsing.multiplicity.singleton, VarParsing.varType.int,
                 "Maximal expected job runtime in hours.")
options.register('maxFiles', -1, VarParsing.multiplicity.singleton, VarParsing.varType.int,
                 "Maximal number of files to process.")
options.register('recoveryIndex', -1, VarParsing.multiplicity.singleton, VarParsing.varType.int,
                 "If task recovery index >= 0, it will be used as a suffix in output file names.")
options.register('customise', '', VarParsing.multiplicity.singleton, VarParsing.varType.string,
                 "Production customization code (if any)")
options.register('customiseCmds', '', VarParsing.multiplicity.singleton, VarParsing.varType.string,
                 "Production customization commands (if any)")
options.register('writePSet', False, VarParsing.multiplicity.singleton, VarParsing.varType.bool,
                 "Dump configuration into PSet.py.")
options.register('copyInputsToLocal', True, VarParsing.multiplicity.singleton, VarParsing.varType.bool,
                 "Copy inputs (one at the time) to a job working directory before processing them.")
options.register('inputDBS', 'global', VarParsing.multiplicity.singleton, VarParsing.varType.string,
                 "DBS instance")
options.register('inputPFNSprefix', '', VarParsing.multiplicity.singleton, VarParsing.varType.string,
                 "Custom pfns prefix for input files")
options.register('output', '', VarParsing.multiplicity.list, VarParsing.varType.string,
                 """Output descriptions. Possible formats:
                    file
                    file;output_pfn
                    file;output_pfn;skim_cfg;skim_setup
                    file;output_pfn;skim_cfg;skim_setup;skim_setup_failed
                 """)
options.register('datasetFiles', '', VarParsing.multiplicity.singleton, VarParsing.varType.string,
                 """Path to a JSON file with the dict of all files in the dataset.
                    It is used to assing file ids to the outputs.
                    If empty, indices of input files as specified in inputFiles are used.""")

options.parseArguments()

cond_mc = {
  'Run2_2016_HIPM': 'auto:run2_mc_pre_vfp',
  'Run2_2016': 'auto:run2_mc',
  'Run2_2017': 'auto:phase1_2017_realistic',
  'Run2_2018': 'auto:phase1_2018_realistic',
  'Run3_2022': 'auto:phase1_2022_realistic',
  'Run3_2022EE': 'auto:phase1_2022_realistic_postEE',
  'Run3_2023': 'auto:phase1_2023_realistic',
  'Run3_2023BPix': 'auto:phase1_2023_realistic_postBPix'
}

if options.era.startswith('Run2'):
  cond_data = 'auto:run2_data'
  era_str = options.era
  era_mod = ',run2_nanoAOD_106Xv2'
elif options.era.startswith('Run3'):
  cond_data_run3 = {
    'Run3_2022': 'auto:run3_data',
    'Run3_2023': 'auto:run3_data',
  }
  if options.sampleType == 'data':
    cond_data = cond_data_run3[options.era]
  era_str = 'Run3'
  era_mod = ',run3_nanoAOD_124'
else:
  raise RuntimeError(f'Unknown era = "{options.era}"')

if options.sampleType == 'data':
  cond = cond_data
elif options.sampleType == 'mc':
  cond = cond_mc[options.era]
else:
  raise RuntimeError(f'Unknown sample type = "{options.sampleType}"')

process = cms.Process('NanoProd')
process.source = cms.Source("PoolSource", fileNames = cms.untracked.vstring(options.inputFiles))
process.options = cms.untracked.PSet(wantSummary = cms.untracked.bool(False))
process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32(-1))
if options.maxEvents > 0:
  process.maxEvents.input = options.maxEvents

for output in options.output:
  output = output.split(';')
  if len(output) not in [1, 2, 4, 5]:
    raise RuntimeError(f'Invalid output format: {output}')
  while len(output) < 5:
    output.append('')
  file, output_pfn, skim_cfg, skim_setup, skim_setup_failed = output
  if len(file) == 0:
    raise RuntimeError(f'Empty output file name.')
  if len(skim_cfg) > 0:
    if len(skim_setup) == 0:
      raise RuntimeError(f'skimCfg={skim_cfg}, but skimSetup is not specified.')
    if os.path.isfile(skim_cfg):
      with open(skim_cfg, 'r') as f:
        skim_config = yaml.safe_load(f)
        if skim_setup not in skim_config:
          raise RuntimeError(f'Setup "{skim_setup}" not found in skimCfg={skim_cfg}.')
        if len(skim_setup_failed) > 0 and skim_setup not in skim_config:
          raise RuntimeError(f"Setup {skim_setup_failed} not found in skimCfg={skim_cfg}.")
  else:
    if len(skim_setup) > 0 or len(skim_setup_failed) > 0:
      raise RuntimeError(f"Skim setup can not be specified without a skim configuration file.")

process.exParams = cms.untracked.PSet(
  sampleType = cms.untracked.string(options.sampleType),
  era = cms.untracked.string(era_str + era_mod),
  cond = cms.untracked.string(cond),
  customisationFunction = cms.untracked.string(options.customise),
  customisationCommands = cms.untracked.string(options.customiseCmds),
  mustProcessAllInputs = cms.untracked.bool(options.mustProcessAllInputs),
  jobModule = cms.untracked.string('crabJob_nanoProd.py'),
  output = cms.untracked.vstring(options.output),
  datasetFiles = cms.untracked.string(options.datasetFiles),
  maxFiles = cms.untracked.int32(options.maxFiles),
  recoveryIndex = cms.untracked.int32(options.recoveryIndex),
  copyInputsToLocal = cms.untracked.bool(options.copyInputsToLocal),
  inputDBS = cms.untracked.string(options.inputDBS),
  inputPFNSprefix = cms.untracked.string(options.inputPFNSprefix),
)

if options.writePSet:
  with open('PSet.py', 'w') as f:
    print(process.dumpPython(), file=f)
