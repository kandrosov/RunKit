# Crab wrapper.

import FWCore.ParameterSet.Config as cms
from FWCore.ParameterSet.VarParsing import VarParsing

options = VarParsing('analysis')
options.register('sampleType', '', VarParsing.multiplicity.singleton, VarParsing.varType.string,
                 "Indicates the sample type: data or mc")
options.register('era', '', VarParsing.multiplicity.singleton, VarParsing.varType.string,
                 "Indicates era: Run2_2016_HIPM, Run2_2016, Run2_2017, Run2_2018")
options.register('skimCfg', '', VarParsing.multiplicity.singleton, VarParsing.varType.string,
                 "Skimming configuration in YAML format.")
options.register('skimSetup', '', VarParsing.multiplicity.singleton, VarParsing.varType.string,
                 "Name of the skim setup for passed events.")
options.register('skimSetupFailed', '', VarParsing.multiplicity.singleton, VarParsing.varType.string,
                 "Name of the skim setup for failed events.")
options.register('storeFailed', False, VarParsing.multiplicity.singleton, VarParsing.varType.bool,
                 "Store minimal information about events that failed selection.")
options.register('mustProcessAllInputs', False, VarParsing.multiplicity.singleton, VarParsing.varType.bool,
                 "To sucessfully finish, all inputs must be processed.")
options.register('createTar', True, VarParsing.multiplicity.singleton, VarParsing.varType.bool,
                 "Create a tar file with all outputs.")
options.register('maxRuntime', 20, VarParsing.multiplicity.singleton, VarParsing.varType.int,
                 "Maximal expected job runtime in hours.")
options.register('maxFiles', -1, VarParsing.multiplicity.singleton, VarParsing.varType.int,
                 "Maximal number of files to process.")
options.register('customise', '', VarParsing.multiplicity.singleton, VarParsing.varType.string,
                 "Production customization code (if any)")
options.register('customiseCmds', '', VarParsing.multiplicity.singleton, VarParsing.varType.string,
                 "Production customization commands (if any)")
options.register('writePSet', False, VarParsing.multiplicity.singleton, VarParsing.varType.bool,
                 "Dump configuration into PSet.py.")
options.register('copyInputsToLocal', True, VarParsing.multiplicity.singleton, VarParsing.varType.bool,
                 "Copy inputs (one at the time) to a job working directory before processing them.")
options.register('output', 'nano.root', VarParsing.multiplicity.singleton, VarParsing.varType.string,
                 "Name of the output file.")
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
  'Run3_2022': '126X_mcRun3_2022_realistic_v2',
  'Run3_2022EE': '126X_mcRun3_2022_realistic_postEE_v1',
}

if options.era.startswith('Run2'):
  cond_data = 'auto:run2_data'
  era_str = options.era
  era_mod = ',run2_nanoAOD_106Xv2'
elif options.era.startswith('Run3'):
  cond_data_run3 = {
    'Run3_2022CDE': '124X_dataRun3_v14',
    'Run3_2022FG': '124X_dataRun3_Prompt_v10',
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
process.exParams = cms.untracked.PSet(
  sampleType = cms.untracked.string(options.sampleType),
  era = cms.untracked.string(era_str + era_mod),
  cond = cms.untracked.string(cond),
  skimCfg = cms.untracked.string(options.skimCfg),
  skimSetup = cms.untracked.string(options.skimSetup),
  skimSetupFailed = cms.untracked.string(options.skimSetupFailed),
  storeFailed = cms.untracked.bool(options.storeFailed),
  customisationFunction = cms.untracked.string(options.customise),
  customisationCommands = cms.untracked.string(options.customiseCmds),
  createTar = cms.untracked.bool(options.createTar),
  mustProcessAllInputs = cms.untracked.bool(options.mustProcessAllInputs),
  maxRuntime = cms.untracked.int32(options.maxRuntime),
  jobModule = cms.untracked.string('crabJob_nanoProd.py'),
  output = cms.untracked.string(options.output),
  datasetFiles = cms.untracked.string(options.datasetFiles),
  maxFiles = cms.untracked.int32(options.maxFiles),
  copyInputsToLocal = cms.untracked.bool(options.copyInputsToLocal),
)

if options.writePSet:
  with open('PSet.py', 'w') as f:
    print(process.dumpPython(), file=f)