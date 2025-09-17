import os
import yaml
import FWCore.ParameterSet.Config as cms
from FWCore.ParameterSet.VarParsing import VarParsing

def defineCommonArguments(name='analysis'):
  options = VarParsing(name)
  options.register('mustProcessAllInputs', False, VarParsing.multiplicity.singleton, VarParsing.varType.bool,
                  "To sucessfully finish, all inputs must be processed.")
  options.register('keepIntermediateFiles', False, VarParsing.multiplicity.singleton, VarParsing.varType.bool,
                  "Do not delete temporary files.")
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
  options.register('processEachRunSeparately', False, VarParsing.multiplicity.singleton, VarParsing.varType.bool,
                  "Run a separate cmsRun instance for each run.")
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
  return options

def checkOutputs(outputs):
  for output in outputs:
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

def createProcess(process_name, options):
  process = cms.Process(process_name)
  process.source = cms.Source("PoolSource", fileNames = cms.untracked.vstring(options.inputFiles))
  process.options = cms.untracked.PSet(wantSummary = cms.untracked.bool(False))
  process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32(-1))
  if options.maxEvents > 0:
    process.maxEvents.input = options.maxEvents
  return process

def setCommonParametersFromOptions(pSet, options):
  pSet.customisationFunction = cms.untracked.string(options.customise)
  pSet.customisationCommands = cms.untracked.string(options.customiseCmds)
  pSet.mustProcessAllInputs = cms.untracked.bool(options.mustProcessAllInputs)
  pSet.keepIntermediateFiles = cms.untracked.bool(options.keepIntermediateFiles)
  pSet.output = cms.untracked.vstring(options.output)
  pSet.datasetFiles = cms.untracked.string(options.datasetFiles)
  pSet.maxFiles = cms.untracked.int32(options.maxFiles)
  pSet.recoveryIndex = cms.untracked.int32(options.recoveryIndex)
  pSet.copyInputsToLocal = cms.untracked.bool(options.copyInputsToLocal)
  pSet.inputDBS = cms.untracked.string(options.inputDBS)
  pSet.inputPFNSprefix = cms.untracked.string(options.inputPFNSprefix)
  pSet.processEachRunSeparately = cms.untracked.bool(options.processEachRunSeparately)

def writePSet(process, options, output_filename='PSet.py'):
  if options.writePSet:
    with open(output_filename, 'w') as f:
      print(process.dumpPython(), file=f)
