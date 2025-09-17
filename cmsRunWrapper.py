# Crab wrapper.

from wrapperBase import defineCommonArguments, setCommonParametersFromOptions, checkOutputs, createProcess, writePSet
import FWCore.ParameterSet.Config as cms
from FWCore.ParameterSet.VarParsing import VarParsing

options = defineCommonArguments()
options.register('cmsRunCfg', '', VarParsing.multiplicity.singleton, VarParsing.varType.string,
                "Path to the cmsRun python configuration file.")
options.register('cmsRunOptions', '', VarParsing.multiplicity.singleton, VarParsing.varType.string,
                "Comma separated list of options that should be passed to cmsRun.")
options.parseArguments()

checkOutputs(options.output)

process = createProcess('cmsRun', options)
process.exParams = cms.untracked.PSet(
  cmsRunCfg = cms.untracked.string(options.cmsRunCfg),
  cmsRunOptions = cms.untracked.string(options.cmsRunOptions),
  jobModule = cms.untracked.string('crabJob_cmsRun.py'),
)
setCommonParametersFromOptions(process.exParams, options)

writePSet(process, options)
