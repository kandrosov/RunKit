import json
import os
import sys

if __name__ == "__main__":
  file_dir = os.path.dirname(os.path.abspath(__file__))
  sys.path.append(os.path.dirname(file_dir))
  __package__ = 'RunKit'

from .grid_tools import run_dasgoclient

def getFileRunLumi(inputDataset, inputDBS='global', timeout=None, verbose=0):
  def getDasInfo(query, mode, verbose):
    output = run_dasgoclient(mode + ' ' + query, inputDBS=inputDBS, timeout=timeout, verbose=verbose)
    descs = []
    for desc in output:
      desc = desc.strip()
      if len(desc) > 0:
        if mode == 'file,run,lumi':
          split_desc = desc.split(' ')
          if len(split_desc) != 3:
            raise RuntimeError(f'Bad file,run,lumi format in "{desc}"')
          descs.append([split_desc[0], json.loads(split_desc[1]), json.loads(split_desc[2])])
        elif mode == 'run':
          descs.append(int(desc))
        else:
          raise RuntimeError(f'Unknown format "{format}"')
    return descs

  if verbose > 0:
    print(f'Gathering file->(run,lumi) correspondance for {inputDataset}...')
  baseQuery = f'dataset={inputDataset}'
  fileRunLumi = {}
  runList = sorted(getDasInfo(baseQuery, 'run', verbose))
  nRuns = len(runList)
  nRunsWidth = len(str(nRuns))
  if verbose > 1:
    print(f'Found {len(runList)} runs.')
  for run_idx, run in enumerate(runList):
    if verbose > 1:
      print(f'{run_idx+1:>{nRunsWidth}}/{nRuns} run={run}')
    runQuery = baseQuery + f' status=valid run={run}'
    runVerbose = verbose - 2 if verbose >= 2 else 0
    for runFile, runRuns, runLumis in getDasInfo(runQuery, 'file,run,lumi', runVerbose):
      if runFile not in fileRunLumi:
        fileRunLumi[runFile] = {}
      fileRunLumi[runFile][str(run)] = runLumis
  return fileRunLumi

if __name__ == '__main__':
  dataset = sys.argv[1]
  output = sys.argv[2]

  fileRunLumi = getFileRunLumi(dataset, verbose=2)
  with open(output, 'w') as f:
    json.dump(fileRunLumi, f, indent=2)
