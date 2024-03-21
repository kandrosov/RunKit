import json
import sys

def convertFileRunLumiToRunLumiRanges(fileRunLumiFile):
  with open(fileRunLumiFile, 'r') as f:
    fileRunLumi = json.load(f)
  runLumi = {}
  for file, runLumis in fileRunLumi.items():
    for run, lumis in runLumis.items():
      if run not in runLumi:
        runLumi[run] = set()
      runLumi[run].update(lumis)
  runLumiRanges = {}
  for run, lumis in runLumi.items():
    ranges = []
    current_range = None
    for lumi in sorted(lumis):
      if current_range is None:
        current_range = [lumi, lumi]
      else:
        if lumi == current_range[1] + 1:
          current_range[1] = lumi
        else:
          ranges.append(current_range)
          current_range = [lumi, lumi]
    if current_range is not None:
      ranges.append(current_range)
    runLumiRanges[run] = ranges
  return runLumiRanges

if __name__ == '__main__':
  input = sys.argv[1]
  output = sys.argv[2]

  runLumiRanges = convertFileRunLumiToRunLumiRanges(input)
  with open(output, 'w') as f:
    json.dump(runLumiRanges, f)
