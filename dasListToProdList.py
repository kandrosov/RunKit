import os
import re
import sys

if __name__ == "__main__":
  file_dir = os.path.dirname(os.path.abspath(__file__))
  sys.path.append(os.path.dirname(file_dir))
  __package__ = 'RunKit'

from .run_tools import natural_sort

if __name__ == "__main__":
  import argparse
  parser = argparse.ArgumentParser(description='create crabOverseer dataset yaml config from list of das samples.')
  parser.add_argument('--input', required=True, type=str, help="Input txt file with a dataset list")
  parser.add_argument('--output', required=True, type=str, help="Output yaml file")
  parser.add_argument('--extract-pattern', required=False, type=str, default='^/([^/]*)/',
                      help="regex pattern to extract user-friendly names")
  parser.add_argument('--rename-pattern', required=False, type=str, default=None,
                      help="regex pattern to rename extracted user-friendly names, in needed")
  args = parser.parse_args()

  if args.rename_pattern:
    rename_pattern, replace_pattern = args.rename_pattern.split('/')
  else:
    rename_pattern = None

  output_dict = {}
  with open(args.input, 'r') as f:
    for line in f.readlines():
      line = line.strip()
      if len(line) == 0 or line[0] == '#':
        continue
      match = re.match(args.extract_pattern, line)
      if not match:
        raise RuntimeError(f"Failed to match {line} with {args.extract_pattern}")
      name = match.group(1)
      if rename_pattern:
        name = re.sub(rename_pattern, replace_pattern, name)
      if name in output_dict:
        raise RuntimeError(f"Duplicate name={name} for {line} and {output_dict[name]}")
      output_dict[name] = line

  keys = natural_sort(output_dict.keys())

  with open(args.output, 'w') as f:
    for key in keys:
      value = output_dict[key]
      print(f'{key}: {value}')
      f.write(f'{key}: {value}\n')
