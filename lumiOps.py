import os
import sys

if __name__ == "__main__":
  file_dir = os.path.dirname(os.path.abspath(__file__))
  sys.path.append(os.path.dirname(file_dir))
  __package__ = 'RunKit'

from .LumiList import LumiList

if __name__ == '__main__':
  lumi1 = LumiList(sys.argv[1])
  op = sys.argv[2].lower()
  lumi2 = LumiList(sys.argv[3])

  if op in [ '+', 'plus' ]:
    lumi_out = lumi1 + lumi2
  elif op in [ '-', 'minus' ]:
    lumi_out = lumi1 - lumi2
  elif op in [ '&', 'and' ]:
    lumi_out = lumi1 & lumi2
  elif op in [ '|', 'or' ]:
    lumi_out = lumi1 | lumi2
  else:
    raise RuntimeError(f'Unknown operator "{op}"')
  print(str(lumi_out))
