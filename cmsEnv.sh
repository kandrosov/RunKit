#!/bin/bash
CURRENT_DIR=$PWD
cd $DEFAULT_CMSSW_BASE/src
source /cvmfs/cms.cern.ch/cmsset_default.sh
eval $(scramv1 runtime -sh 2>/dev/null)
cd $CURRENT_DIR
"$@"