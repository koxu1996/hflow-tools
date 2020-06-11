#!/usr/bin/env bash
set -e

PACKAGE="hflow-tools"
NPM_ROOT=$(npm root -g)

#pushd . > /dev/null
#cd ${NPM_ROOT}/${PACKAGE}/hflow-viz-trace

#PYTHON="$(pipenv --venv)/bin/python"
#if [ ! -f $PYTHON ]; then
#  # fallback to windows path
#  PYTHON="$(pipenv --venv)/Scripts/python.exe"
#fi

#popd  > /dev/null

#$PYTHON ${NPM_ROOT}/${PACKAGE}/hflow-viz-trace/main.py $@

python ${NPM_ROOT}/${PACKAGE}/hflow-viz-trace/main.py $@
