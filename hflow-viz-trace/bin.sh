#!/usr/bin/env bash
set -e

PACKAGE="hflow-tools"
NPM_ROOT=$(npm root ${PACKAGE})

(
  cd ${NPM_ROOT}/${PACKAGE}/hflow-viz-trace
  pipenv run ./main.py $@
)
