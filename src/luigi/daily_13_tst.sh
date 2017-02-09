#!/bin/bash

PYENV=bi
APPDIR=$(dirname $0)

cd $APPDIR
source $HOME/.virtualenvs/$PYENV/bin/activate

PYTHONPATH='' luigi --module edm_tasks DailyHccMarathon --batch 1

