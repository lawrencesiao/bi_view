#!/bin/bash

PYENV=bi
APPDIR=$(dirname $0)

cd $APPDIR
source $HOME/.virtualenvs/$PYENV/bin/activate

PYTHONPATH='' luigi --module edm_tasks DailyLin32Music --batch 1
PYTHONPATH='' luigi --module edm_tasks DailyItriSmart --batch 1