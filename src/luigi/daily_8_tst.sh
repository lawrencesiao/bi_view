#!/bin/bash

PYENV=bi
APPDIR=$(dirname $0)

TODAY=$(date "+%Y-%m-%d")

cd $APPDIR
source $HOME/.virtualenvs/$PYENV/bin/activate

PYTHONPATH='' luigi --module edm_tasks DailyTSDiscount --batch 1
PYTHONPATH='' luigi --module edm_tasks DailyTSDiscount --batch 2
PYTHONPATH='' luigi --module edm_tasks DailyTSDiscount --batch 3


