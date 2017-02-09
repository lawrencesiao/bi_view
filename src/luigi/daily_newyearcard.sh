#!/bin/bash

PYENV=bi
APPDIR=$(dirname $0)

TODAY=$(date "+%Y-%m-%d")
YESTERDAY=$(date --date='yesterday' "+%Y-%m-%d")

cd $APPDIR
source $HOME/.virtualenvs/$PYENV/bin/activate

PYTHONPATH='' luigi --module edm_tasks DailyNewYear2017 --batch 1
