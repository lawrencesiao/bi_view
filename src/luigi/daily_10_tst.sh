#!/bin/bash

PYENV=bi
APPDIR=$(dirname $0)

TODAY=$(date "+%Y-%m-%d")
YESTERDAY=$(date --date='yesterday' "+%Y-%m-%d")


cd $APPDIR
source $HOME/.virtualenvs/$PYENV/bin/activate

PYTHONPATH='' luigi --module send_report_tasks DailyRegisterReport --date $YESTERDAY
PYTHONPATH='' luigi --module edm_tasks DailyImportRegisterEvent60
PYTHONPATH='' luigi --module edm_tasks DailyEdmSpartan
#PYTHONPATH='' luigi --module edm_tasks DailyEdmHuaHua --batch 1
#PYTHONPATH='' luigi --module edm_tasks DailyEdmHuaHua --batch 2
#PYTHONPATH='' luigi --module edm_tasks DailyEdmHuaHua --batch 3

PYTHONPATH='' luigi --module edm_tasks DailyEdmHuiSun --batch 1
PYTHONPATH='' luigi --module edm_tasks DailyEdmHuiSunDiscount --batch 2

python br_mapping_tasks.py

#PYTHONPATH='' luigi --module edm_tasks DailyEdmRun
