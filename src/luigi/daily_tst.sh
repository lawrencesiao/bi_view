#!/bin/bash

PYENV=bi
APPDIR=$(dirname $0)

TODAY=$(date "+%Y-%m-%d")
YESTERDAY=$(date --date='yesterday' "+%Y-%m-%d")

cd $APPDIR
source $HOME/.virtualenvs/$PYENV/bin/activate

PYTHONPATH='' luigi --module register_tasks DailyRegister --date $TODAY
PYTHONPATH='' luigi --module sales_tasks DailySales --date $TODAY
PYTHONPATH='' luigi --module item_tasks DailyItem --date $TODAY
PYTHONPATH='' luigi --module action_tasks DailyAction --date $TODAY
#PYTHONPATH='' luigi --module auo_tasks DailyRunKeeperGet

## PYTHONPATH='' luigi --module send_report_tasks DailyRegisterReport --date $TODAY
