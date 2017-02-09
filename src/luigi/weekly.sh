#!/bin/bash
PYENV=bi
APPDIR=$(dirname $0)

WEEK=$(date --date='yesterday' "+%W")
#WEEK=$(date +%W)
YEAR=$(date --date='yesterday' +%Y)
#YEAR=$(date +%Y)
PERIOD=$YEAR-W$WEEK

cd $APPDIR
source $HOME/.virtualenvs/$PYENV/bin/activate


PYTHONPATH='' luigi --module register_tasks WeeklyRegister --interval $PERIOD
PYTHONPATH='' luigi --module sales_tasks WeeklySales --interval $PERIOD
PYTHONPATH='' luigi --module item_tasks WeeklyItem --interval $PERIOD
PYTHONPATH='' luigi --module sales_tasks WeeklySalesByItem --interval $PERIOD

