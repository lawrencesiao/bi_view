#!/bin/bash

PYENV=bi
APPDIR=$(dirname $0)

TODAY=$(date "+%Y-%m-%d")
YESTERDAY=$(date --date='yesterday' "+%Y-%m-%d")

cd $APPDIR
source $HOME/.virtualenvs/$PYENV/bin/activate

PYTHONPATH='' luigi --module action_tasks DailyAction --date $YESTERDAY
PYTHONPATH='' luigi --module profile_tasks DailyAddUser --date $YESTERDAY
#PYTHONPATH='' luigi --module profile_tasks DailyCreateProfile --date $YESTERDAY
PYTHONPATH='' luigi --module profile_tasks UpdateProfile --date $YESTERDAY