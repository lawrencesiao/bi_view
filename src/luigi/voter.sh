#!/bin/bash

PYENV=bi
APPDIR=$(dirname $0)

TODAY=$(date "+%Y-%m-%d")

cd $APPDIR
source $HOME/.virtualenvs/$PYENV/bin/activate

python /home/ec2-user/projects/bi/src/luigi/voter_niceplay_once.py 