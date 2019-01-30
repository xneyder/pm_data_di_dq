#!/usr/bin/ksh

#Get the script path
__REAL_SCRIPTDIR=$( cd -P -- "$(dirname -- "$(command -v -- "$0")")" && pwd -P )

cd $__REAL_SCRIPTDIR
. ./env.ksh

$HOME/.local/bin/pipenv run python pm_data_di_dq.py &
