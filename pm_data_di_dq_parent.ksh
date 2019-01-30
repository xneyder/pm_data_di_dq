#!/usr/bin/ksh

. ./env.ksh

cd $INTEGRATION_DIR/implementation/pm_data_di_dq

$HOME/.local/bin/pipenv run python pm_data_di_dq.py &
