#!/usr/bin/ksh

BASE_DIR=/teoco/sa_root_med06
. $BASE_DIR/project/env/env.ksh

cd $INTEGRATION_DIR/implementation/pm_data_di_dq
$HOME/.local/bin/pipenv run python pm_data_di_dq.py &
