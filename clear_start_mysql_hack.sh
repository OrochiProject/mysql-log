#! /bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

function clear_sql {
  rm -rf /tmp/sql_log/
}

killall -9 mysqld_safe 2>/dev/null
killall -9 mysqld      2>/dev/null
clear_sql

$SCRIPT_DIR/bin/mysqld_safe --basedir=$SCRIPT_DIR --user=cheng --transaction-isolation=SERIALIZABLE >> /tmp/mysql.debug 2>&1 &
#python $SCRIPT_DIR/merge.py &
