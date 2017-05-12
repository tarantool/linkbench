#!/bin/sh

SCRIPT=`realpath "$0"`
SCRIPT_DIR=`dirname "${SCRIPT}"`
LINKBENCH_DIR=$(realpath ${SCRIPT_DIR}/../..)

echo ${LINKBENCH_DIR}

mkdir -p log
FILE_LOG="log/linkbench.log"

PID_FILE=tarantool.pid
rm -f ${PID_FILE}

if nc -zv 127.0.0.1 3301; then
    echo "Server is already started"
else
    # Launch server
    echo "Launching server..."
    tarantool app.lua &
    echo $! > ${PID_FILE}
fi

set -x

#here we specify the test parameters
TARANTOOL_PROPS="${LINKBENCH_DIR}/config/LinkConfigTarantool.properties"

LINKBENCH_CMD="sh ${LINKBENCH_DIR}/bin/linkbench -c $TARANTOOL_PROPS -L $FILE_LOG"

#Load Phase
ok=`(($LINKBENCH_CMD -l 2>&1) | awk '{for(i=1;i<=NF;i++){ if($i=="ERROR"){print $0; exit(1)} } }')`
if ! test -z $ok
then
    echo 'Error in Load Phase'
    tail -20 $FILE_LOG
    exit 1
fi

#Request Phase
ok=`(($LINKBENCH_CMD -r 2>&1) | awk '{for(i=1;i<=NF;i++){ if($i=="ERROR" && $NF!="conflict"){print $0; exit(1)}}}')`
if ! test -z $ok
then
    echo 'Error in Request Phase'
    tail -20 $FILE_LOG
    exit 1
fi
echo "OK"
tail $FILE_LOG
res=`(tail -n 1 $FILE_LOG | awk '{print $NF}')`
echo "QPS = $res" | tee result.log

if test -f ${PID_FILE}; then
    cat ${PID_FILE} | xargs kill -s TERM
    rm -f ${PID_FILE}
fi

if [ -f /credentials/auth.conf ]
then
	pip install requests
	python ${LINKBENCH_DIR}/export.py /credentials/auth.conf result.log linkbench linkbench
fi
